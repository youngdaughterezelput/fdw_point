from dotenv import load_dotenv
import psycopg2
import pandas as pd
import re
import json
import time
import os
from collections import defaultdict
from typing import Dict, List, Optional, Union, Any, Tuple
from .security import AuthManager


class VirtualFDWManager:
    def __init__(self):
        """Инициализация менеджера виртуальных FDW подключений."""
        self.connection_params = {}
        self.table_mapping = {}  # Маппинг таблиц на подключения
        self.join_config = []    # Конфигурация JOIN между таблицами
        self.connections = {}    # Активные подключения
        self.log_messages = []   # Лог сообщений
        self.saved_credentials = {}
        self.load_env_config()

    def log(self, message: str, error: bool = False) -> None:
        """Логирование сообщений."""
        prefix = "[ERROR] " if error else "[INFO] "
        full_message = prefix + message
        print(full_message)
        self.log_messages.append(full_message)

    def load_env_config(self) -> None:
        """Загрузка конфигурации из .env файла."""
        try:
            env_path = os.path.abspath('.env')
            self.log(f"Загружаем конфигурацию из файла: {env_path}")
            
            # Создаем файл, если он не существует
            if not os.path.exists(env_path):
                with open(env_path, 'w') as f:
                    f.write("CONNECTIONS={}\nTABLE_MAPPINGS={}\nJOIN_CONFIG=[]\n")
            
            load_dotenv(env_path, override=True)
            
            # Загрузка подключений
            self.connection_params = json.loads(os.getenv("CONNECTIONS", "{}"))
            self.log(f"Загружены подключения: {self.connection_params}")
            
            # Загрузка маппинга таблиц
            self.table_mapping = json.loads(os.getenv("TABLE_MAPPINGS", "{}"))
            self.log(f"Загружен маппинг таблиц: {self.table_mapping}")
            
            # Загрузка конфигурации JOIN
            self.join_config = json.loads(os.getenv("JOIN_CONFIG", "[]"))
            self.log(f"Загружены правила JOIN: {self.join_config}")
            
        except Exception as e:
            self.log(f"Ошибка загрузки конфигурации: {str(e)}", error=True)
            self.connection_params = {}
            self.table_mapping = {}
            self.join_config = []
    
    def save_env_config(self) -> None:
        """Сохранение конфигурации в .env файл."""
        try:
            env_path = os.path.abspath('.env')
            self.log(f"Сохраняем конфигурацию в файл: {env_path}")
            
            # Читаем текущее содержимое файла
            current_content = {}
            if os.path.exists(env_path):
                with open(env_path, 'r') as f:
                    for line in f:
                        if '=' in line:
                            key, value = line.strip().split('=', 1)
                            current_content[key] = value
            
            # Обновляем только нужные ключи
            current_content['CONNECTIONS'] = json.dumps(self.connection_params)
            current_content['TABLE_MAPPINGS'] = json.dumps(self.table_mapping)
            current_content['JOIN_CONFIG'] = json.dumps(self.join_config)
            
            # Записываем обновленное содержимое
            with open(env_path, 'w') as f:
                for key, value in current_content.items():
                    f.write(f"{key}={value}\n")
            
            self.log(f"Успешно сохранено: CONNECTIONS={current_content['CONNECTIONS']}")
            self.log(f"Успешно сохранено: TABLE_MAPPINGS={current_content['TABLE_MAPPINGS']}")
            self.log(f"Успешно сохранено: JOIN_CONFIG={current_content['JOIN_CONFIG']}")
        except Exception as e:
            self.log(f"Критическая ошибка при сохранении в .env: {str(e)}", error=True)
            raise

    def add_connection(self, name: str, params: Dict[str, str]) -> None:
        """Добавление нового подключения."""
        required = ['host', 'port', 'dbname']
        if not all(k in params for k in required):
            raise ValueError(f"Необходимые параметры: {', '.join(required)}")
        
        self.connection_params[name] = params
        self.save_env_config()
        self.log(f"Добавлено новое подключение: {name}")

    def remove_connection(self, name: str) -> None:
        """Удаление подключения."""
        if name in self.connection_params:
            del self.connection_params[name]
            self.save_env_config()
            self.log(f"Удалено подключение: {name}")
        else:
            self.log(f"Подключение {name} не найдено", error=True)

    def map_table(self, table: str, connection: str) -> None:
        """Сопоставление таблицы с подключением."""
        if connection not in self.connection_params:
            raise ValueError(f"Подключение {connection} не существует")
            
        self.table_mapping[table] = connection
        self.save_env_config()
        self.log(f"Таблица {table} сопоставлена с подключением {connection}")

    def add_join_rule(self, tables: List[str], key: str, join_type: str = 'inner') -> None:
        """Добавление правила JOIN между таблицами."""
        if len(tables) < 2:
            raise ValueError("Необходимо указать минимум 2 таблицы")
        
        # Проверяем, что все таблицы есть в маппинге
        for table in tables:
            if table not in self.table_mapping:
                raise ValueError(f"Таблица {table} не найдена в маппинге")
        
        self.join_config.append({
            'tables': tables,
            'key': key,
            'join_type': join_type,
            'execute_in_db': False  # По умолчанию JOIN выполняется на стороне клиента
        })
        self.save_env_config()
        self.log(f"Добавлено правило JOIN для таблиц {', '.join(tables)} по ключу {key}")

    def set_join_execution(self, rule_index: int, execute_in_db: bool) -> None:
        """Установка места выполнения JOIN (БД или клиент)."""
        if 0 <= rule_index < len(self.join_config):
            self.join_config[rule_index]['execute_in_db'] = execute_in_db
            self.save_env_config()
            self.log(f"Правило JOIN #{rule_index} установлено на выполнение {'в БД' if execute_in_db else 'на клиенте'}")
        else:
            raise IndexError(f"Неверный индекс правила JOIN: {rule_index}")

    def get_connection(self, key: str, user: Optional[str] = None, password: Optional[str] = None) -> psycopg2.extensions.connection:
        """Получение подключения к БД."""
        if key not in self.connection_params:
            raise ValueError(f"Не найден ключ подключения: '{key}'")
        
        # Получаем учетные данные из AuthManager
        stored_user, stored_password = AuthManager.get_credentials(key)
        
        # Используем переданные данные или сохраненные
        params = self.connection_params[key].copy()
        params.update({
            'user': user or stored_user or '',
            'password': password or stored_password or ''
        })
        
        try:
            conn = psycopg2.connect(**params)
            conn.autocommit = True
            self.connections[key] = conn
            self.log(f"Успешное подключение к {key}")
            return conn
        except Exception as e:
            self.log(f"Ошибка подключения к {key}: {str(e)}", error=True)
            raise ConnectionError(f"Ошибка подключения к {key}: {str(e)}") from e

    def execute_query(self, query: str) -> Tuple[pd.DataFrame, float]:
        """Выполнение SQL запроса с поддержкой JOIN между разными БД."""
        start_time = time.time()


        # Разделяем запрос на отдельные команды
        commands = [cmd.strip() for cmd in query.split(';') if cmd.strip()]
        results = []
        last_successful_result = pd.DataFrame()
        
        for cmd in commands:
            try:
                # Пропускаем пустые команды
                if not cmd:
                    continue
                    
                # Определяем тип команды
                cmd_lower = cmd.lower().strip()
                
                if cmd_lower.startswith('select'):
                    # Обработка SELECT запросов
                    result, exec_time = self._execute_select(cmd)
                    results.append(result)
                    last_successful_result = result
                    self.log(f"SELECT выполнен за {exec_time:.2f} сек. Найдено строк: {len(result)}")
                    
                elif cmd_lower.startswith(('insert', 'update', 'delete')):
                    # Обработка DML команд
                    affected_rows = self._execute_dml(cmd)
                    results.append(pd.DataFrame({'affected_rows': [affected_rows]}))
                    self.log(f"DML команда выполнена. Затронуто строк: {affected_rows}")
                    
                else:
                    # Обработка других команд (CREATE, DROP и т.д.)
                    self._execute_generic(cmd)
                    results.append(pd.DataFrame({'status': ['success']}))
                    self.log(f"Команда выполнена успешно")
                    
            except Exception as e:
                error_msg = f"Ошибка выполнения команды: {str(e)}"
                self.log(error_msg, error=True)
                results.append(pd.DataFrame({'error': [error_msg]}))
        
        exec_time = time.time() - start_time
        
        
        #обработаем первый селект
        select_match = re.search(r"SELECT\s.+?FROM\s.+?;", query, re.IGNORECASE | re.DOTALL)
        if select_match:
            query = select_match.group(0)
        
        try:
            # Парсинг SQL запроса
            parsed = self.parse_sql(query)
            self.log(f"Парсинг SQL завершен: {parsed}")
            
            # Определение таблиц и их подключений
            table_info = self._resolve_table_mappings(parsed)
            self.log(f"Определены подключения для таблиц: {table_info}")
            
            # Группировка таблиц по подключениям
            conn_groups = self._group_tables_by_connection(table_info)
            self.log(f"Таблицы сгруппированы по подключениям: {conn_groups}")
            
            # Загрузка данных с учетом JOIN внутри одного подключения
            dfs = self._fetch_data(parsed, table_info, conn_groups)
            self.log(f"Данные загружены для таблиц: {list(dfs.keys())}")
            
            # Объединение результатов из разных подключений
            merged = self._merge_results(parsed, table_info, dfs)
            self.log(f"Результаты объединены, строк: {len(merged)}")
            
            # Фильтрация результатов после объединения
            if not merged.empty and parsed.get('where'):
                merged = self._apply_global_where(merged, parsed['where'])
                self.log(f"Применены условия WHERE, строк осталось: {len(merged)}")
            
            # Финализация результата
            merged = merged.fillna('NULL').reset_index(drop=True)
            
            exec_time = time.time() - start_time
            self.log(f"Запрос выполнен за {exec_time:.2f} сек.")
            return merged, exec_time

        except Exception as e:
            exec_time = time.time() - start_time
            error_msg = f"{str(e)} (Время выполнения: {exec_time:.2f} сек.)"
            self.log(error_msg, error=True)
            raise RuntimeError(error_msg) from e
        finally:
            self._close_connections()

    def _resolve_table_mappings(self, parsed: Dict[str, Any]) -> Dict[str, Dict[str, str]]:
        """Определение подключений для таблиц в запросе."""
        table_info = {}
        
        for full_table in parsed['tables']:
            # Поиск таблицы в маппинге
            if full_table not in self.table_mapping:
                # Поиск по имени таблицы без схемы
                table_name_only = full_table.split('.')[-1]
                candidates = [t for t in self.table_mapping.keys() if t.split('.')[-1] == table_name_only]
                
                if len(candidates) == 1:
                    full_table = candidates[0]
                elif len(candidates) > 1:
                    raise ValueError(f"Неоднозначное соответствие для таблицы '{full_table}': найдено несколько таблиц в маппинге ({', '.join(candidates)}). Уточните схему.")
                else:
                    raise ValueError(f"Таблица '{full_table}' не найдена в маппинге")
            
            connection_name = self.table_mapping[full_table]
            
            # Разделяем на схему и таблицу
            if '.' in full_table:
                schema, table_name = full_table.split('.', 1)
            else:
                schema = 'public'
                table_name = full_table
            
            # Определяем алиас
            alias = next((a for a, t in parsed['aliases'].items() if t == full_table), None)
            if alias is None:
                alias = table_name
            
            table_info[full_table] = {
                'connection': connection_name,
                'schema': schema,
                'table_name': table_name,
                'alias': alias,
                'columns': []
            }
        
        return table_info

    def _group_tables_by_connection(self, table_info: Dict[str, Dict[str, str]]) -> Dict[str, List[str]]:
        """Группировка таблиц по подключениям."""
        conn_groups = defaultdict(list)
        for table, info in table_info.items():
            conn_groups[info['connection']].append(table)
        return conn_groups

    def _fetch_data(self, parsed: Dict[str, Any], table_info: Dict[str, Dict[str, str]], 
                   conn_groups: Dict[str, List[str]]) -> Dict[str, pd.DataFrame]:
        """Загрузка данных из БД с учетом JOIN внутри одного подключения."""
        dfs = {}
        join_rules = self._get_applicable_join_rules(table_info)
        
        for conn_name, tables_in_conn in conn_groups.items():
            # Проверяем, можно ли выполнить JOIN на стороне БД
            db_join_possible = self._check_db_join_possible(tables_in_conn, join_rules)
            
            if db_join_possible:
                # Выполняем JOIN на стороне БД
                dfs.update(self._execute_db_join(parsed, table_info, conn_name, tables_in_conn, join_rules))
            else:
                # Выполняем отдельные запросы и JOIN на стороне клиента
                dfs.update(self._execute_client_join(parsed, table_info, conn_name, tables_in_conn, join_rules))
        
        return dfs

    def _check_db_join_possible(self, tables: List[str], join_rules: List[Dict[str, Any]]) -> bool:
        """Проверяет, можно ли выполнить JOIN на стороне БД."""
        if len(tables) == 1:
            return False
            
        # Проверяем, есть ли правило JOIN для этих таблиц с execute_in_db=True
        for rule in join_rules:
            if set(tables).issubset(set(rule['tables'])) and rule.get('execute_in_db', False):
                return True
                
        return False

    def _execute_db_join(self, parsed: Dict[str, Any], table_info: Dict[str, Dict[str, str]], 
                        conn_name: str, tables_in_conn: List[str], 
                        join_rules: List[Dict[str, Any]]) -> Dict[str, pd.DataFrame]:
        """Выполняет JOIN на стороне БД."""
        dfs = {}
        base_table = tables_in_conn[0]
        base_info = table_info[base_table]
        
        # Формируем SELECT часть
        select_parts = []
        column_aliases = {}
        for table in tables_in_conn:
            info = table_info[table]
            columns = self._get_columns_for_table(parsed['columns'], info['alias'], table)
            if columns == ['*']:
                select_parts.append(f"{info['alias']}.*")
            else:
                for col in columns:
                    col_alias = f"{info['alias']}_{col}"
                    select_parts.append(f"{info['alias']}.{col} AS {col_alias}")
                    column_aliases[(info['alias'], col)] = col_alias
        
        # Формируем FROM и JOIN части
        from_parts = [f"{base_info['schema']}.{base_info['table_name']} AS {base_info['alias']}"]
        for table in tables_in_conn[1:]:
            info = table_info[table]
            from_parts.append(f"JOIN {info['schema']}.{info['table_name']} AS {info['alias']}")
        
        # Получаем условия JOIN из правил
        join_conditions = []
        for rule in join_rules:
            if set(tables_in_conn).issubset(set(rule['tables'])) and rule.get('execute_in_db', False):
                for i in range(1, len(rule['tables'])):
                    if rule['tables'][i] in tables_in_conn:
                        left_table = rule['tables'][0]
                        right_table = rule['tables'][i]
                        left_alias = table_info[left_table]['alias']
                        right_alias = table_info[right_table]['alias']
                        join_conditions.append(f"{left_alias}.{rule['key']} = {right_alias}.{rule['key']}")
        
        # Собираем полный запрос
        sql = f"SELECT {', '.join(select_parts)} FROM {' '.join(from_parts)}"
        if join_conditions:
            sql += " ON " + " AND ".join(join_conditions)
        
        # Добавляем WHERE условия
        where_conditions = []
        for table in tables_in_conn:
            info = table_info[table]
            table_where = self._extract_table_where(parsed.get('where', ''), info['alias'])
            if table_where:
                where_conditions.append(table_where)
        
        if where_conditions:
            sql += " WHERE " + " AND ".join(where_conditions)
        
        self.log(f"Выполняем JOIN-запрос в БД {conn_name}:\n{sql}")
        
        # Выполняем запрос
        with self.get_connection(conn_name).cursor() as cur:
            cur.execute(sql)
            df_joined = pd.DataFrame(cur.fetchall(), columns=[desc[0] for desc in cur.description])
        
        # Разделяем результат на отдельные таблицы
        for table in tables_in_conn:
            info = table_info[table]
            # Выбираем колонки относящиеся к текущей таблице
            table_cols = []
            if parsed['columns'] == ['*']:
                prefix = info['alias'] + '_'
                table_cols = [col for col in df_joined.columns if col.startswith(prefix)]
            else:
                table_cols = [column_aliases.get((info['alias'], col)) 
                            for col in self._get_columns_for_table(parsed['columns'], info['alias'], table)
                            if (info['alias'], col) in column_aliases]
                table_cols = [col for col in table_cols if col]
            
            df_table = df_joined[table_cols].copy()
            
            # Убираем префикс алиаса из имен колонок
            if not parsed['select_all']:
                df_table.columns = [col.split('_', 1)[1] if '_' in col else col 
                                for col in df_table.columns]
            
            # Добавляем префикс алиаса таблицы к именам колонок
            df_table.columns = [f"{info['alias']}.{col}" for col in df_table.columns]
            
            dfs[table] = df_table
            info['columns'] = df_table.columns.tolist()
        
        return dfs

    def _execute_client_join(self, parsed: Dict[str, Any], table_info: Dict[str, Dict[str, str]], 
                           conn_name: str, tables_in_conn: List[str], 
                           join_rules: List[Dict[str, Any]]) -> Dict[str, pd.DataFrame]:
        """Выполняет отдельные запросы и JOIN на стороне клиента."""
        dfs = {}
        
        for full_table in tables_in_conn:
            info = table_info[full_table]
            
            # Определяем условия WHERE для текущей таблицы
            table_where = self._extract_table_where(parsed.get('where', ''), info['alias'])
            
            # Формируем SQL запрос
            columns = self._get_columns_for_table(parsed['columns'], info['alias'], full_table)
            cols = ', '.join(columns) if columns and columns != ['*'] else '*'
            
            sql = f"SELECT {cols} FROM {info['schema']}.{info['table_name']}"
            
            # Добавляем условия WHERE, если есть
            conditions = []
            if table_where:
                conditions.append(table_where)
            
            # Добавляем JOIN условия для межсерверных соединений
            join_params = []
            join_key = None
            for rule in join_rules:
                if full_table in rule['tables']:
                    for other_table in rule['tables']:
                        if other_table != full_table and other_table in dfs:
                            join_key = rule['key']
                            other_info = table_info[other_table]
                            other_df = dfs[other_table]
                            
                            other_col = f"{other_info['alias']}.{join_key}"
                            if other_col in other_df.columns:
                                values = other_df[other_col].unique()
                                join_params.extend(values.tolist())
            
            # Если есть JOIN условия, добавляем их в запрос
            if join_params and join_key:
                join_condition = f"{info['alias']}.{join_key} IN %s"
                conditions.append(join_condition)
            
            if conditions:
                sql += " WHERE " + " AND ".join(conditions)
            
            self.log(f"Выполняем запрос к {full_table}: {sql}")
            
            # Выполняем запрос
            with self.get_connection(info['connection']).cursor() as cur:
                if join_params:
                    params = (tuple(join_params),)
                    cur.execute(sql, params)
                else:
                    cur.execute(sql)
                
                df = pd.DataFrame(cur.fetchall(), columns=[desc[0] for desc in cur.description])
                # Добавляем префикс алиаса
                df.columns = [f"{info['alias']}.{col}" for col in df.columns]
                info['columns'] = df.columns.tolist()
                dfs[full_table] = df
        
        return dfs

    def _merge_results(self, parsed: Dict[str, Any], table_info: Dict[str, Dict[str, str]], 
                      dfs: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Объединение результатов из разных подключений."""
        if len(dfs) == 1:
            return next(iter(dfs.values()))
        
        # Определяем порядок объединения (по порядку в FROM)
        tables_ordered = [t for t in parsed['tables'] if t in dfs]
        merged = dfs[tables_ordered[0]]
        
        for table in tables_ordered[1:]:
            if table not in dfs:
                continue
                
            # Формируем ключи для объединения
            join_keys = self._get_join_keys(parsed, table_info, table, merged.columns)
            
            if join_keys:
                self.log(f"Объединяем {table} по ключам: {join_keys}")
                merged = pd.merge(
                    merged,
                    dfs[table],
                    how='left',
                    left_on=join_keys['left_keys'],
                    right_on=join_keys['right_keys'],
                    suffixes=('', '_DROP')
                )
                # Удаляем дублирующиеся колонки
                drop_cols = [c for c in merged.columns if c.endswith('_DROP')]
                if drop_cols:
                    merged = merged.drop(columns=drop_cols)
            else:
                # Используем предварительно настроенные правила JOIN
                join_found = False
                for rule in self._get_applicable_join_rules(table_info):
                    if table in rule['tables']:
                        # Находим общую таблицу для JOIN
                        common_tables = [t for t in rule['tables'] if t in dfs and t != table]
                        if common_tables:
                            common_table = common_tables[0]
                            join_key = rule['key']
                            
                            left_keys = [f"{table_info[common_table]['alias']}.{join_key}"]
                            right_keys = [f"{table_info[table]['alias']}.{join_key}"]
                            
                            # Проверяем наличие ключей в данных
                            if (all(k in merged.columns for k in left_keys) and 
                                all(k in dfs[table].columns for k in right_keys)):
                                
                                self.log(f"Объединяем {table} по правилу JOIN: {rule}")
                                merged = pd.merge(
                                    merged,
                                    dfs[table],
                                    how='left',
                                    left_on=left_keys,
                                    right_on=right_keys,
                                    suffixes=('', '_DROP')
                                )
                                # Удаляем дублирующиеся колонки
                                drop_cols = [c for c in merged.columns if c.endswith('_DROP')]
                                if drop_cols:
                                    merged = merged.drop(columns=drop_cols)
                                join_found = True
                                break
                
                if not join_found:
                    self.log(f"Явных ключей JOIN для {table} не найдено, используем конкатенацию")
                    merged = pd.concat([merged, dfs[table]], axis=1)
        
        return merged

    def _apply_global_where(self, df: pd.DataFrame, where_clause: str) -> pd.DataFrame:
        """Применение глобального условия WHERE после объединения."""
        global_where = self._prepare_where_condition(where_clause, df.columns)
        self.log(f"Применяем глобальное условие WHERE: {global_where}")
        
        try:
            return df.query(global_where, engine='python')
        except Exception as e:
            self.log(f"Ошибка при query(): {e}. Пробуем альтернативный метод...", error=True)
            return self._apply_where_manually(df, global_where)

    def _close_connections(self) -> None:
        """Закрытие всех активных подключений."""
        for conn in self.connections.values():
            if conn and not conn.closed:
                try:
                    conn.close()
                    self.log(f"Закрыто подключение {conn}")
                except Exception as e:
                    self.log(f"Ошибка при закрытии подключения: {str(e)}", error=True)
        self.connections.clear()

    def parse_sql(self, query: str) -> Dict[str, Any]:
        """Надежный парсер SQL с поддержкой JOIN и сложных запросов."""
        parsed = {
            'columns': [],
            'tables': set(),
            'aliases': {},
            'where': '',
            'select_all': False,
            'joins': []
        }

        # Нормализуем пробелы
        normalized_query = re.sub(r'\s+', ' ', query).strip()
        query_lower = normalized_query.lower()
        
        # Извлекаем SELECT
        select_idx = query_lower.find('select')
        from_idx = query_lower.find('from', select_idx)
        
        if select_idx == -1 or from_idx == -1:
            raise ValueError("Некорректный SQL: отсутствует SELECT или FROM")
        
        # Колонки
        columns_part = normalized_query[select_idx+6:from_idx].strip()
        parsed['columns'] = self._split_columns(columns_part)
        parsed['select_all'] = any('*' in col for col in parsed['columns'])

        # Секция FROM
        from_end = from_idx + 4
        from_part = normalized_query[from_end:]

        # Разбор JOIN
        join_pattern = re.compile(
            r'\b(?:INNER\s+JOIN|LEFT\s+JOIN|RIGHT\s+JOIN|FULL\s+JOIN|CROSS\s+JOIN|JOIN)\b\s+(\w+\.\w+|\w+)(?:\s+AS\s+)?(\w+)?\s+ON\s+([^)]+)',
            re.IGNORECASE
        )
        
        join_matches = list(join_pattern.finditer(normalized_query))
        for match in join_matches:
            table_name = match.group(1)
            alias = match.group(2)
            condition = match.group(3)
            
            # Добавляем таблицу в список
            parsed['tables'].add(table_name)
            
            # Сохраняем псевдоним
            if alias:
                parsed['aliases'][alias] = table_name
                
            # Сохраняем условие JOIN
            parsed['joins'].append({
                'table': table_name,
                'alias': alias,
                'condition': condition.strip()
            })
        
        # Находим начало WHERE
        where_idx = query_lower.find('where', from_end)
        
        # Извлекаем только часть с таблицами (до WHERE или конца)
        tables_part = from_part
        if where_idx != -1:
            where_end = len(normalized_query)
            where_clause = normalized_query[where_idx+5:where_end].strip()
            
            # Удаляем лишние части (GROUP BY, ORDER BY и т.д.)
            for terminator in ['group by', 'order by', 'limit']:
                pos = where_clause.lower().find(terminator)
                if pos != -1:
                    where_clause = where_clause[:pos].strip()
                    break
            
            # Нормализуем имена таблиц в условии WHERE
            where_clause = re.sub(r'(\b\w+\b\.\b\w+\b\.\b\w+\b)', lambda m: m.group(0).replace('.', '_'), where_clause)
            parsed['where'] = where_clause
        
        # Удаляем условия JOIN (ON ...)
        tables_part = re.sub(r'\bon\b.+', '', tables_part, flags=re.IGNORECASE)
        
        # Разбиваем на токены, игнорируя JOIN
        tokens = re.split(r',|\bjoin\b', tables_part, flags=re.IGNORECASE)
        
        for token in tokens:
            token = token.strip()
            if not token:
                continue
                
            parts = token.split()
            if not parts:
                continue
                
            # Первое слово - имя таблицы
            table_name = parts[0]
            parsed['tables'].add(table_name)
            
            # Обрабатываем псевдоним (если есть)
            alias = None
            if len(parts) > 1:
                # Пропускаем "AS"
                if parts[1].lower() == 'as' and len(parts) > 2:
                    alias = parts[2]
                else:
                    alias = parts[1]
            
            # Сохраняем псевдоним
            if alias:
                # Удаляем кавычки и игнорируем ключевые слова
                alias = alias.strip('"\'')
                if alias.lower() not in ['inner', 'outer', 'left', 'right', 'full', 'cross']:
                    parsed['aliases'][alias] = table_name

        # Условие WHERE
        if where_idx != -1:
            where_end = len(normalized_query)
            where_clause = normalized_query[where_idx+5:where_end].strip()
            
            # Удаляем лишние части (GROUP BY, ORDER BY и т.д.)
            for terminator in ['group by', 'order by', 'limit']:
                pos = where_clause.lower().find(terminator)
                if pos != -1:
                    where_clause = where_clause[:pos].strip()
                    break
                    
            parsed['where'] = where_clause

        return parsed
    
    @staticmethod
    def _split_columns(columns_str: str) -> List[str]:
        """Надежный парсер колонок без регулярных выражений."""
        parts = []
        current = []
        in_quotes = False
        in_parentheses = 0
        
        for char in columns_str:
            if char == "'" or char == '"':
                in_quotes = not in_quotes
            elif char == '(' and not in_quotes:
                in_parentheses += 1
            elif char == ')' and not in_quotes and in_parentheses > 0:
                in_parentheses -= 1
            elif char == ',' and not in_quotes and in_parentheses == 0:
                parts.append(''.join(current).strip())
                current = []
                continue
                
            current.append(char)
        
        if current:
            parts.append(''.join(current).strip())
        
        return parts

    def _get_applicable_join_rules(self, table_info: Dict[str, Dict[str, str]]) -> List[Dict[str, Any]]:
        """Возвращает JOIN правила, применимые к текущим таблицам."""
        applicable_rules = []
        tables = list(table_info.keys())
        
        for rule in self.join_config:
            # Проверяем, что все таблицы из правила присутствуют в запросе
            if all(table in tables for table in rule['tables']):
                applicable_rules.append(rule)
        
        return applicable_rules

    def _extract_table_where(self, where_clause: str, table_alias: str) -> str:
        """Извлекает условия WHERE относящиеся к конкретной таблице."""
        if not where_clause:
            return ''
        
        # Упрощенная реализация: берем только условия с указанием алиаса таблицы
        conditions = []
        tokens = where_clause.split(' AND ')
        
        for token in tokens:
            token = token.strip()
            if not token:
                continue
                
            # Проверяем относится ли условие к этой таблице
            if f"{table_alias}." in token or ('.' not in token and '=' in token):
                conditions.append(token)
        
        return ' AND '.join(conditions)

    def _get_columns_for_table(self, columns: List[str], table_alias: str, full_table: str) -> List[str]:
        """Определяет какие колонки запрашивать для конкретной таблицы."""
        result = []
        
        for col in columns:
            if col == '*':
                return ['*']
                
            if '.' in col:
                parts = col.split('.')
                if len(parts) == 2 and parts[0] == table_alias:
                    result.append(parts[1])
                elif len(parts) == 3 and f"{parts[0]}.{parts[1]}" == full_table:
                    result.append(parts[2])
            else:
                # Колонки без указания таблицы добавляем для всех таблиц
                result.append(col)
        
        return result if result else ['*']

    def _get_join_keys(self, parsed: Dict[str, Any], table_info: Dict[str, Dict[str, str]], 
                      current_table: str, available_columns: List[str]) -> Optional[Dict[str, List[str]]]:
        """Определяет ключи для объединения таблиц."""
        join_keys = {'left_keys': [], 'right_keys': []}
        current_alias = table_info[current_table]['alias']
        
        for join in parsed.get('joins', []):
            if join['table'] != current_table and join.get('alias') != current_alias:
                continue
                
            condition = join['condition']
            comparisons = [c.strip() for c in re.split(r'\bAND\b|\bOR\b', condition, flags=re.IGNORECASE) if c.strip()]
            
            for comp in comparisons:
                if '=' in comp:
                    left, right = [p.strip() for p in comp.split('=', 1)]
                    
                    left_table = left.split('.')[0] if '.' in left else None
                    right_table = right.split('.')[0] if '.' in right else None
                    
                    if left_table == current_alias and right in available_columns:
                        join_keys['right_keys'].append(left.split('.')[-1])
                        join_keys['left_keys'].append(right)
                    elif right_table == current_alias and left in available_columns:
                        join_keys['left_keys'].append(left)
                        join_keys['right_keys'].append(right.split('.')[-1])
        
        return join_keys if join_keys['left_keys'] else None

    def _prepare_where_condition(self, where_clause: str, available_columns: List[str]) -> str:
        """Подготавливает условие WHERE для использования в pandas."""
        # Создаем маппинг для замены имен колонок (удаляем префиксы таблиц)
        column_mapping = {}
        for col in available_columns:
            if '.' in col:
                # Сохраняем полное имя и имя без префикса таблицы
                column_mapping[col] = col
                column_mapping[col.split('.')[-1]] = col
        
        # Заменяем имена в условии WHERE на полные имена колонок
        for original, new in sorted(column_mapping.items(), key=lambda x: -len(x[0])):
            where_clause = re.sub(
                rf'(?<!\w){re.escape(original)}(?!\w)',
                new,
                where_clause
            )
        
        # Удаляем префиксы таблиц из имен колонок
        where_clause = re.sub(r'\b\w+\.(\w+)\b', r'\1', where_clause)
        
        # Преобразуем SQL в pandas-синтаксис
        where_clause = (
            where_clause
            .replace('=', '==')
            .replace('<>', '!=')
            .replace('IS NULL', '.isna()')
            .replace('IS NOT NULL', '.notna()')
            .replace("'", '"')
        )
        
        return where_clause
    
    def _apply_where_manually(self, df: pd.DataFrame, where_condition: str) -> pd.DataFrame:
        """Применяет условие WHERE вручную, если query() не сработал."""
        # Удаляем префиксы таблиц из имен колонок
        where_condition = re.sub(r'\b\w+\.(\w+)\b', r'\1', where_condition)
        
        conditions = [c.strip() for c in where_condition.split(' AND ')]
        mask = pd.Series(True, index=df.index)
        
        for cond in conditions:
            try:
                if '==' in cond:
                    col, val = cond.split('==', 1)
                    col = col.strip()
                    val = val.strip().strip('"\'')
                    mask = mask & (df[col].astype(str) == val)
                elif '!=' in cond:
                    col, val = cond.split('!=', 1)
                    col = col.strip()
                    val = val.strip().strip('"\'')
                    mask = mask & (df[col].astype(str) != val)
                elif '.isna()' in cond:
                    col = cond.replace('.isna()', '').strip()
                    mask = mask & df[col].isna()
                elif '.notna()' in cond:
                    col = cond.replace('.notna()', '').strip()
                    mask = mask & df[col].notna()
            except Exception as e:
                self.log(f"Ошибка обработки условия {cond}: {str(e)}", error=True)
                continue
        
        return df[mask]

    @staticmethod
    def _split_where_conditions(where_clause: str) -> List[str]:
        """Надежное разбиение условий WHERE без сложных регулярных выражений."""
        conditions = []
        current = []
        in_quotes = False
        quote_char = None
        
        for char in where_clause:
            if char in ['"', "'"]:
                if in_quotes and char == quote_char:
                    in_quotes = False
                    quote_char = None
                elif not in_quotes:
                    in_quotes = True
                    quote_char = char
                current.append(char)
            elif not in_quotes:
                if char == '.' and current and current[-1] != ' ':
                    # Проверка на AND/OR
                    last_chars = ''.join(current[-4:]).lower()
                    if last_chars.endswith(' and') or last_chars.endswith(' or '):
                        condition = ''.join(current[:-4]).strip()
                        if condition:
                            conditions.append(condition)
                        current = []
                current.append(char)
            else:
                current.append(char)
        
        if current:
            conditions.append(''.join(current).strip())
        
        # Фильтрация пустых условий
        return [c for c in conditions if c]
    
    def query_database(self, connection_name: str, schema: str, table: str, columns: List[str]) -> pd.DataFrame:
        """Выполняет простой запрос к указанной таблице."""
        try:
            conn = self.get_connection(connection_name)
            cur = conn.cursor()
            
            cols = ', '.join(columns)
            query = f"SELECT {cols} FROM {schema}.{table}"
            
            cur.execute(query)
            rows = cur.fetchall()
            colnames = [desc[0] for desc in cur.description]
            
            df = pd.DataFrame(rows, columns=colnames)
            cur.close()
            return df
        except Exception as e:
            self.log(f"Ошибка при запросе к {schema}.{table}: {str(e)}", error=True)
            raise Exception(f"Ошибка при запросе к {schema}.{table}: {str(e)}") from e
        

    
from dotenv import load_dotenv
import psycopg2
import pandas as pd
import re
import json
import time
import os
from collections import defaultdict
from .security import AuthManager

class VirtualFDWManager:
    def __init__(self):
        self.connection_params = {}
        self.schema_mapping = {}
        self.table_mapping = {}  # Новый словарь для маппинга таблиц
        self.join_config = []    # Конфигурация JOIN
        self.connections = {}
        self.log_messages = []  # Для хранения сообщений
        self.saved_credentials = {}
        self.load_env_config()

    def log(self, message, error=False):
        """Логирование сообщений"""
        prefix = "[ERROR] " if error else "[INFO] "
        full_message = prefix + message
        print(full_message)  # Вывод в консоль
        self.log_messages.append(full_message)  # Сохранение для GUI

    def load_env_config(self):
        """Загрузка конфигурации из .env файла"""
        try:
            env_path = os.path.abspath('.env')
            print(f"Загружаем конфигурацию из файла: {env_path}")
            
            # Создаем файл, если он не существует
            if not os.path.exists(env_path):
                with open(env_path, 'w') as f:
                    f.write("CONNECTIONS={}\nTABLE_MAPPINGS={}\nJOIN_CONFIG=[]\n")
            
            # Перезагружаем переменные окружения
            load_dotenv(env_path, override=True)
            
            # Загрузка подключений
            connections_json = os.getenv("CONNECTIONS", "{}")
            self.connection_params = json.loads(connections_json)
            print(f"Загружены подключения: {self.connection_params}")
            
            # Загрузка маппинга таблиц (новый формат)
            table_mappings_json = os.getenv("TABLE_MAPPINGS", "{}")
            self.table_mapping = json.loads(table_mappings_json)
            print(f"Загружен маппинг таблиц: {self.table_mapping}")
            
            # Загрузка конфигурации JOIN
            join_config_json = os.getenv("JOIN_CONFIG", "[]")
            self.join_config = json.loads(join_config_json)
            print(f"Загружены правила JOIN: {self.join_config}")
            
        except Exception as e:
            print(f"Ошибка загрузки конфигурации: {str(e)}")
            # Создаем пустые структуры
            self.connection_params = {}
            self.table_mapping = {}
            self.join_config = []
    
    def save_env_config(self):
        """Сохранение конфигурации в .env файл"""
        try:
            env_path = os.path.abspath('.env')
            print(f"Сохраняем конфигурацию в файл: {env_path}")
            
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
            
            print(f"Успешно сохранено: CONNECTIONS={current_content['CONNECTIONS']}")
            print(f"Успешно сохранено: TABLE_MAPPINGS={current_content['TABLE_MAPPINGS']}")
            print(f"Успешно сохранено: JOIN_CONFIG={current_content['JOIN_CONFIG']}")
        except Exception as e:
            print(f"Критическая ошибка при сохранении в .env: {str(e)}")
            raise

    def _validate_join_rule(self, rule):
        if not isinstance(rule.get('join_type', 'inner'), str):
            rule['join_type'] = 'inner'
        return rule

    def get_connection(self, key, user=None, password=None):
        if key not in self.connection_params:
            raise ValueError(f"Не найден ключ подключения: '{key}'")
        
        # Всегда получаем учетные данные из AuthManager
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
            return conn
        except Exception as e:
            raise ConnectionError(f"Ошибка подключения к {key}: {str(e)}")

    def parse_sql(self, query):
        """Надежный парсер SQL с поддержкой JOIN и сложных запросов"""
        parsed = {
            'columns': [],
            'tables': set(),
            'aliases': {},
            'where': '',
            'select_all': False,
            'joins': []  # Список для хранения условий JOIN
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
    def _split_columns(columns_str):
        """Надежный парсер колонок без регулярных выражений"""
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

    def execute_query(self, query):
        print(f"Выполняем запрос: {query}")
        print(f"Текущий маппинг таблиц: {self.table_mapping}")
        start_time = time.time()
        
        try:
            parsed = self.parse_sql(query)
            print(f"Парсинг результата: {parsed}")
            
            # 1. Определение таблиц и их подключений
            table_info = {}
            for full_table in parsed['tables']:
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

            # 2. Загрузка данных с учетом JOIN условий
            dfs = {}
            join_rules = self._get_applicable_join_rules(table_info)
            
            for full_table, info in table_info.items():
                # Определяем условия WHERE для текущей таблицы
                table_where = self._extract_table_where(parsed.get('where', ''), info['alias'])
                
                # Формируем SQL запрос для текущей таблицы
                columns = self._get_columns_for_table(parsed['columns'], info['alias'], full_table)
                cols = ', '.join(columns) if columns and columns != ['*'] else '*'
                
                sql = f"SELECT {cols} FROM {info['schema']}.{info['table_name']}"
                
                # Добавляем условия WHERE, если есть
                conditions = []
                if table_where:
                    conditions.append(table_where)
                
                # Добавляем JOIN условия из предварительно настроенных правил
                join_params = []
                for rule in join_rules:
                    if full_table in rule['tables']:
                        for other_table in rule['tables']:
                            if other_table != full_table and other_table in dfs:
                                # Получаем значения для JOIN из уже загруженной таблицы
                                join_key = rule['key']
                                other_info = table_info[other_table]
                                other_df = dfs[other_table]
                                
                                # Формируем имя колонки с префиксом алиаса
                                other_col = f"{other_info['alias']}.{join_key}"
                                if other_col in other_df.columns:
                                    values = other_df[other_col].unique()
                                    join_params.extend(values.tolist())
                
                # Если есть JOIN условия, добавляем их в запрос
                if join_params:
                    join_condition = f"{info['alias']}.{rule['key']} IN %s"
                    conditions.append(join_condition)
                
                if conditions:
                    sql += " WHERE " + " AND ".join(conditions)
                
                print(f"Выполняем запрос к {full_table}: {sql}")
                
                # Выполняем запрос
                with self.get_connection(info['connection']).cursor() as cur:
                    if join_params:
                        # Для IN условий преобразуем список в кортеж
                        params = (tuple(join_params),)
                        cur.execute(sql, params)
                    else:
                        cur.execute(sql)
                    
                    df = pd.DataFrame(cur.fetchall(), columns=[desc[0] for desc in cur.description])
                    # Добавляем префикс алиаса только если это не *
                    if columns != ['*']:
                        df.columns = [f"{info['alias']}.{col}" for col in df.columns]
                    else:
                        # Для * добавляем префикс ко всем колонкам
                        df.columns = [f"{info['alias']}.{col}" for col in df.columns]
                    
                    info['columns'] = df.columns.tolist()
                    dfs[full_table] = df

            # 3. Объединение результатов
            if len(dfs) == 1:
                merged = next(iter(dfs.values()))
            else:
                # Определяем порядок объединения (по порядку в FROM)
                tables_ordered = list(dfs.keys())
                merged = dfs[tables_ordered[0]]
                
                for table in tables_ordered[1:]:
                    join_on = self._get_join_keys(parsed, table_info, table, merged.columns)
                    
                    if join_on:
                        print(f"Объединяем {table} по условиям: {join_on}")
                        merged = pd.merge(
                            merged,
                            dfs[table],
                            how='left',
                            left_on=join_on['left_keys'],
                            right_on=join_on['right_keys'],
                            suffixes=('', '_DROP')
                        )
                        # Удаляем дублирующиеся колонки
                        merged = merged[[c for c in merged.columns if not c.endswith('_DROP')]]
                    else:
                        # Используем предварительно настроенные правила JOIN
                        join_found = False
                        for rule in join_rules:
                            if table in rule['tables']:
                                # Находим общую таблицу для JOIN
                                common_tables = [t for t in rule['tables'] if t in dfs and t != table]
                                if common_tables:
                                    common_table = common_tables[0]
                                    join_key = rule['key']
                                    
                                    left_keys = [f"{table_info[common_table]['alias']}.{join_key}"]
                                    right_keys = [f"{table_info[table]['alias']}.{join_key}"]
                                    
                                    print(f"Объединяем {table} по предварительному правилу JOIN: {rule}")
                                    merged = pd.merge(
                                        merged,
                                        dfs[table],
                                        how='left',
                                        left_on=left_keys,
                                        right_on=right_keys,
                                        suffixes=('', '_DROP')
                                    )
                                    # Удаляем дублирующиеся колонки
                                    merged = merged[[c for c in merged.columns if not c.endswith('_DROP')]]
                                    join_found = True
                                    break
                        
                        if not join_found:
                            print(f"Явных условий JOIN для {table} не найдено, используем concat")
                            merged = pd.concat([merged, dfs[table]], axis=1)

            # 4. Фильтрация результатов после объединения
            if not merged.empty and parsed.get('where'):
                global_where = self._prepare_where_condition(parsed['where'], merged.columns)
                print(f"Применяем глобальное условие WHERE: {global_where}")
                
                try:
                    merged = merged.query(global_where, engine='python')
                except Exception as e:
                    print(f"Ошибка при query(): {e}. Пробуем альтернативный метод...")
                    merged = self._apply_where_manually(merged, global_where)

            # Финализация результата
            merged = merged.fillna('NULL')
            merged = merged.reset_index(drop=True)
            
            exec_time = time.time() - start_time
            print(f"Запрос выполнен за {exec_time:.2f} сек. Результат: {len(merged)} строк")
            return merged, exec_time

        except Exception as e:
            exec_time = time.time() - start_time
            error_msg = f"{str(e)} (Время выполнения: {exec_time:.2f} сек.)"
            print(f"Ошибка выполнения: {error_msg}")
            raise RuntimeError(error_msg) from e
        finally:
            # Закрытие соединений
            for conn in self.connections.values():
                if conn and not conn.closed:
                    conn.close()

    def _prepare_join_conditions(self, parsed, table_info):
        """Подготавливает условия JOIN для фильтрации данных перед объединением"""
        join_conditions = defaultdict(list)
        
        for join in parsed.get('joins', []):
            table = join['table']
            condition = join['condition']
            
            # Получаем алиас присоединяемой таблицы
            join_alias = join.get('alias') or table.split('.')[-1]
            
            # Находим основную таблицу (из FROM)
            main_table_alias = next(iter(table_info.values()))['alias']
            
            # Парсим условие JOIN
            comparisons = [c.strip() for c in re.split(r'\bAND\b|\bOR\b', condition, flags=re.IGNORECASE) if c.strip()]
            for comp in comparisons:
                if any(op in comp for op in ['=', '!=', '<', '>']):
                    # Разделяем на левую и правую части
                    for op in ['=', '!=', '<', '>']:
                        if op in comp:
                            left, right = [p.strip() for p in comp.split(op, 1)]
                            break
                    
                    # Определяем какая часть относится к какой таблице
                    if main_table_alias in left and join_alias in right:
                        local_col = right.split('.')[-1]
                        remote_col = left.split('.')[-1]
                        op = op  # сохраняем оператор
                    elif main_table_alias in right and join_alias in left:
                        local_col = left.split('.')[-1]
                        remote_col = right.split('.')[-1]
                        # Инвертируем оператор если нужно
                        op = {'=':'=', '!=':'!=', '<':'>', '>':'<'}[op]
                    else:
                        continue
                    
                    join_conditions[table].append({
                        'local_column': local_col,
                        'remote_column': remote_col,
                        'operator': op
                    })
        
        return join_conditions
    
    def _get_applicable_join_rules(self, table_info):
        """Возвращает JOIN правила, применимые к текущим таблицам"""
        applicable_rules = []
        tables = list(table_info.keys())
        
        for rule in self.join_config:
            # Проверяем, что все таблицы из правила присутствуют в запросе
            if all(table in tables for table in rule['tables']):
                applicable_rules.append(rule)
        
        return applicable_rules

    def _extract_table_where(self, where_clause, table_alias):
        """Извлекает условия WHERE относящиеся к конкретной таблице"""
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

    def _get_columns_for_table(self, columns, table_alias, full_table):
        """Определяет какие колонки запрашивать для конкретной таблицы"""
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

    def _get_join_keys(self, parsed, table_info, current_table, available_columns):
        """Определяет ключи для объединения таблиц"""
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

    def _extract_join_conditions(self, parsed, current_alias, available_columns):
        """Извлекает условия JOIN для текущей таблицы"""
        join_conditions = {'left_keys': [], 'right_keys': []}
        
        for join in parsed.get('joins', []):
            if join['table'].split('.')[-1] == current_alias or (join.get('alias') and join['alias'] == current_alias):
                condition = join['condition']
                comparisons = [c.strip() for c in re.split(r'\bAND\b|\bOR\b', condition, flags=re.IGNORECASE) if c.strip()]
                
                for comp in comparisons:
                    if '=' in comp:
                        left, right = [p.strip() for p in comp.split('=', 1)]
                        
                        # Определяем, какая часть относится к текущей таблице
                        left_table = left.split('.')[0] if '.' in left else None
                        right_table = right.split('.')[0] if '.' in right else None
                        
                        if left_table == current_alias and right in available_columns:
                            join_conditions['right_keys'].append(left.split('.')[-1])
                            join_conditions['left_keys'].append(right)
                        elif right_table == current_alias and left in available_columns:
                            join_conditions['left_keys'].append(left)
                            join_conditions['right_keys'].append(right.split('.')[-1])
        
        return join_conditions if join_conditions['left_keys'] else None

    def _parse_join_condition(self, condition):
        """Парсит условие JOIN и возвращает пары (левая_колонка, правая_колонка)"""
        comparisons = [c.strip() for c in re.split(r'\bAND\b|\bOR\b', condition, flags=re.IGNORECASE) if c.strip()]
        for comp in comparisons:
            if '=' in comp:
                left, right = [p.strip() for p in comp.split('=', 1)]
                return left, right
        return None, None

    def _prepare_where_condition(self, where_clause, available_columns):
        """Подготавливает условие WHERE для использования в pandas"""
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
    
    def _apply_where_manually(self, df, where_condition):
        """Применяет условие WHERE вручную, если query() не сработал"""
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
                print(f"Ошибка обработки условия {cond}: {str(e)}")
                continue
        
        return df[mask]

    @staticmethod
    def _split_where_conditions(where_clause):  # <-- УБРАТЬ self
        """Надежное разбиение условий WHERE без сложных регулярных выражений"""
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
    
    def query_database(self, connection_name, schema, table, columns):
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
            raise Exception(f"Ошибка при запросе к {schema}.{table}: {str(e)}")

    def _merge_tables(self, dfs, parsed):

        """Объединяет таблицы согласно порядку в запросе и условиям JOIN"""
        if not dfs:
            return pd.DataFrame()
        
        # Если есть явные JOIN в запросе, используем их
        if parsed.get('joins'):
            base_table = list(parsed['tables'])[0]
            merged = dfs[base_table]
            
            # Обрабатываем JOIN в порядке их указания в запросе
            for join_info in parsed['joins']:
                table_name = join_info['table']
                alias = join_info['alias'] or table_name.split('.')[-1]
                condition = join_info['condition']
                
                # Парсим условие JOIN
                if '=' in condition:
                    left_col, right_col = condition.split('=', 1)
                    left_col = left_col.strip()
                    right_col = right_col.strip()
                    
                    # Определяем таблицы для колонок
                    left_table = base_table.split('.')[-1] if '.' in base_table else base_table
                    right_table = table_name.split('.')[-1]
                    
                    # Форматируем имена колонок с префиксами таблиц
                    left_col_full = f"{left_table}.{left_col.split('.')[-1]}" if '.' not in left_col else left_col
                    right_col_full = f"{right_table}.{right_col.split('.')[-1]}" if '.' not in right_col else right_col
                    
                    # Выполняем JOIN
                    try:
                        merged = pd.merge(
                            merged,
                            dfs[table_name],
                            left_on=left_col_full,
                            right_on=right_col_full,
                            how='inner'
                        )
                        # Обновляем базовую таблицу для следующего JOIN
                        base_table = table_name
                    except KeyError as e:
                        self.log(f"Ошибка объединения: {str(e)}", error=True)
                        continue
            return merged
        
        """Объединяет таблицы согласно конфигурации JOIN"""
        if not dfs:
            return pd.DataFrame()
        
        # Создаем копию словаря таблиц для работы
        remaining_dfs = dfs.copy()
        merged_dfs = []
        
        # Применяем предопределенные правила JOIN
        for rule in self.join_config:
            try:
                # Проверяем, есть ли все таблицы из правила в запросе
                rule_tables = set(rule['tables'])
                if not rule_tables.issubset(set(remaining_dfs.keys())):
                    continue
                
                # Объединяем таблицы по правилу
                base_table = rule['tables'][0]
                merged = remaining_dfs[base_table]
                
                for table in rule['tables'][1:]:
                    if table not in remaining_dfs:
                        continue
                        
                    how = rule['join_type']
                    # Формируем имя колонки для соединения
                    join_key = rule['key']
                    
                    # Проверяем наличие ключа в обеих таблицах
                    base_key = f"{base_table.split('.')[-1]}.{join_key}"
                    table_key = f"{table.split('.')[-1]}.{join_key}"
                    
                    if base_key not in merged.columns:
                        raise KeyError(f"Ключ {base_key} не найден в таблице {base_table}")
                    if table_key not in remaining_dfs[table].columns:
                        raise KeyError(f"Ключ {table_key} не найден в таблице {table}")
                    
                    merged = pd.merge(
                        merged, 
                        remaining_dfs[table], 
                        left_on=base_key, 
                        right_on=table_key, 
                        how=how
                    )
                    # Удаляем объединенную таблицу
                    del remaining_dfs[table]
                
                # Удаляем базовую таблицу
                del remaining_dfs[base_table]
                
                # Добавляем объединенный результат
                merged_dfs.append(merged)
                
            except Exception as e:
                print(f"Ошибка объединения по правилу: {str(e)}")
                continue
        
        # Объединяем все результаты
        if merged_dfs and remaining_dfs:
            final_merged = pd.concat(merged_dfs + list(remaining_dfs.values()), axis=1)
        elif merged_dfs:
            final_merged = pd.concat(merged_dfs, axis=1)
        elif remaining_dfs:
            final_merged = pd.concat(list(remaining_dfs.values()), axis=1)
        else:
            return pd.DataFrame()
        
        return final_merged
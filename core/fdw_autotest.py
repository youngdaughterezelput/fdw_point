import pytest
from unittest.mock import MagicMock, patch, call
import os
import json
import psycopg2
import pandas as pd
from fdw_manager import VirtualFDWManager

class TestVirtualFDWManager:
    @pytest.fixture
    def manager(self):
        """Фикстура для создания экземпляра менеджера с моками"""
        with patch('fdw_manager.security.AuthManager.get_credentials') as mock_auth, \
             patch('psycopg2.connect') as mock_connect:
            
            mock_auth.return_value = ('test_user', 'test_pass')
            mock_conn = MagicMock()
            mock_connect.return_value = mock_conn
            
            m = VirtualFDWManager()
            m.connection_params = {'db1': {'host': 'host1', 'dbname': 'db1'}}
            m.table_mapping = {
                'public.users': 'db1',
                'public.orders': 'db1'
            }
            m.join_config = [{
                'tables': ['public.users', 'public.orders'],
                'key': 'user_id',
                'join_type': 'inner'
            }]
            
            yield m

    def test_load_save_config(self, tmp_path):
        """Тестирование загрузки/сохранения конфигурации"""
        # 1. Подготовка тестового .env файла
        env_path = tmp_path / ".env"
        config_data = {
            "CONNECTIONS": json.dumps({"db1": {"host": "localhost"}}),
            "TABLE_MAPPINGS": json.dumps({"table1": "db1"}),
            "JOIN_CONFIG": json.dumps([{"key": "id"}])
        }
        with open(env_path, 'w') as f:
            for k, v in config_data.items():
                f.write(f"{k}={v}\n")
        
        # 2. Тестирование загрузки
        with patch('fdw_manager.load_dotenv'), \
             patch('fdw_manager.os.getenv') as mock_getenv:
            
            mock_getenv.side_effect = lambda k, d=None: config_data.get(k, d)
            
            m = VirtualFDWManager()
            m.load_env_config()
            
            assert m.connection_params == {"db1": {"host": "localhost"}}
            assert m.table_mapping == {"table1": "db1"}
            assert m.join_config == [{"key": "id"}]

        # 3. Тестирование сохранения
        with patch('builtins.open') as mock_open:
            m.save_env_config()
            mock_open.assert_called()
            written_content = mock_open().write.call_args[0][0]
            assert "CONNECTIONS=" in written_content
            assert "TABLE_MAPPINGS=" in written_content

    def test_parse_sql(self, manager):
        """Тестирование парсера SQL запросов"""
        # 1. Простой запрос
        parsed = manager.parse_sql("SELECT id, name FROM public.users")
        assert parsed['columns'] == ['id', 'name']
        assert parsed['tables'] == {'public.users'}
        
        # 2. Запрос с JOIN
        sql = """
        SELECT u.id, o.product 
        FROM public.users u 
        JOIN public.orders o ON u.id = o.user_id
        WHERE u.age > 30
        """
        parsed = manager.parse_sql(sql)
        assert parsed['tables'] == {'public.users', 'public.orders'}
        assert parsed['aliases'] == {'u': 'public.users', 'o': 'public.orders'}
        assert len(parsed['joins']) == 1
        assert parsed['where'] == 'u.age > 30'

    def test_execute_query_single_table(self, manager):
        """Тест запроса к одной таблице"""
        # 1. Мокирование курсора и результатов
        mock_cursor = MagicMock()
        mock_cursor.description = [('id',), ('name',)]
        mock_cursor.fetchall.return_value = [(1, 'Alice'), (2, 'Bob')]
        manager.connections['db1'].cursor.return_value = mock_cursor
        
        # 2. Выполнение запроса
        result, _ = manager.execute_query("SELECT * FROM public.users")
        
        # 3. Проверки
        assert list(result.columns) == ['users.id', 'users.name']
        assert result.shape == (2, 2)
        mock_cursor.execute.assert_called_once_with("SELECT * FROM public.users", None)

    def test_execute_query_join(self, manager):
        """Тест запроса с JOIN через предопределенные правила"""
        # 1. Мокирование данных для двух таблиц
        mock_cursor = MagicMock()
        mock_cursor.description = [('id',), ('user_id',), ('product',)]
        
        # Первый вызов - для users
        mock_cursor.fetchall.side_effect = [
            [(1, 'Alice')],  # users
            [(1, 1, 'Book'), (2, 1, 'Pen')]  # orders
        ]
        manager.connections['db1'].cursor.return_value = mock_cursor
        
        # 2. Выполнение запроса
        query = "SELECT * FROM public.users, public.orders"
        result, _ = manager.execute_query(query)
        
        # 3. Проверки
        assert list(result.columns) == [
            'users.id', 'users.name', 
            'orders.id', 'orders.user_id', 'orders.product'
        ]
        assert result.shape == (2, 5)
        
        # Проверка JOIN-условия в запросе
        orders_call = mock_cursor.execute.call_args_list[1]
        assert "user_id IN %s" in orders_call[0][0]
        assert orders_call[1] == ((1,),)  # Переданные параметры

    def test_where_processing(self, manager):
        """Тест обработки условий WHERE"""
        # 1. Мокирование
        mock_cursor = MagicMock()
        mock_cursor.description = [('id',), ('name',)]
        mock_cursor.fetchall.return_value = [(1, 'Alice')]
        manager.connections['db1'].cursor.return_value = mock_cursor
        
        # 2. Запрос с WHERE
        query = "SELECT * FROM public.users WHERE users.name = 'Alice'"
        result, _ = manager.execute_query(query)
        
        # 3. Проверка
        assert result.iloc[0]['users.name'] == 'Alice'
        assert "name = 'Alice'" in mock_cursor.execute.call_args[0][0]

    def test_error_handling(self, manager):
        """Тест обработки ошибок"""
        # 1. Имитация ошибки БД
        manager.connections['db1'].cursor.side_effect = Exception("DB error")
        
        # 2. Выполнение запроса
        with pytest.raises(RuntimeError) as e:
            manager.execute_query("SELECT * FROM invalid_table")
        
        # 3. Проверка сообщения
        assert "DB error" in str(e.value)
import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext
#from .dialogs_main import TableMappingDialog, JoinRuleDialog, MappingDialog
from hfpoint.core.security import AuthManager
import re

class TableMappingDialog(tk.Toplevel):
    """Диалог для добавления/редактирования маппинга таблиц"""
    def __init__(self, parent, connections, table=None, connection=None):
        super().__init__(parent)
        self.title("Добавление маппинга" if not table else "Редактирование маппинга")
        self.result = None
        
        ttk.Label(self, text="Таблица (схема.таблица):").grid(row=0, column=0, padx=5, pady=5, sticky='e')
        self.table_entry = ttk.Entry(self)
        self.table_entry.grid(row=0, column=1, padx=5, pady=5, sticky='we')
        
        ttk.Label(self, text="Подключение:").grid(row=1, column=0, padx=5, pady=5, sticky='e')
        self.conn_combo = ttk.Combobox(self, values=list(connections))
        self.conn_combo.grid(row=1, column=1, padx=5, pady=5, sticky='we')
        
        if table:
            self.table_entry.insert(0, table)
            self.conn_combo.set(connection)
        
        btn_frame = ttk.Frame(self)
        btn_frame.grid(row=2, column=0, columnspan=2, pady=10)
        
        ttk.Button(btn_frame, text="OK", command=self.save).pack(side=tk.LEFT, padx=5)
        ttk.Button(btn_frame, text="Отмена", command=self.destroy).pack(side=tk.LEFT, padx=5)
    
    def save(self):
        table = self.table_entry.get().strip()
        connection = self.conn_combo.get().strip()
        
        if not table or '.' not in table:
            messagebox.showerror("Ошибка", "Введите таблицу в формате 'схема.таблица'")
            return
            
        if not connection:
            messagebox.showerror("Ошибка", "Выберите подключение")
            return
            
        self.result = (table, connection)
        self.destroy()

class JoinRuleDialog(tk.Toplevel):
    """Диалог для добавления/редактирования правил JOIN"""
    def __init__(self, parent, all_tables, key=None, tables=None, join_type='inner'):
        super().__init__(parent)
        self.title("Добавление правила JOIN" if not key else "Редактирование правила JOIN")
        self.result = None
        self.all_tables = all_tables
        
        ttk.Label(self, text="Ключевое поле:").grid(row=0, column=0, padx=5, pady=5, sticky='e')
        self.key_entry = ttk.Entry(self)
        self.key_entry.grid(row=0, column=1, padx=5, pady=5, sticky='we')
        
        ttk.Label(self, text="Тип JOIN:").grid(row=1, column=0, padx=5, pady=5, sticky='e')
        self.join_type_combo = ttk.Combobox(self, values=['inner', 'left', 'right', 'full'])
        self.join_type_combo.grid(row=1, column=1, padx=5, pady=5, sticky='we')
        self.join_type_combo.set('inner')
        
        ttk.Label(self, text="Таблицы (через запятую):").grid(row=2, column=0, padx=5, pady=5, sticky='ne')
        self.tables_text = scrolledtext.ScrolledText(self, height=8, width=50)
        self.tables_text.grid(row=2, column=1, padx=5, pady=5, sticky='nsew')
        
        # Подсказка с доступными таблицами
        ttk.Label(self, text="Доступные таблицы:").grid(row=3, column=0, padx=5, pady=5, sticky='ne')
        tables_list = "\n".join(all_tables)
        ttk.Label(self, text=tables_list, justify=tk.LEFT).grid(row=3, column=1, padx=5, pady=5, sticky='nw')
        
        if key:
            self.key_entry.insert(0, key)
            self.join_type_combo.set(join_type)
            self.tables_text.insert(tk.END, ", ".join(tables))
        
        btn_frame = ttk.Frame(self)
        btn_frame.grid(row=4, column=0, columnspan=2, pady=10)
        
        ttk.Button(btn_frame, text="OK", command=self.save).pack(side=tk.LEFT, padx=5)
        ttk.Button(btn_frame, text="Отмена", command=self.destroy).pack(side=tk.LEFT, padx=5)
        
        self.grid_rowconfigure(2, weight=1)
        self.grid_columnconfigure(1, weight=1)
    
    def save(self):
        key = self.key_entry.get().strip()
        join_type = self.join_type_combo.get().strip()
        tables_text = self.tables_text.get("1.0", tk.END).strip()
        
        if not key:
            messagebox.showerror("Ошибка", "Введите ключевое поле")
            return
            
        if not tables_text:
            messagebox.showerror("Ошибка", "Введите таблицы")
            return
            
        # Разделение таблиц и очистка
        tables = [t.strip() for t in tables_text.split(',')]
        tables = [t for t in tables if t]  # Удаление пустых
        
        # Проверка существования таблиц
        invalid_tables = [t for t in tables if t not in self.all_tables]
        if invalid_tables:
            messagebox.showerror(
                "Ошибка", 
                f"Неизвестные таблицы: {', '.join(invalid_tables)}"
            )
            return
            
        self.result = {
            'key': key,
            'tables': tables,
            'join_type': join_type
        }
        self.destroy()

class SQLText(scrolledtext.ScrolledText):
    """Кастомный текстовый редактор с подсветкой SQL"""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.configure(font=('Courier New', 12), wrap=tk.WORD)
        self._setup_tags()
        self.bind('<KeyRelease>', self._highlight)
        
    def _setup_tags(self):
        # Цвета для различных элементов SQL
        self.tag_configure('keyword', foreground='#CC7A00', font=('Courier New', 12, 'bold'))
        self.tag_configure('string', foreground='#007300')
        self.tag_configure('comment', foreground='#666666')
        self.tag_configure('function', foreground='#0000CC')
        self.tag_configure('operator', foreground='#AA22FF')
        
    def _highlight(self, event=None):
        # Очищаем предыдущие теги
        for tag in ['keyword', 'string', 'comment', 'function', 'operator']:
            self.tag_remove(tag, '1.0', tk.END)

        # Регулярные выражения для элементов SQL
        patterns = [
            (r'\b(SELECT|FROM|WHERE|JOIN|INNER|LEFT|RIGHT|FULL|OUTER|'
             r'GROUP BY|HAVING|ORDER BY|LIMIT|AS|AND|OR|NOT|NULL|'
             r'INSERT|UPDATE|DELETE|CREATE|TABLE|INDEX|VIEW|'
             r'EXISTS|BETWEEN|LIKE|IN|IS)\b', 'keyword'),
            (r"'[^']*'", 'string'),
            (r'--.*$', 'comment'),
            (r'\b(COUNT|SUM|AVG|MIN|MAX)\b', 'function'),
            (r'[=<>!+*/%-]', 'operator')
        ]

        # Применяем подсветку
        text = self.get('1.0', tk.END)
        for pattern, tag in patterns:
            matches = re.finditer(pattern, text, re.IGNORECASE | re.MULTILINE)
            for match in matches:
                start = f"1.0 + {match.start()}c"
                end = f"1.0 + {match.end()}c"
                self.tag_add(tag, start, end)

class EditConnectionWindow(tk.Toplevel):
    def __init__(self, parent, fdw, mode='add', connection_name=None):
        super().__init__(parent)
        self.fdw = fdw
        self.mode = mode
        self.connection_name = connection_name
        self.transient(parent)
        self.grab_set()
        
        self.title(f"{'Редактирование' if mode == 'edit' else 'Добавление'} подключения")
        self._create_widgets()
        self._load_existing_data()

    def _create_widgets(self):
        self.frame = ttk.Frame(self, padding=10)
        self.frame.pack(fill=tk.BOTH, expand=True)

        # Поля для ввода
        fields = [
            ('Имя подключения:', 'name'),
            ('Хост:', 'host'),
            ('Порт:', 'port'),
            ('Имя БД:', 'dbname'),
            ('Пользователь:', 'user'),
            ('Пароль:', 'password')
        ]

        self.entries = {}
        for row, (label, field) in enumerate(fields):
            ttk.Label(self.frame, text=label).grid(row=row, column=0, sticky='e', padx=5, pady=5)
            entry = ttk.Entry(self.frame) if field != 'password' else ttk.Entry(self.frame, show='*')
            entry.grid(row=row, column=1, sticky='we', padx=5, pady=5)
            self.entries[field] = entry

        # Чекбокс для сохранения пароля
        self.save_pass_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(
            self.frame, 
            text="Сохранить пароль", 
            variable=self.save_pass_var
        ).grid(row=len(fields), column=0, columnspan=2, pady=5)

        # Кнопки
        btn_frame = ttk.Frame(self.frame)
        btn_frame.grid(row=len(fields)+1, column=0, columnspan=2, pady=10)
        
        ttk.Button(btn_frame, text="Сохранить", command=self.save).pack(side=tk.LEFT, padx=5)
        ttk.Button(btn_frame, text="Отмена", command=self.destroy).pack(side=tk.LEFT, padx=5)

        # Настройка расширения колонок
        self.frame.columnconfigure(1, weight=1)

    def _load_existing_data(self):
        if self.mode == 'edit' and self.connection_name:
            params = self.fdw.connection_params.get(self.connection_name, {})
            user, password = AuthManager.get_credentials(self.connection_name)
            
            self.entries['name'].insert(0, self.connection_name)
            self.entries['host'].insert(0, params.get('host', ''))
            self.entries['port'].insert(0, params.get('port', '5432'))
            self.entries['dbname'].insert(0, params.get('dbname', ''))
            self.entries['user'].insert(0, user or '')
            self.entries['password'].insert(0, password or '')

    def _validate(self):
        errors = []
        name = self.entries['name'].get().strip()
        host = self.entries['host'].get().strip()
        port = self.entries['port'].get().strip()
        dbname = self.entries['dbname'].get().strip()
        user = self.entries['user'].get().strip()
        password = self.entries['password'].get()

        if not name:
            errors.append("Имя подключения обязательно")
        if self.mode == 'add' and name in self.fdw.connection_params:
            errors.append("Подключение с таким именем уже существует")
        if not host:
            errors.append("Хост обязателен")
        if not port.isdigit():
            errors.append("Порт должен быть числом")
        if not dbname:
            errors.append("Имя базы данных обязательно")
        if not user:
            errors.append("Пользователь обязателен")
        if not password:
            errors.append("Пароль обязателен")

        return errors

    def save(self):
        errors = self._validate()
        if errors:
            messagebox.showerror("Ошибка", "\n".join(errors))
            return

        name = self.entries['name'].get().strip()
        params = {
            'host': self.entries['host'].get().strip(),
            'port': int(self.entries['port'].get().strip()),
            'dbname': self.entries['dbname'].get().strip()
        }
        user = self.entries['user'].get().strip()
        password = self.entries['password'].get()

        # Гарантируем, что connection_params - словарь
        if not isinstance(self.fdw.connection_params, dict):
            self.fdw.connection_params = {}
            
        # Обновление параметров подключения
        if self.mode == 'edit' and self.connection_name != name:
            if self.connection_name in self.fdw.connection_params:
                del self.fdw.connection_params[self.connection_name]
        
        self.fdw.connection_params[name] = params

        # Сохраняем параметры подключения
        self.fdw.connection_params[name] = params
        
        # Всегда сохраняем учетные данные в AuthManager
        AuthManager.save_credentials(name, user, password)
        
        # Обновляем кеш в VirtualFDWManager
        if hasattr(self.fdw, 'saved_credentials'):
            self.fdw.saved_credentials[name] = {
                'user': user,
                'password': password
            }

        # Сохранение учетных данных
        if self.save_pass_var.get():
            AuthManager.save_credentials(name, user, password)
        else:
            AuthManager.delete_credentials(name)

        # Сохранение всей конфигурации
        self.fdw.save_env_config()
        
        # Обновление интерфейса
        if hasattr(self.master, 'update_connections'):
            self.master.update_connections()
        self.destroy()

class ConnectionWindow(tk.Toplevel):
    """Окно для ввода учетных данных"""
    def __init__(self, parent, connection_name, callback):
        super().__init__(parent)
        self.title(f"Аутентификация: {connection_name}")
        self.callback = callback
        self.connection_name = connection_name
        
        ttk.Label(self, text="Логин:").grid(row=0, column=0, padx=5, pady=5)
        self.user_entry = ttk.Entry(self)
        self.user_entry.grid(row=0, column=1, padx=5, pady=5)
        
        ttk.Label(self, text="Пароль:").grid(row=1, column=0, padx=5, pady=5)
        self.pass_entry = ttk.Entry(self, show="*")
        self.pass_entry.grid(row=1, column=1, padx=5, pady=5)
        
        self.save_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(self, text="Сохранить подключение", variable=self.save_var).grid(row=2, columnspan=2)
        
        ttk.Button(self, text="Подключиться", command=self.authenticate).grid(row=3, columnspan=2, pady=5)

        # Автозаполнение сохраненных данных
        saved_user, saved_pass = AuthManager.get_credentials(connection_name)
        if saved_user:
            self.user_entry.insert(0, saved_user)
            self.pass_entry.insert(0, saved_pass)
            self.save_var.set(True)
    
    def authenticate(self):
        user = self.user_entry.get()
        password = self.pass_entry.get()
        if self.save_var.get():
            AuthManager.save_credentials(self.connection_name, user, password)
        self.callback(user, password)
        self.destroy()

class MappingDialog(tk.Toplevel):
    """Диалог для добавления/редактирования маппинга"""
    def __init__(self, parent, connections, schema=None, connection=None):
        super().__init__(parent)
        self.title("Добавление маппинга" if not schema else "Редактирование маппинга")
        self.result = None
        self.connections = list(connections)
        
        # Схема
        ttk.Label(self, text="Схема:").grid(row=0, column=0, padx=5, pady=5, sticky='e')
        self.schema_entry = ttk.Entry(self)
        self.schema_entry.grid(row=0, column=1, padx=5, pady=5, sticky='we')
        
        # Подключение
        ttk.Label(self, text="Подключение:").grid(row=1, column=0, padx=5, pady=5, sticky='e')
        self.conn_combo = ttk.Combobox(self, values=self.connections)
        self.conn_combo.grid(row=1, column=1, padx=5, pady=5, sticky='we')
        
        # Заполнение данных при редактировании
        if schema:
            self.schema_entry.insert(0, schema)
            self.conn_combo.set(connection)
        
        # Кнопки
        btn_frame = ttk.Frame(self)
        btn_frame.grid(row=2, column=0, columnspan=2, pady=10)
        
        ttk.Button(btn_frame, text="OK", command=self.save).pack(side=tk.LEFT, padx=5)
        ttk.Button(btn_frame, text="Отмена", command=self.destroy).pack(side=tk.LEFT, padx=5)
    
    def save(self):
        schema = self.schema_entry.get().strip()
        connection = self.conn_combo.get().strip()
        
        if not schema:
            messagebox.showerror("Ошибка", "Введите название схемы")
            return
            
        if not connection or connection not in self.connections:
            messagebox.showerror("Ошибка", "Выберите подключение")
            return
            
        # Нормализация регистра
        schema = schema.lower()
        
        self.result = (schema, connection)
        self.destroy()

import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext
from hfpoint.gui.dialogs_main import TableMappingDialog, JoinRuleDialog, MappingDialog

class TableMappingWindow(tk.Toplevel):
    """Окно для управления маппингом таблиц"""
    def __init__(self, parent, fdw):
        super().__init__(parent)
        self.fdw = fdw
        self.title("Управление маппингом таблиц")
        self.geometry("700x400")
        self._create_widgets()
        self._load_mappings()
    
    def _create_widgets(self):
        self.tree = ttk.Treeview(self, columns=('table', 'connection'), show='headings')
        self.tree.heading('table', text="Таблица (схема.таблица)")
        self.tree.heading('connection', text="Подключение")
        self.tree.column('table', width=300)
        self.tree.column('connection', width=200)
        self.tree.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        btn_frame = ttk.Frame(self)
        btn_frame.pack(fill=tk.X, padx=10, pady=5)
        
        ttk.Button(btn_frame, text="Добавить", command=self.add_mapping).pack(side=tk.LEFT, padx=5)
        ttk.Button(btn_frame, text="Редактировать", command=self.edit_mapping).pack(side=tk.LEFT, padx=5)
        ttk.Button(btn_frame, text="Удалить", command=self.delete_mapping).pack(side=tk.LEFT, padx=5)
        ttk.Button(btn_frame, text="Сохранить", command=self.save_mappings).pack(side=tk.RIGHT)
    
    def _load_mappings(self):
        self.tree.delete(*self.tree.get_children())
        for table, connection in self.fdw.table_mapping.items():
            self.tree.insert('', 'end', values=(table, connection))
    
    def add_mapping(self):
        dialog = TableMappingDialog(self, list(self.fdw.connection_params.keys()))
        self.wait_window(dialog)
        
        if dialog.result:
            table, connection = dialog.result
            self.fdw.table_mapping[table] = connection
            self._load_mappings()
    
    def edit_mapping(self):
        selected = self.tree.selection()
        if not selected: return
            
        item = self.tree.item(selected[0])
        table, connection = item['values']
        
        dialog = TableMappingDialog(
            self, 
            self.fdw.connection_params.keys(), 
            table, 
            connection
        )
        self.wait_window(dialog)
        
        if dialog.result:
            new_table, new_connection = dialog.result
            if new_table != table:
                del self.fdw.table_mapping[table]
            self.fdw.table_mapping[new_table] = new_connection
            self._load_mappings()
    
    def delete_mapping(self):
        selected = self.tree.selection()
        if not selected: return
            
        item = self.tree.item(selected[0])
        table, _ = item['values']
        
        if messagebox.askyesno("Подтверждение", f"Удалить маппинг для '{table}'?"):
            del self.fdw.table_mapping[table]
            self._load_mappings()
    
    def save_mappings(self):
        try:
            self.fdw.save_env_config()
            messagebox.showinfo("Сохранено", "Маппинг таблиц успешно сохранен")
            self.destroy()
        except Exception as e:
            messagebox.showerror("Ошибка", f"Ошибка сохранения: {str(e)}")


class JoinRulesWindow(tk.Toplevel):
    """Окно для управления правилами JOIN"""
    def __init__(self, parent, fdw):
        super().__init__(parent)
        self.fdw = fdw
        self.title("Управление правилами JOIN")
        self.geometry("800x500")
        self._create_widgets()
        self._load_rules()
    
    def _create_widgets(self):
        self.tree = ttk.Treeview(self, columns=('key', 'tables', 'join_type'), show='headings')
        self.tree.heading('key', text="Ключевое поле")
        self.tree.heading('tables', text="Таблицы")
        self.tree.heading('join_type', text="Тип JOIN")
        self.tree.column('key', width=150)
        self.tree.column('tables', width=400)
        self.tree.column('join_type', width=100)
        self.tree.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        btn_frame = ttk.Frame(self)
        btn_frame.pack(fill=tk.X, padx=10, pady=5)
        
        ttk.Button(btn_frame, text="Добавить", command=self.add_rule).pack(side=tk.LEFT, padx=5)
        ttk.Button(btn_frame, text="Редактировать", command=self.edit_rule).pack(side=tk.LEFT, padx=5)
        ttk.Button(btn_frame, text="Удалить", command=self.delete_rule).pack(side=tk.LEFT, padx=5)
        ttk.Button(btn_frame, text="Сохранить", command=self.save_rules).pack(side=tk.RIGHT)
    
    def _load_rules(self):
        self.tree.delete(*self.tree.get_children())
        for rule in self.fdw.join_config:
            tables = ", ".join(rule['tables'])
            self.tree.insert('', 'end', values=(
                rule['key'],
                tables,
                rule['join_type']
            ))
    
    def add_rule(self):
        dialog = JoinRuleDialog(self, self._get_all_tables())
        self.wait_window(dialog)
        
        if dialog.result:
            self.fdw.join_config.append(dialog.result)
            self._load_rules()
    
    def edit_rule(self):
        selected = self.tree.selection()
        if not selected: return
            
        index = self.tree.index(selected[0])
        rule = self.fdw.join_config[index]
        
        dialog = JoinRuleDialog(
            self, 
            self._get_all_tables(),
            rule['key'],
            rule['tables'],
            rule['join_type']
        )
        self.wait_window(dialog)
        
        if dialog.result:
            self.fdw.join_config[index] = dialog.result
            self._load_rules()
    
    def delete_rule(self):
        selected = self.tree.selection()
        if not selected: return
            
        index = self.tree.index(selected[0])
        if messagebox.askyesno("Подтверждение", "Удалить это правило JOIN?"):
            del self.fdw.join_config[index]
            self._load_rules()
    
    def save_rules(self):
        try:
            self.fdw.save_env_config()
            messagebox.showinfo("Сохранено", "Правила JOIN успешно сохранены")
            self.destroy()
        except Exception as e:
            messagebox.showerror("Ошибка", f"Ошибка сохранения: {str(e)}")
    
    def _get_all_tables(self):
        """Получить все таблицы из маппинга"""
        return list(self.fdw.table_mapping.keys())
    

class SchemaMappingWindow(tk.Toplevel):
    """Окно для управления маппингом схем"""
    def __init__(self, parent, fdw):
        super().__init__(parent)
        self.fdw = fdw
        self.title("Управление маппингом схем")
        self.geometry("600x400")
        self._create_widgets()
        self._load_mappings()
    
    def _create_widgets(self):
        # Таблица для отображения маппинга
        self.tree = ttk.Treeview(self, columns=('schema', 'connection'), show='headings')
        self.tree.heading('schema', text="Схема")
        self.tree.heading('connection', text="Подключение")
        self.tree.column('schema', width=200)
        self.tree.column('connection', width=200)
        self.tree.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # Кнопки управления
        btn_frame = ttk.Frame(self)
        btn_frame.pack(fill=tk.X, padx=10, pady=5)
        
        ttk.Button(btn_frame, text="Добавить", command=self.add_mapping).pack(side=tk.LEFT, padx=5)
        ttk.Button(btn_frame, text="Редактировать", command=self.edit_mapping).pack(side=tk.LEFT, padx=5)
        ttk.Button(btn_frame, text="Удалить", command=self.delete_mapping).pack(side=tk.LEFT, padx=5)
        ttk.Button(btn_frame, text="Сохранить", command=self.save_mappings).pack(side=tk.RIGHT)
    
    def _load_mappings(self):
        """Загрузка маппингов в таблицу"""
        self.tree.delete(*self.tree.get_children())
        for schema, connection in self.fdw.schema_mapping.items():
            self.tree.insert('', 'end', values=(schema, connection))
    
    def add_mapping(self):
        """Добавление нового маппинга"""
        dialog = MappingDialog(self, list(self.fdw.connection_params.keys()))
        self.wait_window(dialog)
        
        if dialog.result:
            schema, connection = dialog.result
            
            # Проверяем, что подключение существует
            if connection not in self.fdw.connection_params:
                messagebox.showerror("Ошибка", f"Подключение {connection} не существует!")
                return
                
            self.fdw.schema_mapping[schema] = connection
            self._load_mappings()
            
            # Сразу сохраняем изменения
            try:
                self.fdw.save_env_config()
                print(f"Добавлен маппинг: {schema} -> {connection}")
            except Exception as e:
                messagebox.showerror("Ошибка", f"Ошибка сохранения: {str(e)}")
    
    def edit_mapping(self):
        """Редактирование выбранного маппинга"""
        selected = self.tree.selection()
        if not selected:
            return
            
        item = self.tree.item(selected[0])
        schema, connection = item['values']
        
        dialog = MappingDialog(self, self.fdw.connection_params.keys(), schema, connection)
        if dialog.result:
            new_schema, new_connection = dialog.result
            if new_schema != schema:
                del self.fdw.schema_mapping[schema]
            self.fdw.schema_mapping[new_schema] = new_connection
            self._load_mappings()
    
    def delete_mapping(self):
        """Удаление выбранного маппинга"""
        selected = self.tree.selection()
        if not selected:
            return
            
        item = self.tree.item(selected[0])
        schema, _ = item['values']
        
        if messagebox.askyesno("Подтверждение", f"Удалить маппинг для схемы '{schema}'?"):
            del self.fdw.schema_mapping[schema]
            self._load_mappings()
    
    def save_mappings(self):
        """Сохранение маппингов в .env"""
        try:
            # Сохраняем конфигурацию
            self.fdw.save_env_config()
            
            # Принудительно перезагружаем конфигурацию
            self.fdw.load_env_config()
            
            messagebox.showinfo("Сохранено", "Маппинг схем успешно сохранен")
            self.destroy()  # Закрываем окно после сохранения
        except Exception as e:
            messagebox.showerror("Ошибка", f"Ошибка сохранения: {str(e)}")
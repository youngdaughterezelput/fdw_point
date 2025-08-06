from datetime import datetime
import tkinter as tk
from tkinter import ttk, messagebox, filedialog, scrolledtext
import os

from dotenv import load_dotenv
import pandas as pd
from hfpoint.core.fdw_manager import VirtualFDWManager
from hfpoint.core.security import AuthManager
from .windows import TableMappingWindow, JoinRulesWindow, SchemaMappingWindow
from .dialogs_main import EditConnectionWindow, ConnectionWindow
from .widgets import SQLText
import uuid
from icon_manager import IconManager


class FDWGUI(tk.Tk):
    def __init__(self):
        super().__init__()
        load_dotenv() 
        # Инициализация менеджера иконок с передачей root
        self.icon_manager = IconManager()
        # Установка иконки
        self.icon_manager.set_icon(self)
        self.title("HF-Point")
        self.geometry("1100x700")
        env_path = os.path.abspath('.env')
        if not os.path.exists(env_path):
            with open(env_path, 'w') as f:
                f.write("CONNECTIONS={}\nMAPPINGS={}\n")
        
        self.fdw = VirtualFDWManager()
        self.current_data = None
        self.query_results = {}
        self.current_tree = None
        
        # Сначала создаём все виджеты
        self.main_frame = ttk.Frame(self)
        self.main_frame.pack(fill=tk.BOTH, expand=True)
        self._create_widgets()  # Теперь result_notebook точно будет создан
        
        # Затем настраиваем меню и горячие клавиши
        self._create_menu()
        self._create_toolbar()
        self._bind_hotkeys()
        
        # Остальные атрибуты
        self.explain_window = None
        self.connections_window = None
        self.mapping_window = None

    def _check_auth(self):
        """Проверка необходимости аутентификации"""
        for conn_name in self.fdw.connection_params:
            user, password = AuthManager.get_credentials(conn_name)
            if not user or not password:
                self._show_auth_window(conn_name)
                break
            else:
                # Предварительная инициализация подключения
                try:
                    self.fdw.get_connection(conn_name)
                except Exception as e:
                    self.log(f"Ошибка предварительного подключения: {str(e)}")

    def _show_auth_window(self, connection_name):
        def auth_callback(user, password):
            try:
                # Явная передача учетных данных
                conn = self.fdw.get_connection(connection_name, user, password)
                self.fdw.saved_credentials[connection_name] = {
                    'user': user,
                    'password': password
                }
            except Exception as e:
                messagebox.showerror("Ошибка", str(e))
                self._show_auth_window(connection_name)
        
        ConnectionWindow(self, connection_name, auth_callback)

    def _create_widgets(self):
        """Создает основные элементы интерфейса"""
        # SQL Editor
        editor_frame = ttk.LabelFrame(self.main_frame, text="SQL Editor")
        editor_frame.pack(fill=tk.X, padx=10, pady=5)
        self.editor = SQLText(editor_frame, height=15)
        self.editor.pack(fill=tk.BOTH, expand=True)

        # Notebook для результатов
        self.result_notebook = ttk.Notebook(self.main_frame)
        self.result_notebook.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)
        
        # Консоль
        console_frame = ttk.LabelFrame(self.main_frame, text="Execution Console")
        console_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)
        self.console = scrolledtext.ScrolledText(console_frame, height=8, wrap=tk.WORD)
        self.console.pack(fill=tk.BOTH, expand=True)

    def _create_result_tab(self, query_text=None):
        """Создает новую вкладку для отображения результатов"""
        if self.result_notebook is None:
            raise RuntimeError("Notebook не инициализирован. Сначала вызовите _create_widgets()")
        
        tab_id = str(uuid.uuid4())
        tab_frame = ttk.Frame(self.result_notebook)
        
        # Создаем Treeview с прокруткой
        tree_frame = ttk.Frame(tab_frame)
        tree_frame.pack(fill=tk.BOTH, expand=True)
        
        # Вертикальная прокрутка
        y_scroll = ttk.Scrollbar(tree_frame, orient=tk.VERTICAL)
        y_scroll.pack(side=tk.RIGHT, fill=tk.Y)
        
        # Горизонтальная прокрутка
        x_scroll = ttk.Scrollbar(tree_frame, orient=tk.HORIZONTAL)
        x_scroll.pack(side=tk.BOTTOM, fill=tk.X)
        
        tree = ttk.Treeview(
            tree_frame,
            yscrollcommand=y_scroll.set,
            xscrollcommand=x_scroll.set,
            selectmode='extended'  # Разрешаем множественное выделение
        )
        tree.pack(fill=tk.BOTH, expand=True)
        
        # Настройка команд прокрутки
        y_scroll.config(command=tree.yview)
        x_scroll.config(command=tree.xview)
        
        # Добавляем контекстное меню
        self._setup_tree_context_menu(tree)
        
        # Название вкладки
        tab_name = f"Query {len(self.result_notebook.tabs()) + 1}"
        if query_text:
            tab_name = query_text[:20] + "..." if len(query_text) > 20 else query_text
        
        self.result_notebook.add(tab_frame, text=tab_name)
        self.result_notebook.select(tab_frame)
        
        # Сохраняем ссылку на текущее дерево
        self.current_tree = tree  # Обновляем current_tree
        
        return tree
    
    def _display_results_in_tab(self, df, query_text=None):
        """Отображает результаты в новой вкладке"""
        tree = self._create_result_tab(query_text)
        
        try:
            # Очищаем предыдущие данные
            tree.delete(*tree.get_children())
            
            if df.empty:
                self.log("Нет данных для отображения", error=True)
                return

            # Настраиваем колонки
            tree["columns"] = list(df.columns)
            for col in df.columns:
                tree.heading(col, text=col, anchor=tk.W)
                tree.column(col, width=120, stretch=False, anchor=tk.W)
            
            # Вставляем данные
            for _, row in df.iterrows():
                tree.insert("", tk.END, values=list(row))
                
            # Сохраняем результат
            tab_id = self.result_notebook.tabs()[-1]
            self.query_results[tab_id] = {
                'tree': tree,
                'data': df,
                'query': query_text
            }

        except Exception as e:
            self.log(f"Ошибка отображения: {str(e)}", error=True)

    def _create_tooltip(self, widget, text):
        # Реализация всплывающих подсказок
        tooltip = tk.Toplevel(widget)
        tooltip.withdraw()
        tooltip.overrideredirect(True)
        
        def enter(event):
            x = widget.winfo_rootx() + widget.winfo_width() + 5
            y = widget.winfo_rooty() + (widget.winfo_height() // 2)
            tooltip.geometry(f"+{x}+{y}")
            tooltip.deiconify()
            tk.Label(tooltip, text=text, bg="#ffffe0", relief=tk.SOLID, borderwidth=1).pack()
        
        def leave(event):
            tooltip.withdraw()
        
        widget.bind("<Enter>", enter)
        widget.bind("<Leave>", leave)

    def _create_toolbar(self):
        toolbar = tk.Frame(self, bd=1, relief=tk.RAISED)
        
        # Иконки
        icons = [
            ("Выполнить запрос (Ctrl+Enter)", self.execute),
            ("План запроса (Ctrl+Shift+P)", self.explain),
            ("Очистить результаты", self.clear_results)
        ]
        
        for tooltip, command in icons:
            btn = ttk.Button(toolbar, text=tooltip.split()[0], command=command)
            btn.pack(side=tk.LEFT, padx=2, pady=2)
            self._create_tooltip(btn, tooltip)
        
        toolbar.pack(side=tk.TOP, fill=tk.X, before=self.main_frame)

    def _bind_hotkeys(self):
        """Настраиваем горячие клавиши"""
        self.bind("<Control-Return>", lambda e: self.execute())
        self.bind("<Control-P>", lambda e: self.explain())
        self.editor.bind("<Control-Return>", lambda e: self.execute())

    def _create_menu(self):
        menu_bar = tk.Menu(self)
        
        # File Menu
        file_menu = tk.Menu(menu_bar, tearoff=0)
        file_menu.add_command(label="Export to File", command=self.export_to_file)
        file_menu.add_separator()
        file_menu.add_command(label="Exit", command=self.destroy)
        menu_bar.add_cascade(label="File", menu=file_menu)

        # Execute Menu
        execute_menu = tk.Menu(menu_bar, tearoff=0)
        execute_menu.add_command(label="Execute (Ctrl+Enter)", command=self.execute)
        execute_menu.add_command(label="Execute Multiple", command=self.execute_multiple)
        execute_menu.add_command(label="Explain (Ctrl+Shift+P)", command=self.explain)
        execute_menu.add_command(label="Clear Results", command=self.clear_results)
        execute_menu.add_separator()
        execute_menu.add_command(label="Map Results", command=self.map_results)
        menu_bar.add_cascade(label="Execute", menu=execute_menu)

        # View Menu
        view_menu = tk.Menu(menu_bar, tearoff=0)
        view_menu.add_command(label="Show Connections", command=self.show_connections)
        view_menu.add_command(label="Show Table Mapping", command=self.show_table_mapping)
        view_menu.add_command(label="Show JOIN Rules", command=self.show_join_rules)
        menu_bar.add_cascade(label="View", menu=view_menu)

        # Debug Menu
        debug_menu = tk.Menu(menu_bar, tearoff=0)
        debug_menu.add_command(label="Show Config", command=self.show_config)
        menu_bar.add_cascade(label="Debug", menu=debug_menu)

        self.config(menu=menu_bar)

    def show_table_mapping(self):
        """Окно управления маппингом таблиц"""
        TableMappingWindow(self, self.fdw)
    
    def show_join_rules(self):
        """Окно управления правилами JOIN"""
        JoinRulesWindow(self, self.fdw)

    def show_config(self):
        """Показать текущую конфигурацию"""
        env_path = os.path.abspath('.env')
        content = ""
        
        try:
            with open(env_path, 'r') as f:
                content = f.read()
        except Exception as e:
            content = f"Ошибка чтения .env: {str(e)}"
        
        config_info = f"Путь к .env: {env_path}\n\n"
        config_info += f"Содержимое .env:\n{content}\n\n"
        config_info += f"Текущие подключения: {self.fdw.connection_params}\n"
        config_info += f"Текущий маппинг схем: {self.fdw.schema_mapping}"
        
        messagebox.showinfo("Конфигурация", config_info)

    def execute_multiple(self):
        """Выполняет несколько SQL-запросов"""
        query_text = self.editor.get("1.0", tk.END).strip()
        if not query_text:
            self.log("Нет запроса для выполнения", error=True)
            return
        
        queries = [q.strip() for q in query_text.split(';') if q.strip()]
        
        if len(queries) == 1:
            self.execute()
            return
        
        for query in queries:
            try:
                result, exec_time = self.fdw.execute_query(query)
                self._display_results_in_tab(result, query)
                self.log(f"Запрос выполнен за {exec_time:.2f} сек. Найдено строк: {len(result)}")
            except Exception as e:
                self.log(f"Ошибка выполнения запроса '{query[:20]}...': {str(e)}", error=True)
                self._display_results_in_tab(pd.DataFrame({'Error': [str(e)]}), query)

    def map_results(self):
        """Маппинг результатов из разных вкладок"""
        if len(self.query_results) < 2:
            messagebox.showwarning("Предупреждение", "Для маппинга нужно как минимум 2 набора результатов")
            return
        
        dialog = tk.Toplevel(self)
        dialog.title("Map Query Results")
        dialog.geometry("500x300")
        
        # Выбор таблиц и ключей
        ttk.Label(dialog, text="First Result:").grid(row=0, column=0, padx=5, pady=5, sticky='e')
        first_combo = ttk.Combobox(dialog, values=list(self.query_results.keys()))
        first_combo.grid(row=0, column=1, padx=5, pady=5, sticky='we')
        
        ttk.Label(dialog, text="Second Result:").grid(row=1, column=0, padx=5, pady=5, sticky='e')
        second_combo = ttk.Combobox(dialog, values=list(self.query_results.keys()))
        second_combo.grid(row=1, column=1, padx=5, pady=5, sticky='we')
        
        ttk.Label(dialog, text="First Key Column:").grid(row=2, column=0, padx=5, pady=5, sticky='e')
        first_key_combo = ttk.Combobox(dialog)
        first_key_combo.grid(row=2, column=1, padx=5, pady=5, sticky='we')
        
        ttk.Label(dialog, text="Second Key Column:").grid(row=3, column=0, padx=5, pady=5, sticky='e')
        second_key_combo = ttk.Combobox(dialog)
        second_key_combo.grid(row=3, column=1, padx=5, pady=5, sticky='we')
        
        ttk.Label(dialog, text="Join Type:").grid(row=4, column=0, padx=5, pady=5, sticky='e')
        join_type_combo = ttk.Combobox(dialog, values=['inner', 'left', 'right', 'outer'])
        join_type_combo.grid(row=4, column=1, padx=5, pady=5, sticky='we')
        join_type_combo.set('inner')
        
        # Кнопки
        btn_frame = ttk.Frame(dialog)
        btn_frame.grid(row=5, column=0, columnspan=2, pady=10)
        
        def update_columns(*args):
            """Обновляет доступные колонки"""
            if first_combo.get() in self.query_results:
                df = self.query_results[first_combo.get()]['data']
                first_key_combo['values'] = list(df.columns)
                if df.columns.size > 0:
                    first_key_combo.current(0)
            
            if second_combo.get() in self.query_results:
                df = self.query_results[second_combo.get()]['data']
                second_key_combo['values'] = list(df.columns)
                if df.columns.size > 0:
                    second_key_combo.current(0)
        
        first_combo.bind('<<ComboboxSelected>>', update_columns)
        second_combo.bind('<<ComboboxSelected>>', update_columns)
        
        def perform_join():
            """Выполняет соединение"""
            try:
                df1 = self.query_results[first_combo.get()]['data']
                df2 = self.query_results[second_combo.get()]['data']
                
                merged = pd.merge(
                    df1, 
                    df2, 
                    left_on=first_key_combo.get(), 
                    right_on=second_key_combo.get(), 
                    how=join_type_combo.get(),
                    suffixes=('_1', '_2')
                )
                
                self._display_results_in_tab(
                    merged, 
                    f"Mapped: {self.result_notebook.tab(first_combo.get(), 'text')} & {self.result_notebook.tab(second_combo.get(), 'text')}"
                )
                dialog.destroy()
                
            except Exception as e:
                messagebox.showerror("Ошибка", f"Ошибка при соединении: {str(e)}")
        
        ttk.Button(btn_frame, text="Join", command=perform_join).pack(side=tk.LEFT, padx=5)
        ttk.Button(btn_frame, text="Cancel", command=dialog.destroy).pack(side=tk.LEFT, padx=5)
        
        # Инициализация
        if len(self.query_results) >= 2:
            first_combo.current(0)
            second_combo.current(1 if len(self.query_results) > 1 else 0)
            update_columns()
        
        def update_columns(*args):
            """Обновляет доступные колонки при выборе таблицы"""
            first_tab = first_combo.get()
            second_tab = second_combo.get()
            
            if first_tab in self.query_results:
                df = self.query_results[first_tab]['data']
                first_key_combo['values'] = list(df.columns)
                if len(df.columns) > 0:
                    first_key_combo.set(df.columns[0])
            
            if second_tab in self.query_results:
                df = self.query_results[second_tab]['data']
                second_key_combo['values'] = list(df.columns)
                if len(df.columns) > 0:
                    second_key_combo.set(df.columns[0])
        
        first_combo.bind('<<ComboboxSelected>>', update_columns)
        second_combo.bind('<<ComboboxSelected>>', update_columns)
        

    def explain(self):
        """Вывод плана выполнения запроса в отдельное окно"""
        query = self.editor.get("1.0", tk.END).strip()
        if not query:
            self.log("Нет запроса для объяснения", error=True)
            return
        
        try:
            explain_query = f"EXPLAIN ANALYZE {query}"
            result, exec_time = self.fdw.execute_query(explain_query)
            
            if self.explain_window is None or not self.explain_window.winfo_exists():
                self.explain_window = tk.Toplevel(self)
                self.explain_window.title("План выполнения")
                self.explain_text = scrolledtext.ScrolledText(self.explain_window, wrap=tk.WORD)
                self.explain_text.pack(fill=tk.BOTH, expand=True)
            
            self.explain_text.delete('1.0', tk.END)
            if not result.empty:
                plan_text = "\n".join(result.iloc[:, 0].astype(str))
                self.explain_text.insert(tk.END, plan_text)
            self.log(f"План выполнен за {exec_time:.2f} сек.")
            
        except Exception as e:
            self.log(f"Ошибка плана: {str(e)}", error=True)



    #def export_csv(self):
    #    if self.current_data is None or self.current_data.empty:
    #        self.log("Нет данных для экспорта", error=True)
    #        return
    #    
    #    try:
    #        file_path = filedialog.asksaveasfilename(
    #            defaultextension=".csv",
    #            filetypes=[("CSV Files", "*.csv")]
    #        )
    #        if not file_path:  # Пользователь отменил сохранение
    #            return
    #            
    #        self.current_data.to_csv(file_path, index=False, encoding='utf-8')
    #        self.log(f"Данные экспортированы в {file_path}")
    #        
    #    except PermissionError:
    #        self.log("Ошибка доступа: файл используется другой программой", error=True)
    #    except Exception as e:
    #        self.log(f"Ошибка экспорта: {str(e)}", error=True)

    def export_to_file(self):
        """Экспорт текущих результатов в файл"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"exportData_{timestamp}.xlsx"
        
        if self.current_data is None or self.current_data.empty:
            messagebox.showinfo("Информация", "Нет данных для экспорта")
            return
        
        try:
            # Создаем копию данных для преобразования
            export_data = self.current_data.copy()
            
            # Преобразуем столбцы с datetime, содержащие timezone
            for col in export_data.columns:
                if pd.api.types.is_datetime64_any_dtype(export_data[col]):
                    # Удаляем информацию о часовом поясе
                    export_data[col] = export_data[col].dt.tz_localize(None)
            
            # Сохраняем файл в текущей директории
            file_path = os.path.abspath(filename)
            export_data.to_excel(file_path, index=False)
            
            self.log(f"Данные экспортированы в Excel: {file_path}")
            messagebox.showinfo("Успех", f"Данные успешно экспортированы в файл:\n{file_path}")
                
        except Exception as e:
            messagebox.showerror("Ошибка", f"Ошибка при экспорте данных:\n{str(e)}")
            self.log(f"Ошибка экспорта: {str(e)}", error=True)

    def show_connections(self):
        """Окно управления подключениями"""
        if self.connections_window and self.connections_window.winfo_exists():
            self.connections_window.destroy()
            
        self.connections_window = tk.Toplevel(self)
        self.connections_window.title("Управление подключениями")
        self.connections_window.geometry("500x300")
        
        frame = ttk.Frame(self.connections_window)
        frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        columns = ("name", "host", "status")
        self.conn_tree = ttk.Treeview(frame, columns=columns, show="headings")
        
        self.conn_tree.heading("name", text="Имя")
        self.conn_tree.heading("host", text="Хост")
        self.conn_tree.heading("status", text="Статус")
        
        for col in columns:
            self.conn_tree.column(col, width=100, anchor="w")
            
        scroll = ttk.Scrollbar(frame, orient="vertical", command=self.conn_tree.yview)
        self.conn_tree.configure(yscroll=scroll.set)
        
        self.conn_tree.pack(side="left", fill=tk.BOTH, expand=True)
        scroll.pack(side="right", fill="y")
        
        btn_frame = ttk.Frame(frame)
        btn_frame.pack(side="bottom", fill=tk.X, pady=5)
        
        ttk.Button(btn_frame, text="Добавить", command=self.add_connection).pack(side=tk.LEFT)
        ttk.Button(btn_frame, text="Редактировать", command=self.edit_connection).pack(side=tk.LEFT)
        ttk.Button(btn_frame, text="Удалить", command=self.delete_connection).pack(side=tk.LEFT)
        ttk.Button(btn_frame, text="Обновить", command=self.update_connections).pack(side=tk.LEFT)
        ttk.Button(btn_frame, text="Закрыть", command=self.close_connection).pack(side=tk.RIGHT)
        
        self.update_connections()

    def show_schema_mapping(self):
        """Окно управления маппингом схем"""
        if self.mapping_window and self.mapping_window.winfo_exists():
            self.mapping_window.destroy()
            
        self.mapping_window = SchemaMappingWindow(self, self.fdw)
        self.mapping_window.grab_set()

    def add_connection(self):
        """Добавление нового подключения"""
        EditConnectionWindow(self, self.fdw, mode='add')
    
    def edit_connection(self):
        """Редактирование выбранного подключения"""
        selected = self.conn_tree.selection()
        if not selected:
            return
        conn_name = self.conn_tree.item(selected[0], 'values')[0]
        EditConnectionWindow(self, self.fdw, mode='edit', connection_name=conn_name)
    
    def delete_connection(self):
        """Удаление подключения"""
        selected = self.conn_tree.selection()
        if not selected:
            return
        conn_name = self.conn_tree.item(selected[0], "values")[0]
        if messagebox.askyesno("Подтверждение", f"Удалить подключение {conn_name}?"):
            # Удаляем связанные маппинги
            schemas_to_delete = []
            for schema, conn in self.fdw.schema_mapping.items():
                if conn == conn_name:
                    schemas_to_delete.append(schema)
            
            for schema in schemas_to_delete:
                del self.fdw.schema_mapping[schema]
            
            # Удаляем подключение (только если это словарь)
            if isinstance(self.fdw.connection_params, dict) and conn_name in self.fdw.connection_params:
                del self.fdw.connection_params[conn_name]
            
            AuthManager.delete_credentials(conn_name)
            self.update_connections()
            self.fdw.save_env_config()

    def update_connections(self):
        """Обновление списка подключений"""
        self.conn_tree.delete(*self.conn_tree.get_children())
        
        # Гарантируем, что работаем со словарем
        if not isinstance(self.fdw.connection_params, dict):
            self.fdw.connection_params = {}
        
        for name, params in self.fdw.connection_params.items():
            # Добавляем проверку существования соединения
            status = "Активно" 
            if name in self.fdw.connections:
                try:
                    # Проверяем статус соединения
                    if self.fdw.connections[name].closed:
                        status = "Неактивно"
                except:
                    status = "Неактивно"
            else:
                status = "Неактивно"
                
            self.conn_tree.insert("", "end", values=(
                name,
                params.get("host", "N/A"),
                status
            ))

    def reconnect_connection(self):
        """Переподключение выбранного соединения"""
        selected = self.conn_tree.selection()
        if not selected:
            return
            
        conn_name = self.conn_tree.item(selected[0], "values")[0]
        try:
            if conn_name in self.fdw.connections:
                self.fdw.connections[conn_name].close()
                del self.fdw.connections[conn_name]
            self.fdw.get_connection(conn_name)
            self.update_connections()
            messagebox.showinfo("Успех", f"Подключение {conn_name} восстановлено")
        except Exception as e:
            messagebox.showerror("Ошибка", str(e))

    def close_connection(self):
        """Закрытие выбранного соединения"""
        selected = self.conn_tree.selection()
        if not selected:
            return
            
        conn_name = self.conn_tree.item(selected[0], "values")[0]
        if conn_name in self.fdw.connections:
            self.fdw.connections[conn_name].close()
            del self.fdw.connections[conn_name]
            self.update_connections()
            messagebox.showinfo("Успех", f"Подключение {conn_name} закрыто")

    def execute(self):
        """Выполняет текущий запрос"""
        query = self.editor.get("1.0", tk.END).strip()
        if not query:
            self.log("Нет запроса для выполнения", error=True)
            return
        
        try:
            result, exec_time = self.fdw.execute_query(query)
            self.current_data = result
            self._display_results_in_tab(result, query)
            self.log(f"Запрос выполнен за {exec_time:.2f} сек. Найдено строк: {len(result)}")
        except Exception as e:
            self.log(f"Ошибка: {str(e)}", error=True)
            self.current_data = None

    def _display_results(self, df):
        try:
        # Полная очистка Treeview перед новым выводом
            self.tree.delete(*self.tree.get_children())
            self.tree["columns"] = []  # Сбрасываем колонки
            
            if df.empty:
                self.log("Нет данных для отображения", error=True)
                return

            # Обновление колонок с правильной инициализацией
            valid_columns = df.columns.tolist()
            
            # 1. Удаление старых колонок
            for col in self.tree["columns"]:
                self.tree.heading(col, text="")
                self.tree.column(col, width=0)
            
            # 2. Создание новых колонок
            self.tree["columns"] = valid_columns
            for col in valid_columns:
                self.tree.heading(
                    col, 
                    text=col,
                    anchor="w"
                )
                self.tree.column(
                    col, 
                    width=120, 
                    stretch=False, 
                    anchor="w"
                )
            
            # 3. Вставка данных
            for _, row in df.iterrows():
                self.tree.insert(
                    "", 
                    tk.END, 
                    values=[row[col] for col in valid_columns]
                )

        except Exception as e:
            self.log(f"Ошибка отображения: {str(e)}", error=True)

    def log(self, message, error=False):
        # Проверяем существование консоли перед использованием
        if not hasattr(self, 'console') or self.console.winfo_exists() == 0:
            return
            
        tag = "ERROR" if error else "INFO"
        try:
            self.console.insert(tk.END, f"[{tag}] {message}\n")
            self.console.see(tk.END)
        except Exception as e:
            print(f"Ошибка логирования: {str(e)}")  # Резервное логирование


    def _setup_tree_context_menu(self, tree=None):
        """Настраивает контекстное меню для Treeview с возможностью копирования конкретного столбца"""
        if tree is None:
            tree = self.current_tree
        
        if tree is None:
            return
        
        # Создаём контекстное меню
        context_menu = tk.Menu(tree, tearoff=0)
        context_menu.add_command(label="Copy", command=lambda: self._copy_selected_data(tree))
        context_menu.add_command(label="Copy with Headers", command=lambda: self._copy_selected_data(tree, with_headers=True))
        
        # Добавляем пункт для копирования значения под курсором (сначала добавляем, но отключаем)
        context_menu.add_command(
            label="Copy Hovered Value", 
            command=lambda: None,
            state=tk.DISABLED
        )
        
        # Подменю для копирования конкретных столбцов
        column_menu = tk.Menu(context_menu, tearoff=0)
        context_menu.add_cascade(label="Copy Column", menu=column_menu)
        
        # Переменные для хранения текущего столбца и значения
        self._current_hover_column = None
        self._current_hover_value = None
        
        def update_column_menu():
            """Обновляет подменю с доступными столбцами"""
            column_menu.delete(0, tk.END)
            columns = tree["columns"]
            
            if not columns:
                column_menu.add_command(label="No columns", state=tk.DISABLED)
                return
                
            for col in columns:
                # Получаем текст заголовка
                heading_text = tree.heading(col)['text']
                column_menu.add_command(
                    label=f"{heading_text}",
                    command=lambda c=col: self._copy_column_data(tree, c)
                )

        def _copy_hovered_value():
            """Копирует значение под курсором"""
            if self._current_hover_value:
                self.clipboard_clear()
                self.clipboard_append(self._current_hover_value)
                self.log(f"Скопировано: {self._current_hover_value[:20]}...")
        
        def on_hover(event):
            """Обработчик наведения на ячейку"""
            region = tree.identify("region", event.x, event.y)
            if region == "cell":
                column = tree.identify_column(event.x)
                row = tree.identify_row(event.y)
                
                # Получаем индекс столбца
                col_index = int(column[1:]) - 1
                columns = tree["columns"]
                
                if col_index < len(columns):
                    # Получаем значение ячейки
                    item = tree.item(row)
                    if col_index < len(item['values']):
                        self._current_hover_column = columns[col_index]
                        self._current_hover_value = str(item['values'][col_index])
                        
                        # Обновляем пункт меню для копирования текущего значения
                        context_menu.entryconfig(2,  # Index of "Copy Hovered Value"
                            label=f"Copy '{self._current_hover_value[:20]}...'",
                            command=_copy_hovered_value,
                            state=tk.NORMAL)
                    else:
                        context_menu.entryconfig(2, state=tk.DISABLED)
                else:
                    context_menu.entryconfig(2, state=tk.DISABLED)
            else:
                context_menu.entryconfig(2, state=tk.DISABLED)
        
        def show_context_menu(event):
            """Показывает контекстное меню с обновленными данными"""
            # Обновляем меню столбцов
            update_column_menu()
            
            # Обновляем информацию о наведении
            on_hover(event)
            
            try:
                context_menu.tk_popup(event.x_root, event.y_root)
            finally:
                context_menu.grab_release()
        
        # Привязка событий
        tree.bind("<Button-3>", show_context_menu)
        tree.bind("<Motion>", on_hover)  # Отслеживаем перемещение мыши
        tree.bind("<Control-c>", lambda e: self._copy_selected_data(tree))

    def _copy_column_data(self, tree, column):
        """Копирует все данные из указанного столбца"""
        # Получаем индекс столбца
        col_index = tree["columns"].index(column)
        
        # Собираем все значения столбца
        values = []
        for item in tree.get_children():
            item_values = tree.item(item, 'values')
            if col_index < len(item_values):
                values.append(str(item_values[col_index]))
        
        # Копируем в буфер обмена
        if values:
            self.clipboard_clear()
            self.clipboard_append("\n".join(values))
            self.log(f"Скопирован столбец '{tree.heading(column)['text']}' ({len(values)} значений)")
            

    def _copy_selected_data(self, tree, with_headers=False):
        """Копирует выделенные данные в буфер обмена"""
        selected_items = tree.selection()
        if not selected_items:
            return
            
        # Получаем все колонки
        columns = tree["columns"]
        
        # Подготавливаем данные для копирования
        data = []
        
        # Добавляем заголовки, если нужно
        if with_headers:
            data.append("\t".join(columns))
        
        # Добавляем данные
        for item in selected_items:
            values = tree.item(item, 'values')
            data.append("\t".join(str(v) for v in values))
        
        # Копируем в буфер обмена
        self.clipboard_clear()
        self.clipboard_append("\n".join(data))
        self.log("Данные скопированы в буфер обмена")

    def _show_context_menu(self, event):
        region = self.tree.identify("region", event.x, event.y)
        if region == "cell":
            self.context_menu.post(event.x_root, event.y_root)

    def _copy_selected_cell(self, event=None):
        selected = self.tree.selection()
        if not selected:
            return
            
        # Получаем все выбранные строки
        rows = self.tree.selection()
        if not rows:
            return
            
        # Для простоты копируем первую выделенную ячейку
        row = rows[0]
        column = self.tree.identify_column(event.x) if event else "#1"
        
        if column:
            item = self.tree.item(row)
            col_index = int(column[1:]) - 1
            if col_index < len(item['values']):
                value = str(item['values'][col_index])
                self.clipboard_clear()
                self.clipboard_append(value)
                self.log(f"Скопировано: {value[:20]}...")

    def clear_results(self):
        """Очищает все результаты"""
        for tab_id in list(self.query_results.keys()):
            self.result_notebook.forget(tab_id)
            del self.query_results[tab_id]
            
        self.console.delete('1.0', tk.END)
        self.current_data = None
        self.log("Все результаты очищены")

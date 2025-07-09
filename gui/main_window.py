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

class FDWGUI(tk.Tk):
    def __init__(self):
        super().__init__()
        load_dotenv() 
        self.title("HF-Point")
        self.geometry("1100x700")
        env_path = os.path.abspath('.env')
        if not os.path.exists(env_path):
            with open(env_path, 'w') as f:
                f.write("CONNECTIONS={}\nMAPPINGS={}\n")
        
        self.fdw = VirtualFDWManager()
        self.current_data = None
        self._check_auth()
        self.main_frame = ttk.Frame(self)
        self.main_frame.pack(fill=tk.BOTH, expand=True)
        self._create_widgets()
        self._create_menu()
        self._create_toolbar()
        self._bind_hotkeys()
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
        """Создает основные элементы интерфейса внутри main_frame"""
        # SQL Editor с подсветкой
        editor_frame = ttk.LabelFrame(self.main_frame, text="SQL Editor")
        editor_frame.pack(fill=tk.X, padx=10, pady=5)
        
        self.editor = SQLText(editor_frame, height=15)
        self.editor.pack(fill=tk.BOTH, expand=True)

        # Results с двойной прокруткой
        result_frame = ttk.LabelFrame(self.main_frame, text="Results")
        result_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)
        
        # Вертикальная прокрутка
        y_scroll = ttk.Scrollbar(result_frame, orient=tk.VERTICAL)
        y_scroll.pack(side=tk.RIGHT, fill=tk.Y)
        
        # Горизонтальная прокрутка
        x_scroll = ttk.Scrollbar(result_frame, orient=tk.HORIZONTAL)
        x_scroll.pack(side=tk.BOTTOM, fill=tk.X)
        
        self.tree = ttk.Treeview(
            result_frame,
            yscrollcommand=y_scroll.set,
            xscrollcommand=x_scroll.set
        )
        self.tree.pack(fill=tk.BOTH, expand=True)
        
        # Настройка команд прокрутки
        y_scroll.config(command=self.tree.yview)
        x_scroll.config(command=self.tree.xview)

        # Console
        console_frame = ttk.LabelFrame(self.main_frame, text="Execution Console")
        console_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)
        self.console = scrolledtext.ScrolledText(console_frame, height=8, wrap=tk.WORD)
        self.console.pack(fill=tk.BOTH, expand=True)

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
        execute_menu.add_command(label="Execute     Ctrl+Enter", command=self.execute)
        execute_menu.add_command(label="Explain         Ctrl+Shift+P", command=self.explain)
        execute_menu.add_command(label="Clear Results", command=self.clear_results)
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

    def clear_results(self):
        """Очистка результатов"""
        self.tree.delete(*self.tree.get_children())
        self.console.delete('1.0', tk.END)
        self.current_data = None
        self.log("Результаты очищены")

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
        self.tree.delete(*self.tree.get_children())
        query = self.editor.get("1.0", tk.END).strip()
        
        try:
            result, exec_time = self.fdw.execute_query(query)
            self.current_data = result  # Сохраняем результат
            self._display_results(result)
            self.log(f"Запрос выполнен за {exec_time:.2f} сек. Найдено строк: {len(result)}")
        except Exception as e:
            self.log(f"Ошибка: {str(e)}", error=True)
            self.current_data = None  # Сбрасываем данные при ошибке

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

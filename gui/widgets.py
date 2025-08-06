import tkinter as tk
from tkinter import scrolledtext
import re


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
            (r'[=<>!+*/%-;]', 'operator')  # Добавлена точка с запятой
        ]

        # Применяем подсветку
        text = self.get('1.0', tk.END)
        for pattern, tag in patterns:
            matches = re.finditer(pattern, text, re.IGNORECASE | re.MULTILINE)
            for match in matches:
                start = f"1.0 + {match.start()}c"
                end = f"1.0 + {match.end()}c"
                self.tag_add(tag, start, end)
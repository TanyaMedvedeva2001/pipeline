import sqlite3
import pandas as pd
import re

class BaseTask:
    """Base Pipeline Task"""

    def run(self):
        raise RuntimeError('Do not run BaseTask!')

    def short_description(self):
        pass

    def __str__(self):
        task_type = self.__class__.__name__
        return f'{task_type}: {self.short_description()}'
    
    def domain_of_url(url):
        domain = re.findall('\/\/(.*?)\/', url)
        return domain[0]


class CopyToFile(BaseTask):
    """Copy table data to CSV file"""

    def __init__(self, table, output_file):
        self.table = table
        self.output_file = output_file

    def short_description(self):
        return f'{self.table} -> {self.output_file}'

    def run(self):
        con = sqlite3.connect('sqlite_pipeline.db')
        db_df = pd.read_sql_query("SELECT * FROM " + self.table, con)
        db_df.to_csv(self.output_file, index=False)
        print(f"Copy table `{self.table}` to file `{self.output_file}`")
        con.close()    # Закрываем соединение


class LoadFile(BaseTask):
    """Load file to table"""
    def __init__(self, table, input_file):
        self.table = table
        self.input_file = input_file

    def short_description(self):
        return f'{self.input_file} -> {self.table}'

    def run(self):
        con = sqlite3.connect('sqlite_pipeline.db')
        df = pd.read_csv(self.input_file)
        df.to_sql(self.table, con, if_exists='append', index=False)
        print(f"Load file `{self.input_file}` to table `{self.table}`")
        con.close()    # Закрываем соединение


class RunSQL(BaseTask):
    """Run custom SQL query"""

    def __init__(self, sql_query, title=None):
        self.title = title
        self.sql_query = sql_query

    def short_description(self):
        return f'{self.title}'

    def run(self):        
        con = sqlite3.connect("sqlite_pipeline.db")
        cur = con.cursor()    # Создаем объект-курсор
        con.create_function("domain_of_url", 1, self.domain_of_url)
        try:    # обработка исключения
            cur.executescript(self.sql_query) # Выполняем SQL-запрос
        except sqlite3.DatabaseError:
            print("Error")
        else:
            print(f"Run SQL ({self.title}):\n{self.sql_query}")
        cur.close()    # Закрываем объект-курсора
        con.close()    # Закрываем соединение



class CTAS(BaseTask):
    """SQL Create Table As Task"""

    def __init__(self, table, sql_query, title=None):
        self.table = table
        self.sql_query = sql_query
        self.title = title or table

    def short_description(self):
        return f'{self.title}'

    def run(self):
        con = sqlite3.connect("sqlite_pipeline.db")
        con.create_function("domain_of_url", 1, self.domain_of_url)
        query = f'''Create table {self.table} as {self.sql_query}'''
        cur = con.cursor()    # Создаем объект-курсор
        try:    # обработка исключения
            cur.executescript(query) # Выполняем SQL-запрос
        except sqlite3.DatabaseError as error:
            print(query)
            print(f"Error:  `{error}`")
        else:
            print(f"Create table `{self.table}` as SELECT:\n{self.sql_query}")
        cur.close()    # Закрываем объект-курсора
        con.close()    # Закрываем соединение

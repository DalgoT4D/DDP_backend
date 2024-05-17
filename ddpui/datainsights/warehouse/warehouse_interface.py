from abc import ABC, abstractmethod


class Warehouse(ABC):

    @abstractmethod
    def execute(self, sql_statement: str):
        pass

    @abstractmethod
    def get_table_columns(self, db_schema: str, db_table: str) -> dict:
        pass

    @abstractmethod
    def get_col_python_type(self, db_schema: str, db_table: str, column_name: str):
        pass

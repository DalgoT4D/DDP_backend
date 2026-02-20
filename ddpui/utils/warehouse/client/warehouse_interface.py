from abc import ABC, abstractmethod
from enum import Enum


class WarehouseType(str, Enum):
    """
    warehouse types available; this will be same as what is stored in OrgWarehouse.wtype
    """

    POSTGRES = "postgres"
    BIGQUERY = "bigquery"


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

    @abstractmethod
    def get_wtype(self):
        pass

    @abstractmethod
    def column_exists(self, db_schema: str, db_table: str, column_name: str) -> bool:
        pass

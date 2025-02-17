from abc import ABC, abstractmethod


class WarehouseInterface(ABC):
    @abstractmethod
    def execute(self, statement: str):
        pass

    @abstractmethod
    def get_tables(self, schema: str):
        pass

    @abstractmethod
    def get_schemas(self):
        pass

    @abstractmethod
    def get_table_data(
        self,
        schema: str,
        table: str,
        limit: int,
        page: int = 1,
        order_by: str = None,
        order: int = 1,  # ASC
    ):
        pass

    @abstractmethod
    def get_table_columns(self, schema: str, table: str):
        pass

    @abstractmethod
    def get_columnspec(self, schema: str, table_id: str):
        pass

    @abstractmethod
    def get_json_columnspec(self, schema: str, table: str, column: str):
        pass

    @abstractmethod
    def ensure_schema(self, schema: str):
        pass

    @abstractmethod
    def ensure_table(self, schema: str, table: str, columns: list):
        pass

    @abstractmethod
    def drop_table(self, schema: str, table: str):
        pass

    @abstractmethod
    def insert_row(self, schema: str, table: str, row: dict):
        pass

    @abstractmethod
    def json_extract_op(self, json_column: str, json_field: str, sql_column: str):
        pass

    @abstractmethod
    def close(self):
        pass

    @abstractmethod
    def generate_profiles_yaml_dbt(self, project_name, default_schema):
        pass

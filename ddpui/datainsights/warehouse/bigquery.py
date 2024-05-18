from sqlalchemy.sql.compiler import IdentifierPreparer
from sqlalchemy.engine import create_engine
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy import inspect
from sqlalchemy_bigquery import BigQueryDialect

from ddpui.datainsights.warehouse.warehouse_interface import Warehouse


class BigqueryClient(Warehouse):

    def __init__(self, creds: dict):
        """
        Establish connection to the postgres database using sqlalchemy engine
        Creds come from the secrets manager
        """
        connection_string = "bigquery://{project_id}".format(**creds)

        self.engine = create_engine(connection_string, credentials_info=creds)
        self.connection = self.engine.connect()
        self.inspect_obj: Inspector = inspect(
            self.engine
        )  # this will be used to fetch metadata of the database

    def execute(self, sql) -> list[dict]:
        """
        Execute the sql query and return the results
        """
        result = self.connection.execute(sql)
        rows = result.fetchall()
        return [dict(row) for row in rows]

    def get_table_columns(self, db_schema: str, db_table: str) -> dict:
        """Fetch columns of a table"""
        return self.inspect_obj.get_columns(table_name=db_table, schema=db_schema)

    def get_col_python_type(self, db_schema: str, db_table: str, column_name: str):
        """Fetch python type of a column"""
        columns = self.get_table_columns(db_schema, db_table)
        for column in columns:
            if column["name"] == column_name:
                return column["type"].python_type
        return None

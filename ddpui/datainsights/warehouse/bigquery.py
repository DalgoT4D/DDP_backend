from sqlalchemy.sql.compiler import IdentifierPreparer
from sqlalchemy.engine import create_engine
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy import inspect
from sqlalchemy_bigquery import BigQueryDialect

from ddpui.datainsights.insights.insight_interface import MAP_TRANSLATE_TYPES
from ddpui.datainsights.warehouse.warehouse_interface import Warehouse
from ddpui.datainsights.warehouse.warehouse_interface import WarehouseType


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
        """Fetch columns of a table; also send the translated col data type"""
        res = []
        for column in self.inspect_obj.get_columns(
            table_name=db_table, schema=db_schema
        ):
            res.append(
                {
                    "name": column["name"],
                    "data_type": str(column["type"]),
                    "translated_type": MAP_TRANSLATE_TYPES[column["type"].python_type],
                    "nullable": column["nullable"],
                }
            )
        return res

    def get_col_python_type(self, db_schema: str, db_table: str, column_name: str):
        """Fetch python type of a column"""
        for column in self.inspect_obj.get_columns:
            if column["name"] == column_name:
                return column["type"].python_type
        return None

    def get_wtype(self):
        return WarehouseType.BIGQUERY

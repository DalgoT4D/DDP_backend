import tempfile
from urllib.parse import quote

from sqlalchemy.engine import create_engine
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy import inspect
from sqlalchemy.types import NullType
from sqlalchemy.sql.expression import TableClause, select, Select, ColumnClause, column, text

from ddpui.datainsights.insights.insight_interface import MAP_TRANSLATE_TYPES
from ddpui.datainsights.warehouse.warehouse_interface import Warehouse
from ddpui.datainsights.warehouse.warehouse_interface import WarehouseType


class PostgresClient(Warehouse):
    """PG implementation of the Warehouse interface"""

    def __init__(self, creds: dict):
        """
        Establish connection to the postgres database using sqlalchemy engine
        Creds come from the secrets manager
        """
        creds["encoded_username"] = quote(creds["username"].strip())
        creds["encoded_password"] = quote(creds["password"].strip())

        connection_args = {
            "host": creds["host"],
            "port": creds["port"],
            "dbname": creds["database"],
            "user": creds["encoded_username"],
            "password": creds["encoded_password"],
        }

        connection_string = "postgresql+psycopg2://"

        if "ssl_mode" in creds:
            creds["sslmode"] = creds["ssl_mode"]

        if "sslrootcert" in creds:
            connection_args["sslrootcert"] = creds["sslrootcert"]

        if "sslmode" in creds and isinstance(creds["sslmode"], str):
            connection_args["sslmode"] = creds["sslmode"]

        if "sslmode" in creds and isinstance(creds["sslmode"], bool):
            connection_args["sslmode"] = "require" if creds["sslmode"] else "disable"

        if (
            "sslmode" in creds
            and isinstance(creds["sslmode"], dict)
            and "ca_certificate" in creds["sslmode"]
        ):
            # connect_params['sslcert'] needs a file path but
            # creds['sslmode']['ca_certificate']
            # is a string (i.e. the actual certificate). so we write
            # it to disk and pass the file path
            with tempfile.NamedTemporaryFile(delete=False) as fp:
                fp.write(creds["sslmode"]["ca_certificate"].encode())
                connection_args["sslrootcert"] = fp.name

        self.engine = create_engine(
            connection_string, connect_args=connection_args, pool_size=5, pool_timeout=30
        )
        self.inspect_obj: Inspector = inspect(
            self.engine
        )  # this will be used to fetch metadata of the database

    def execute(self, sql) -> list[dict]:
        """
        Execute the sql query and return the results
        """
        with self.engine.connect() as connection:
            result = connection.execute(sql)
            rows = result.fetchall()
            return [dict(row) for row in rows]

    def get_table_columns(self, db_schema: str, db_table: str) -> dict:
        """Fetch columns of a table; also send the translated col data type"""
        res = []
        for column_ in self.inspect_obj.get_columns(table_name=db_table, schema=db_schema):
            res.append(
                {
                    "name": column_["name"],
                    "data_type": str(column_["type"]),
                    "translated_type": (
                        None
                        if isinstance(column_["type"], NullType)
                        else MAP_TRANSLATE_TYPES[column_["type"].python_type]
                    ),
                    "nullable": column_["nullable"],
                }
            )
        return res

    def get_col_python_type(self, db_schema: str, db_table: str, column_name: str):
        """Fetch python type of a column"""
        columns = self.get_table_columns(db_schema, db_table)
        for column_ in columns:
            if column_["name"] == column_name:
                return column_["type"].python_type
        return None

    def get_wtype(self):
        return WarehouseType.POSTGRES

    def build_rag_training_sql(
        self, exclude_schemas: list[str], exclude_tables: list[str], exclude_columns: list[str]
    ):
        """This sql query will be sent to llm service for trainig the rag on warehouse"""
        tab: TableClause = text("INFORMATION_SCHEMA.columns")
        stmt: Select = select("*")
        stmt = stmt.select_from(tab)

        if exclude_schemas:
            schema_name_col: ColumnClause = column("table_schema")
            stmt = stmt.where(schema_name_col.not_in(exclude_schemas))

        if exclude_tables:
            table_name_col: ColumnClause = column("table_name")
            stmt = stmt.where(table_name_col.not_in(exclude_tables))

        if exclude_columns:
            column_name_col: ColumnClause = column("column_name")
            stmt = stmt.where(column_name_col.not_in(exclude_columns))

        stmt = stmt.compile(bind=self.engine, compile_kwargs={"literal_binds": True})

        return str(stmt)

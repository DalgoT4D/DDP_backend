import re
import tempfile
from urllib.parse import quote

from sqlalchemy.engine import create_engine
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy import inspect
from sqlalchemy.types import NullType
from sqlalchemy.exc import NoSuchTableError

from ddpui.core.datainsights.insights.insight_interface import MAP_TRANSLATE_TYPES
from ddpui.utils.warehouse.client.warehouse_interface import Warehouse
from ddpui.utils.warehouse.client.warehouse_interface import WarehouseType

POSTGRES_CAST_TYPE_MAP = {
    "numeric": "numeric",
    "integer": "integer",
    "bigint": "bigint",
    "boolean": "boolean",
    "date": "date",
    "timestamp": "timestamp",
    "text": "text",
}


class PostgresClient(Warehouse):
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
            "user": creds["username"],
            "password": creds["password"],
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
        for column in self.inspect_obj.get_columns(table_name=db_table, schema=db_schema):
            res.append(
                {
                    "name": column["name"],
                    "data_type": str(column["type"]),
                    "translated_type": (
                        None
                        if isinstance(column["type"], NullType)
                        else MAP_TRANSLATE_TYPES[column["type"].python_type]
                    ),
                    "nullable": column["nullable"],
                }
            )
        return res

    def get_col_python_type(self, db_schema: str, db_table: str, column_name: str):
        """Fetch python type of a column"""
        columns = self.get_table_columns(db_schema, db_table)
        for column in columns:
            if column["name"] == column_name:
                return column["type"].python_type
        return None

    def get_wtype(self):
        return WarehouseType.POSTGRES

    def column_exists(self, db_schema: str, db_table: str, column_name: str) -> bool:
        """
        Check whether a column exists in the given schema.table.
        Uses SQLAlchemy Inspector to list columns for the table.
        """
        try:
            cols = self.inspect_obj.get_columns(table_name=db_table, schema=db_schema)
        except NoSuchTableError:
            return False
        except Exception:
            return False

        for col in cols:
            if col.get("name") == column_name:
                return True
        return False

    def generate_cast_sql(self, schema: str, table: str, column_casts: dict[str, str]) -> str:
        """Generate ALTER TABLE SQL to cast columns in-place.
        column_casts: {column_name: target_type} — only the columns to cast.
        Raises ValueError for unknown types."""
        preparer = self.engine.dialect.identifier_preparer
        clauses = []
        for col, cast_type in column_casts.items():
            if cast_type not in POSTGRES_CAST_TYPE_MAP:
                raise ValueError(f"Unsupported cast type for Postgres: {cast_type!r}")
            sql_type = POSTGRES_CAST_TYPE_MAP[cast_type]
            # Airbyte Destinations V2: chars not in [a-zA-Z0-9_$] → underscore, case preserved
            normalized_col = re.sub(r"[^a-zA-Z0-9_$]", "_", col)
            col_q = preparer.quote(normalized_col)
            clauses.append(f"ALTER COLUMN {col_q} TYPE {sql_type} USING {col_q}::{sql_type}")
        if not clauses:
            return ""
        schema_q = preparer.quote_schema(schema)
        table_q = preparer.quote(table)
        return f"ALTER TABLE {schema_q}.{table_q}\n  " + ",\n  ".join(clauses)

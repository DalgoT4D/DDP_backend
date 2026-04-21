import tempfile
from urllib.parse import quote
from typing import Any

from sqlalchemy.engine import create_engine
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy import inspect, text
from sqlalchemy.types import NullType
from sqlalchemy.exc import NoSuchTableError

from ddpui.core.datainsights.insights.insight_interface import MAP_TRANSLATE_TYPES
from ddpui.utils.warehouse.client.warehouse_interface import Warehouse
from ddpui.utils.warehouse.client.warehouse_interface import WarehouseType


class PostgresClient(Warehouse):
    def __init__(self, creds: dict):
        """
        Establish connection to the postgres database using sqlalchemy engine
        Creds come from the secrets manager
        """
        self.name = "postgres"

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

    def execute(self, sql, params: dict[str, Any] | None = None) -> list[dict]:
        """
        Execute the sql query and return the results
        """
        statement = text(sql) if isinstance(sql, str) else sql
        with self.engine.begin() as connection:
            result = (
                connection.execute(statement)
                if params is None
                else connection.execute(statement, params)
            )
            if not result.returns_rows:
                return []
            rows = result.fetchall()
            return [dict(row._mapping) for row in rows]

    def execute_many(self, sql: str, params_list: list[dict[str, Any]]) -> int:
        """
        Execute a parameterized SQL statement against multiple rows atomically.
        """
        if not params_list:
            return 0

        statement = text(sql) if isinstance(sql, str) else sql
        with self.engine.begin() as connection:
            result = connection.execute(statement, params_list)
            return int(result.rowcount or 0)

    def execute_transaction(
        self,
        statements: list[tuple[str, dict[str, Any] | None]],
    ) -> list[list[dict]]:
        """
        Execute a list of SQL statements in a single transaction.
        Returns result rows for statements that produce rows.
        """
        results: list[list[dict]] = []

        with self.engine.begin() as connection:
            for sql, params in statements:
                statement = text(sql) if isinstance(sql, str) else sql
                result = (
                    connection.execute(statement)
                    if params is None
                    else connection.execute(statement, params)
                )

                if result.returns_rows:
                    rows = result.fetchall()
                    results.append([dict(row._mapping) for row in rows])
                else:
                    results.append([])

        return results

    def close(self) -> bool:
        """Dispose SQLAlchemy connections held by the engine pool."""
        self.engine.dispose()
        return True

    def get_columnspec(self, schema: str, table_id: str) -> list[str]:
        """Gets the column schema for this table."""
        resultset = self.execute(
            """SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = :schema
                AND table_name = :table_id
            """,
            {"schema": schema, "table_id": table_id},
        )
        return [row["column_name"] for row in resultset]

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

    def get_table_data(
        self,
        schema: str,
        table: str,
        limit: int,
        page: int = 1,
        order_by: str | None = None,
        order: int = 1,
    ) -> list[dict]:
        """Return paginated rows from a schema.table for tests and debugging."""
        offset = max((page - 1) * limit, 0)
        schema_quoted = schema.replace('"', '""')
        table_quoted = table.replace('"', '""')

        query = f'SELECT * FROM "{schema_quoted}"."{table_quoted}"'
        if order_by:
            order_by_quoted = order_by.replace('"', '""')
            sort_order = "ASC" if order == 1 else "DESC"
            query += f' ORDER BY "{order_by_quoted}" {sort_order}'

        query += " OFFSET :offset LIMIT :limit"
        return self.execute(query, {"offset": offset, "limit": int(limit)})

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

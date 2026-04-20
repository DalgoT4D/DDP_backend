import sqlalchemy.types as types
from sqlalchemy.engine import create_engine
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy import inspect, text
from sqlalchemy.types import NullType
from sqlalchemy_bigquery._types import _type_map
from sqlalchemy.exc import NoSuchTableError

from ddpui.core.datainsights.insights.insight_interface import MAP_TRANSLATE_TYPES
from ddpui.utils.warehouse.client.warehouse_interface import Warehouse
from ddpui.utils.warehouse.client.warehouse_interface import WarehouseType

### CAUTION: workaround for missing datatypes; complex queries on such types using sqlalchemy expression might fail
_type_map["JSON"] = types.JSON


class BigqueryClient(Warehouse):
    def __init__(self, creds: dict, location: str | None = None):
        """
        Establish connection to the postgres database using sqlalchemy engine
        Creds come from the secrets manager
        """
        self.name = "bigquery"
        self.location = location

        connection_string = "bigquery://{project_id}".format(**creds)

        engine_kwargs = {
            "credentials_info": creds,
            "pool_size": 5,
            "pool_timeout": 30,
        }
        if location:
            engine_kwargs["location"] = location

        self.engine = create_engine(connection_string, **engine_kwargs)
        self.inspect_obj: Inspector = inspect(
            self.engine
        )  # this will be used to fetch metadata of the database

    def execute(self, sql_statement, params: dict | None = None) -> list[dict]:
        """
        Execute the sql query and return the results
        """
        statement = text(sql_statement) if isinstance(sql_statement, str) else sql_statement
        with self.engine.connect() as connection:
            result = (
                connection.execute(statement)
                if params is None
                else connection.execute(statement, params)
            )
            if not result.returns_rows:
                return []
            rows = result.fetchall()
            return [dict(row._mapping) for row in rows]

    def close(self) -> bool:
        """Dispose SQLAlchemy connections held by the engine pool."""
        self.engine.dispose()
        return True

    def get_columnspec(self, schema: str, table_id: str) -> list[str]:
        """Fetch the list of columns from a BigQuery table."""
        return [col["name"] for col in self.get_table_columns(schema, table_id)]

    def get_table_columns(self, db_schema: str, db_table: str) -> dict:
        """Fetch columns of a table; also send the translated col data type"""
        res = []
        not_supported_cols = []
        for column in self.inspect_obj.get_columns(table_name=db_table, schema=db_schema):
            data_type = None
            translated_type = None
            try:
                data_type = str(column["type"])
                translated_type = (
                    None
                    if isinstance(column["type"], NullType)
                    else MAP_TRANSLATE_TYPES[column["type"].python_type]
                )
            except (
                Exception
            ):  # sqlalchemy doesn't handle bigquery STRUCT type; there is no python_type for STRUCT
                not_supported_cols.append(column["name"])
                continue

            # struct (record in bigquery) fields also come as columns; we don't support them
            # if struct col name is test123; child columns will have names as test123.col1, test123.col3,..
            # we want these col fields (that start with struct col name) to be ignored too
            # struct col itself is ignored in the above "continue" statement
            if any(
                column["name"].startswith(not_supported_col)
                for not_supported_col in not_supported_cols
            ):
                continue

            res.append(
                {
                    "name": column["name"],
                    "data_type": data_type,
                    "translated_type": translated_type,
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
        """Return paginated rows from a dataset.table for tests and debugging."""
        offset = max((page - 1) * limit, 0)
        table_ref = f"`{schema}.{table}`"
        query = f"SELECT * FROM {table_ref}"

        if order_by:
            order_by_quoted = order_by.replace("`", "")
            sort_order = "ASC" if order == 1 else "DESC"
            query += f" ORDER BY `{order_by_quoted}` {sort_order}"

        query += f" LIMIT {int(limit)} OFFSET {int(offset)}"
        return self.execute(query)

    def get_col_python_type(self, db_schema: str, db_table: str, column_name: str):
        """Fetch python type of a column"""
        columns = self.get_table_columns(db_schema, db_table)
        for column in columns:
            if column["name"] == column_name:
                return column["type"].python_type
        return None

    def get_wtype(self):
        return WarehouseType.BIGQUERY

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

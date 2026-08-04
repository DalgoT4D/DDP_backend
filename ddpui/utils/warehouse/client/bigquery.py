import re
import sqlalchemy.types as types
from sqlalchemy.engine import create_engine
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy import inspect
from sqlalchemy.types import NullType
from sqlalchemy_bigquery._types import _type_map
from sqlalchemy.exc import NoSuchTableError

from ddpui.core.datainsights.insights.insight_interface import MAP_TRANSLATE_TYPES
from ddpui.utils.warehouse.client.warehouse_interface import Warehouse
from ddpui.utils.warehouse.client.warehouse_interface import WarehouseType

BIGQUERY_CAST_TYPE_MAP = {
    "numeric": "NUMERIC",
    "integer": "INT64",
    "bigint": "INT64",
    "boolean": "BOOL",
    "date": "DATE",
    "timestamp": "TIMESTAMP",
    "text": "STRING",
}

### CAUTION: workaround for missing datatypes; complex queries on such types using sqlalchemy expression might fail
_type_map["JSON"] = types.JSON


class BigqueryClient(Warehouse):
    def __init__(self, creds: dict):
        """
        Establish connection to the postgres database using sqlalchemy engine
        Creds come from the secrets manager
        """
        connection_string = "bigquery://{project_id}".format(**creds)

        self.engine = create_engine(
            connection_string, credentials_info=creds, pool_size=5, pool_timeout=30
        )
        self.inspect_obj: Inspector = inspect(
            self.engine
        )  # this will be used to fetch metadata of the database

    def execute(self, sql_statement) -> list[dict]:
        """
        Execute the sql query and return the results
        """
        with self.engine.connect() as connection:
            result = connection.execute(sql_statement)
            rows = result.fetchall()
            return [dict(row) for row in rows]

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

    def generate_cast_sql(self, schema: str, table: str, column_casts: dict[str, str]) -> str:
        """Generate CREATE OR REPLACE TABLE SQL using SELECT * REPLACE (...) to cast columns.
        Does not fetch live columns — works even before the first sync.
        column_casts: {column_name: target_type} — only the columns to cast.
        Raises ValueError for unknown types."""
        for cast_type in column_casts.values():
            if cast_type not in BIGQUERY_CAST_TYPE_MAP:
                raise ValueError(f"Unsupported cast type for BigQuery: {cast_type!r}")

        project_id = self.engine.url.host
        preparer = self.engine.dialect.identifier_preparer
        # Quote each identifier component separately so a backtick in the user-supplied
        # schema/table can't escape the identifier boundary.
        full_table = (
            f"{preparer.quote(project_id)}.{preparer.quote(schema)}.{preparer.quote(table)}"
        )

        replace_cols = []
        for col, cast_type in column_casts.items():
            # Airbyte Destinations V2: chars not in [a-zA-Z0-9_$] → underscore, case preserved
            normalized_col = re.sub(r"[^a-zA-Z0-9_$]", "_", col)
            col_q = preparer.quote(normalized_col)
            bq_type = BIGQUERY_CAST_TYPE_MAP[cast_type]
            replace_cols.append(f"CAST({col_q} AS {bq_type}) AS {col_q}")

        if not replace_cols:
            return ""

        replace_str = ",\n  ".join(replace_cols)
        return (
            f"CREATE OR REPLACE TABLE {full_table} AS\n"
            f"SELECT * REPLACE (\n  {replace_str}\n)\n"
            f"FROM {full_table}"
        )

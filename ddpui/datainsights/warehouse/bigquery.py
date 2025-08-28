import sqlalchemy.types as types
from sqlalchemy.engine import create_engine
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy import inspect
from sqlalchemy.types import NullType
from sqlalchemy_bigquery._types import _type_map
from threading import Lock
import hashlib
import json

from ddpui.datainsights.insights.insight_interface import MAP_TRANSLATE_TYPES
from ddpui.datainsights.warehouse.warehouse_interface import Warehouse
from ddpui.datainsights.warehouse.warehouse_interface import WarehouseType

### CAUTION: workaround for missing datatypes; complex queries on such types using sqlalchemy expression might fail
_type_map["JSON"] = types.JSON


class BigqueryClient(Warehouse):
    # Class-level engine cache and lock
    _engines = {}
    _engine_lock = Lock()

    @staticmethod
    def _generate_engine_key(creds: dict, connection_config: dict = None) -> str:
        """Generate a unique hash key from credentials and config"""
        # Create a copy to avoid modifying original
        key_data = creds.copy()

        # Add connection config to the key if provided
        if connection_config:
            key_data["_connection_config"] = connection_config

        # Sort keys for consistent ordering
        key_string = json.dumps(key_data, sort_keys=True, default=str)

        # Generate hash
        return hashlib.md5(key_string.encode()).hexdigest()

    def __init__(self, creds: dict, connection_config: dict = None):
        """
        Establish connection to the BigQuery database using sqlalchemy engine
        Creds come from the secrets manager
        connection_config: Optional dict with connection pooling parameters
        """
        connection_string = "bigquery://{project_id}".format(**creds)

        # Default connection pool configuration
        default_pool_config = {
            "pool_size": 5,
            "max_overflow": 2,
            "pool_timeout": 30,
            "pool_recycle": 3600,
            "pool_pre_ping": True,
        }

        # Use provided config or defaults
        pool_config = connection_config if connection_config else default_pool_config

        # Create engine key from all credentials and config
        engine_key = self._generate_engine_key(creds, connection_config)

        # Use singleton pattern for engines
        with self._engine_lock:
            if engine_key not in self._engines:
                self._engines[engine_key] = create_engine(
                    connection_string, credentials_info=creds, **pool_config
                )
            self.engine = self._engines[engine_key]

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

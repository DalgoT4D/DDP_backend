import tempfile
from urllib.parse import quote
from threading import Lock
import hashlib
import json

from sqlalchemy.engine import create_engine
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy import inspect
from sqlalchemy.types import NullType

from ddpui.datainsights.insights.insight_interface import MAP_TRANSLATE_TYPES
from ddpui.datainsights.warehouse.warehouse_interface import Warehouse
from ddpui.datainsights.warehouse.warehouse_interface import WarehouseType


class PostgresClient(Warehouse):
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
        Establish connection to the postgres database using sqlalchemy engine
        Creds come from the secrets manager
        connection_config: Optional dict with connection pooling parameters
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
                    connection_string, connect_args=connection_args, **pool_config
                )
            self.engine = self._engines[engine_key]

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

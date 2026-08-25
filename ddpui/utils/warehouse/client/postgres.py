import tempfile

from sqlalchemy.engine import create_engine
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy import inspect
from sqlalchemy.types import NullType
from sqlalchemy.exc import NoSuchTableError

from ddpui.core.datainsights.insights.insight_interface import MAP_TRANSLATE_TYPES
from ddpui.utils.warehouse.client import engine_registry
from ddpui.utils.warehouse.client.warehouse_interface import Warehouse
from ddpui.utils.warehouse.client.warehouse_interface import WarehouseType


def build_connection_args(creds: dict) -> dict:
    """
    Translate warehouse credentials into psycopg2 connect_args.

    Works on a copy: this normalises the credentials (sslmode aliasing) and the
    caller's dict should not come back mutated. Only ever called on an engine
    cache miss, which also means the CA certificate below is written to disk once
    per warehouse instead of once per request.
    """
    creds = dict(creds)

    connection_args = {
        "host": creds["host"],
        "port": creds["port"],
        "dbname": creds["database"],
        "user": creds["username"],
        "password": creds["password"],
    }

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

    return connection_args


class PostgresClient(Warehouse):
    def __init__(self, creds: dict):
        """
        Establish connection to the postgres database using sqlalchemy engine
        Creds come from the secrets manager

        The engine (and therefore its connection pool) is shared per set of credentials
        via the engine registry. Building one per client -- and so per request -- left a
        pool of open connections behind on every call.
        """
        cache_key = engine_registry.fingerprint(WarehouseType.POSTGRES, creds)

        self.engine = engine_registry.get_or_create_engine(
            cache_key,
            lambda: create_engine(
                "postgresql+psycopg2://",
                connect_args=build_connection_args(creds),
                **engine_registry.pool_kwargs(),
            ),
            wtype=WarehouseType.POSTGRES,
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

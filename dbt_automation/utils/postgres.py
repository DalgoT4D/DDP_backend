"""helpers for postgres"""

import os
import tempfile
from logging import basicConfig, getLogger, INFO
import psycopg2
from sshtunnel import SSHTunnelForwarder
from dbt_automation.utils.columnutils import quote_columnname
from dbt_automation.utils.interfaces.warehouse_interface import WarehouseInterface


basicConfig(level=INFO)
logger = getLogger()


class PostgresClient(WarehouseInterface):
    """a postgres client that can be used as a context manager"""

    @staticmethod
    def get_connection(conn_info):
        """
        returns a psycopg connection
        parameters are
            host: str
            port: str
            user: str
            password: str
            database: str
            sslmode: require | disable | prefer | allow | verify-ca | verify-full
            sslrootcert: /path/to/cert
            ...
        """
        connect_params = {}
        for key in [
            "host",
            "port",
            "user",
            "password",
            "database",
        ]:
            if key in conn_info:
                connect_params[key] = conn_info[key]

        # ssl_mode is an alias for sslmode
        if "ssl_mode" in conn_info:
            conn_info["sslmode"] = conn_info["ssl_mode"]

        if "sslmode" in conn_info:
            # sslmode can be a string or a boolean or a dict
            if isinstance(conn_info["sslmode"], str):
                # "require", "disable", "verify-ca", "verify-full"
                connect_params["sslmode"] = conn_info["sslmode"]
            elif isinstance(conn_info["sslmode"], bool):
                # true = require, false = disable
                connect_params["sslmode"] = (
                    "require" if conn_info["sslmode"] else "disable"
                )
            elif (
                isinstance(conn_info["sslmode"], dict)
                and "mode" in conn_info["sslmode"]
            ):
                # mode is "require", "disable", "verify-ca", "verify-full" etc
                connect_params["sslmode"] = conn_info["sslmode"]["mode"]
                if "ca_certificate" in conn_info["sslmode"]:
                    # connect_params['sslcert'] needs a file path but
                    # conn_info['sslmode']['ca_certificate']
                    # is a string (i.e. the actual certificate). so we write
                    # it to disk and pass the file path
                    with tempfile.NamedTemporaryFile(delete=False) as fp:
                        fp.write(conn_info["sslmode"]["ca_certificate"].encode())
                        connect_params["sslrootcert"] = fp.name

        if "sslrootcert" in conn_info:
            connect_params["sslrootcert"] = conn_info["sslrootcert"]

        connection = psycopg2.connect(**connect_params)
        return connection

    def __init__(self, conn_info: dict):
        self.name = "postgres"
        self.cursor = None
        self.tunnel = None
        self.connection = None

        if conn_info is None:  # take creds from env
            conn_info = {
                "host": os.getenv("DBHOST"),
                "port": os.getenv("DBPORT"),
                "user": os.getenv("DBUSER"),
                "password": os.getenv("DBPASSWORD"),
                "database": os.getenv("DBNAME"),
            }

        if "ssh_host" in conn_info:
            self.tunnel = SSHTunnelForwarder(
                (conn_info["ssh_host"], conn_info["ssh_port"]),
                remote_bind_address=(conn_info["host"], conn_info["port"]),
                # ...and credentials
                ssh_pkey=conn_info.get("ssh_pkey"),
                ssh_username=conn_info.get("ssh_username"),
                ssh_password=conn_info.get("ssh_password"),
                ssh_private_key_password=conn_info.get("ssh_private_key_password"),
            )
            self.tunnel.start()
            conn_info["host"] = "localhost"
            conn_info["port"] = self.tunnel.local_bind_port
            self.connection = PostgresClient.get_connection(conn_info)

        else:
            self.connection = PostgresClient.get_connection(conn_info)
        self.conn_info = conn_info

    def __del__(self):
        """destructor"""
        if self.cursor is not None:
            self.cursor.close()
            self.cursor = None
        if self.connection is not None:
            self.connection.close()
            self.connection = None
        if self.tunnel is not None:
            self.tunnel.stop()
            self.tunnel = None

    def runcmd(self, statement: str):
        """runs a command"""
        if self.cursor is None:
            self.cursor = self.connection.cursor()
        self.cursor.execute(statement)
        self.connection.commit()

    def execute(self, statement: str) -> list:
        """run a query and return the results"""
        if self.cursor is None:
            self.cursor = self.connection.cursor()
        self.cursor.execute(statement)
        return self.cursor.fetchall()

    def get_tables(self, schema: str) -> list:
        """returns the list of table names in the given schema"""
        resultset = self.execute(
            f"""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = '{schema}'
        """
        )
        return [x[0] for x in resultset]

    def get_schemas(self) -> list:
        """returns the list of schema names in the given database connection"""
        resultset = self.execute(
            """
            SELECT nspname
            FROM pg_namespace
            WHERE nspname NOT LIKE 'pg_%' AND nspname != 'information_schema' AND nspname != 'airbyte_internal';
            """
        )
        return [x[0] for x in resultset]

    def get_table_data(
        self,
        schema: str,
        table: str,
        limit: int,
        page: int = 1,
        order_by: str = None,
        order: int = 1,  # ASC
    ) -> list:
        """
        returns limited rows from the specified table in the given schema
        """
        offset = max((page - 1) * limit, 0)
        # total_rows = self.execute(f"SELECT COUNT(*) FROM {schema}.{table}")[0][0]

        # select
        query = f"""
        SELECT * 
        FROM "{schema}"."{table}"
        """

        # order
        if order_by:
            query += f"""
            ORDER BY {quote_columnname(order_by, "postgres")} {"ASC" if order == 1 else "DESC"}
            """

        # offset, limit
        query += f"""
        OFFSET {offset} LIMIT {limit};
        """

        resultset = self.execute(query)  # returns an array of tuples of values
        col_names = [desc[0] for desc in self.cursor.description]
        rows = [dict(zip(col_names, row)) for row in resultset]

        return rows

    def get_table_columns(self, schema: str, table: str) -> list:
        """returns the column names of the specified table in the given schema"""
        resultset = self.execute(
            f"""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = '{schema}' AND table_name = '{table}';
            """
        )
        return [{"name": x[0], "data_type": x[1]} for x in resultset]

    def get_columnspec(self, schema: str, table_id: str):
        """get the column schema for this table"""
        return [
            x[0]
            for x in self.execute(
                f"""SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = '{schema}'
                AND table_name = '{table_id}'
            """
            )
        ]

    def get_json_columnspec(self, schema: str, table: str, column: str):
        """get the column schema from the specified json field for this table"""
        return [
            x[0]
            for x in self.execute(
                f"""SELECT DISTINCT 
                    jsonb_object_keys({quote_columnname(column, 'postgres')}::jsonb)
                FROM "{schema}"."{table}"
            """
            )
        ]

    def ensure_schema(self, schema: str):
        """creates the schema if it doesn't exist"""
        self.runcmd(f"CREATE SCHEMA IF NOT EXISTS {schema};")

    def ensure_table(self, schema: str, table: str, columns: list):
        """creates the table if it doesn't exist. all columns are TEXT"""
        column_defs = [f"{column} TEXT" for column in columns]
        self.runcmd(
            f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table} (
                {','.join(column_defs)}
            );
            """
        )

    def drop_table(self, schema: str, table: str):
        """drops the table if it exists"""
        self.runcmd(f"DROP TABLE IF EXISTS {schema}.{table};")

    def insert_row(self, schema: str, table: str, row: dict):
        """inserts a row into the table"""
        columns = ",".join(row.keys())
        values = ",".join([f"'{x}'" for x in row.values()])
        self.runcmd(
            f"""
            INSERT INTO {schema}.{table} ({columns})
            VALUES ({values});
            """
        )

    def json_extract_op(self, json_column: str, json_field: str, sql_column: str):
        """outputs a sql query snippet for extracting a json field"""
        return f"{quote_columnname(json_column, 'postgres')}::json->>'{json_field}' as \"{sql_column}\""

    def close(self):
        try:
            if self.cursor is not None:
                self.cursor.close()
                self.cursor = None
            if self.tunnel is not None:
                self.tunnel.stop()
                self.tunnel = None
            if self.connection is not None:
                self.connection.close()
                self.connection = None
        except Exception:
            logger.error("something went wrong while closing the postgres connection")

        return True

    def generate_profiles_yaml_dbt(self, project_name, default_schema):
        """
        Generates the profiles.yml dictionary object for dbt
        <project_name>:
            outputs:
                prod:
                    dbname:
                    host:
                    password:
                    port: 5432
                    user: airbyte_user
                    schema:
                    threads: 4
                    type: postgres
            target: prod
        """
        if project_name is None or default_schema is None:
            raise ValueError("project_name and default_schema are required")

        target = "prod"

        profiles_yml = {
            f"{project_name}": {
                "outputs": {
                    f"{target}": {
                        "dbname": self.conn_info["database"],
                        "host": self.conn_info["host"],
                        "password": self.conn_info["password"],
                        "port": int(self.conn_info["port"]),
                        "user": self.conn_info["user"],
                        "schema": default_schema,
                        "threads": 4,
                        "type": "postgres",
                    }
                },
                "target": target,
            },
        }

        return profiles_yml

    def get_total_rows(self, schema: str, table: str) -> int:
        """Fetches the total number of rows for a specified table."""
        try:
            resultset = self.execute(
                f"""
                SELECT COUNT(*) 
                FROM "{schema}"."{table}";
                """
            )
            total_rows = resultset[0][0] if resultset else 0
            return total_rows
        except Exception as e:
            logger.error(f"Failed to fetch total rows for {schema}.{table}: {e}")
            raise

    def get_column_data_types(self) -> list:
        """Returns a list of distinct column data types from PostgreSQL."""
        postgres_data_types = [
            "boolean",
            "char",
            "character varying",
            "date",
            "double precision",
            "float",
            "integer",
            "jsonb",
            "numeric",
            "text",
            "time",
            "timestamp",
            "timestamp with time zone",
            "uuid",
            "varchar",
        ]
        return postgres_data_types

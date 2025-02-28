from sqlalchemy import inspect
import sqlalchemy.types as types
from sqlalchemy.engine import create_engine
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.types import NullType
from sqlalchemy.sql.expression import TableClause, select, Select, ColumnClause, column, text
from sqlalchemy_bigquery._types import _type_map

from ddpui.datainsights.insights.insight_interface import MAP_TRANSLATE_TYPES
from ddpui.datainsights.warehouse.warehouse_interface import Warehouse
from ddpui.datainsights.warehouse.warehouse_interface import WarehouseType

### CAUTION: workaround for missing datatypes; complex queries on such types using sqlalchemy expression might fail
_type_map["JSON"] = types.JSON


class BigqueryClient(Warehouse):
    """BQ implementation for the Warehouse interface"""

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
        for column_ in self.inspect_obj.get_columns(table_name=db_table, schema=db_schema):
            data_type = None
            translated_type = None
            try:
                data_type = str(column_["type"])
                translated_type = (
                    None
                    if isinstance(column_["type"], NullType)
                    else MAP_TRANSLATE_TYPES[column_["type"].python_type]
                )
            except (
                Exception
            ):  # sqlalchemy doesn't handle bigquery STRUCT type; there is no python_type for STRUCT
                not_supported_cols.append(column_["name"])
                continue

            # struct (record in bigquery) fields also come as columns; we don't support them
            # if struct col name is test123; child columns will have names as test123.col1, test123.col3,..
            # we want these col fields (that start with struct col name) to be ignored too
            # struct col itself is ignored in the above "continue" statement
            if any(
                column_["name"].startswith(not_supported_col)
                for not_supported_col in not_supported_cols
            ):
                continue

            res.append(
                {
                    "name": column_["name"],
                    "data_type": data_type,
                    "translated_type": translated_type,
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
        return WarehouseType.BIGQUERY

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

from abc import ABC, abstractmethod
from dataclasses import dataclass
import datetime
from enum import Enum
from sqlalchemy.sql.selectable import Select

from ddpui.datainsights.query_builder import AggQueryBuilder
from ddpui.datainsights.warehouse.warehouse_interface import WarehouseType


class TranslateColDataType(str, Enum):
    STRING = "String"
    BOOL = "Boolean"
    DATETIME = "Datetime"
    NUMERIC = "Numeric"
    BASE = "base"


MAP_TRANSLATE_TYPES = {
    int: TranslateColDataType.NUMERIC,
    float: TranslateColDataType.NUMERIC,
    datetime.datetime: TranslateColDataType.DATETIME,
    datetime.date: TranslateColDataType.DATETIME,
    bool: TranslateColDataType.BOOL,
    str: TranslateColDataType.STRING,
}


@dataclass
class ColumnConfig:
    name: str
    data_type: str  # sql datatype
    translated_type: TranslateColDataType  # translated type


class ColInsight(ABC):
    """
    Interface
    All insights should implement this interface/abstract class
    Basically returns a sql query to be executed and once executed it parses the result for that query

    Note: 'columns' is a list of strings; we run the same operations for all columns in this one big query to reduce visits to the db
    These 'columns' can have different data types
    """

    def __init__(
        self,
        columns: list[ColumnConfig],
        db_table: str,
        db_schema: str,
        filter: dict = None,
        wtype: str = None,
    ):
        self.columns: list[ColumnConfig] = columns
        self.builder: AggQueryBuilder = AggQueryBuilder()
        self.db_table: str = db_table
        self.db_schema: str = db_schema
        self.filter = filter
        self.wtype = wtype

    @abstractmethod
    def query_id(self) -> str:
        pass

    @abstractmethod
    def generate_sql(self) -> Select:
        pass

    @abstractmethod
    def parse_results(self, result: list[dict]):
        pass

    def chart_type(self) -> str:
        return None


class DataTypeColInsights(ABC):
    """
    Class that maintains a list of ColInsight for a column of particular type
    """

    def __init__(
        self,
        columns: list[dict],
        db_table: str,
        db_schema: str,
        filter: dict = None,
        wtype: str = None,
    ):
        self.columns: list[ColumnConfig] = [
            ColumnConfig(
                name=c["name"],
                data_type=c["data_type"],
                translated_type=c["translated_type"],
            )
            for c in columns
        ]
        self.db_table: str = db_table
        self.db_schema: str = db_schema
        self.filter = filter
        self.wtype = wtype if wtype else WarehouseType.POSTGRES  # default postgres
        self.insights: list[ColInsight] = []

    @abstractmethod
    def generate_sqls(self) -> list[Select]:
        pass

    @abstractmethod
    def merge_output(self, results: list[dict]):
        pass
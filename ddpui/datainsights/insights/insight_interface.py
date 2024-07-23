from abc import ABC, abstractmethod
from dataclasses import dataclass
import datetime
from enum import Enum
from sqlalchemy.sql.selectable import Select
from decimal import Decimal

from ddpui.datainsights.query_builder import AggQueryBuilder
from ddpui.datainsights.warehouse.warehouse_interface import WarehouseType


class TranslateColDataType(str, Enum):
    STRING = "String"
    BOOL = "Boolean"
    DATETIME = "Datetime"
    NUMERIC = "Numeric"
    BASE = "base"
    JSON = "Json"


MAP_TRANSLATE_TYPES = {
    int: TranslateColDataType.NUMERIC,
    float: TranslateColDataType.NUMERIC,
    datetime.datetime: TranslateColDataType.DATETIME,
    datetime.date: TranslateColDataType.DATETIME,
    bool: TranslateColDataType.BOOL,
    str: TranslateColDataType.STRING,
    dict: TranslateColDataType.JSON,
    Decimal: TranslateColDataType.NUMERIC,
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
        filter_: dict = None,
        wtype: str = None,
    ):
        self.columns: list[ColumnConfig] = columns
        self.builder: AggQueryBuilder = AggQueryBuilder()
        self.db_table: str = db_table
        self.db_schema: str = db_schema
        self.filter = filter_
        self.wtype = wtype

    @abstractmethod
    def query_id(self) -> str:
        """
        This will be dictate whether a query is unique or not
        Returns a hash string
        """
        pass

    @abstractmethod
    def generate_sql(self) -> Select:
        """
        Returns a sqlalchemy query statement ready to be executed by an engine
        """
        pass

    @abstractmethod
    def parse_results(self, result: list[dict]):
        """
        Parses the result from the above executed sql query
        """
        pass

    def chart_type(self) -> str:
        """
        This will return a Non null value if the insight is a used for a chart
        """
        return None

    @abstractmethod
    def validate_query_results(self, parsed_results) -> bool:
        """
        Validate the parsed results of the query
        This function assumes the parsed_results sent is for a single column
        """
        pass

    @abstractmethod
    def query_data_type(self) -> TranslateColDataType:
        """
        Returns the data type of the column
        """
        pass


class DataTypeColInsights:
    """
    Class that maintains a list of ColInsight for a column of particular type
    """

    def __init__(
        self,
        columns: list[dict],
        db_table: str,
        db_schema: str,
        filter_: dict = None,
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
        self.filter = filter_
        self.wtype = wtype if wtype else WarehouseType.POSTGRES  # default postgres
        self.insights: list[ColInsight] = []

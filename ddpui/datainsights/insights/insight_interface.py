from abc import ABC, abstractmethod

from ddpui.datainsights.query_builder import AggQueryBuilder


class ColInsight(ABC):
    """
    Functional Interface
    All column insights should implement this interface/abstract class
    """

    def __init__(self, column_name: str, db_table: str, db_schema: str):
        self.column_name: str = column_name
        self.builder: AggQueryBuilder = AggQueryBuilder()
        self.db_table: str = db_table
        self.db_schema: str = db_schema

    @abstractmethod
    def generate_sql(self):
        pass

    @abstractmethod
    def parse_results(self, result: list[dict]):
        pass

    @abstractmethod
    def get_col_type(self):
        pass

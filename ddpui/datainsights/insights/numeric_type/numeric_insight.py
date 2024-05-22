from ddpui.datainsights.insights.insight_interface import (
    DataTypeColInsights,
    ColInsight,
)
from ddpui.datainsights.insights.numeric_type.queries import DataStats


class NumericColInsights(DataTypeColInsights):
    """
    Class that maintains a list of ColInsight queries for a numeric type col
    """

    def __init__(
        self,
        columns: list[dict],
        db_table: str,
        db_schema: str,
        filter: dict = None,
        wtype: str = None,
    ):
        super().__init__(columns, db_table, db_schema, filter, wtype)
        self.insights: list[ColInsight] = [
            DataStats(
                self.columns, self.db_table, self.db_schema, self.filter, self.wtype
            ),
        ]

    def generate_sqls(self) -> list:
        """Returns list of sql queries to be executed"""
        return [query.generate_sql() for query in self.insights]

    def merge_output(self, results: list[dict]):
        output = [
            insight.parse_results(result)
            for insight, result in zip(self.insights, results)
        ]
        # resp = {
        #     "columnName": self.column_name,
        #     "columnType": self.get_col_type(),
        #     "insights": {},
        # }
        # resp = {}

        # if len(output) > 0:
        #     resp["insights"] = output[0]

        return output
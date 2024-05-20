from dataclasses import dataclass

from ddpui.datainsights.insights.insight_interface import (
    DataTypeColInsights,
    ColInsight,
)
from ddpui.datainsights.insights.datetime_type.queries import (
    DataStats,
    DistributionChart,
)


class DatetimeColInsights(DataTypeColInsights):
    """
    Class that maintains a list of ColInsight queries for a numeric type col
    """

    def __init__(
        self,
        column_name: str,
        db_table: str,
        db_schema: str,
        filter: dict = None,
        wtype: str = None,
    ):
        super().__init__(column_name, db_table, db_schema, filter, wtype)
        self.insights: list[ColInsight] = [
            DataStats(column_name, db_table, db_schema, filter, wtype),
            DistributionChart(column_name, db_table, db_schema, filter, wtype),
        ]

    def generate_sqls(self) -> list:
        """Returns list of sql queries to be executed"""
        return [query.generate_sql() for query in self.insights]

    def merge_output(self, results: list[dict]):
        output = [
            insight.parse_results(result)
            for insight, result in zip(self.insights, results)
        ]
        resp = {
            "columnName": self.column_name,
            "columnType": self.get_col_type(),
            "insights": {},
        }

        if len(output) > 0:
            resp["insights"] = output[0]

        if len(output) > 1:
            resp["insights"]["charts"] = [output[1]]

        return resp

    def get_col_type(self) -> str:
        return "Datetime"

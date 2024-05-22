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

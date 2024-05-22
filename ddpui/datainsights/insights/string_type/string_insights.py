from ddpui.datainsights.insights.insight_interface import (
    DataTypeColInsights,
    ColInsight,
    TranslateColDataType,
)
from ddpui.datainsights.insights.string_type.queries import DistributionChart


class StringColInsights(DataTypeColInsights):
    """
    Class that maintains a list of ColInsight queries for a numeric type col
    """

    def __init__(
        self,
        columns: list[str],
        db_table: str,
        db_schema: str,
        filter: dict = None,
        wtype: str = None,
    ):
        super().__init__(columns, db_table, db_schema, filter, wtype)
        self.insights: list[ColInsight] = [
            DistributionChart(
                self.columns, self.db_table, self.db_schema, self.filter, self.wtype
            ),
        ]

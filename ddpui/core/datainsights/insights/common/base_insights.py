from ddpui.core.datainsights.insights.insight_interface import (
    DataTypeColInsights,
    ColInsight,
)
from ddpui.core.datainsights.insights.common.queries import BaseDataStats


class BaseInsights(DataTypeColInsights):
    """
    Class that maintains a list of shared queries across all datatypes
    """

    def __init__(
        self,
        columns: list[dict],
        db_table: str,
        db_schema: str,
        filter_: dict = None,
        wtype: str = None,
    ):
        super().__init__(columns, db_table, db_schema, filter_, wtype)
        self.insights: list[ColInsight] = [
            BaseDataStats(self.columns, self.db_table, self.db_schema, self.filter, self.wtype),
        ]

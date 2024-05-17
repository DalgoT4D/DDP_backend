import datetime

from ddpui.datainsights.insights.numeric_type.numeric_insight import NumericColInsights
from ddpui.datainsights.insights.datetime_type.datetime_insight import (
    DatetimeColInsights,
)
from ddpui.datainsights.insights.boolean_type.boolean_insights import BooleanColInsights
from ddpui.datainsights.insights.insight_interface import (
    DataTypeColInsights,
)


class InsightsFactory:
    """Return the correct object based on type of column"""

    @classmethod
    def initiate_insight(
        cls, column_name: str, db_table: str, db_schema: str, col_type, filter: dict
    ) -> DataTypeColInsights:
        if col_type == int or col_type == float:
            return NumericColInsights(column_name, db_table, db_schema, filter)
        elif col_type == datetime.datetime or col_type == datetime.date:
            return DatetimeColInsights(
                column_name,
                db_table,
                db_schema,
                filter if "chart_type" in filter else None,
            )
        elif col_type == bool:
            return BooleanColInsights(column_name, db_table, db_schema, filter)
        else:
            raise ValueError("Column type not supported for insights generation")

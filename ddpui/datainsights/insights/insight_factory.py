from ddpui.datainsights.insights.numeric_type.numeric_insight import NumericColInsights
from ddpui.datainsights.insights.datetime_type.datetime_insight import (
    DatetimeColInsights,
)
from ddpui.datainsights.insights.boolean_type.boolean_insights import BooleanColInsights
from ddpui.datainsights.insights.string_type.string_insights import StringColInsights
from ddpui.datainsights.insights.insight_interface import (
    DataTypeColInsights,
    TranslateColDataType,
)


class InsightsFactory:
    """Return the correct object based on type of column"""

    @classmethod
    def initiate_insight(
        cls,
        columns: list[dict],
        db_table: str,
        db_schema: str,
        col_type: TranslateColDataType,
        filter_: dict,
        wtype: str,
    ) -> DataTypeColInsights:
        if col_type == TranslateColDataType.NUMERIC:
            return NumericColInsights(columns, db_table, db_schema, filter_, wtype)
        elif col_type == TranslateColDataType.DATETIME:
            return DatetimeColInsights(
                columns,
                db_table,
                db_schema,
                filter_,
                wtype,
            )
        elif col_type == TranslateColDataType.BOOL:
            return BooleanColInsights(columns, db_table, db_schema, filter_, wtype)
        elif col_type == TranslateColDataType.STRING:
            return StringColInsights(columns, db_table, db_schema, filter_, wtype)
        else:
            # base for unknown data types
            # json will be handled here
            return DataTypeColInsights([], db_table, db_schema, None, wtype)

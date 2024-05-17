from ddpui.datainsights.insights.numeric_insight import NumericColInsight
from ddpui.datainsights.insights.insight_interface import ColInsight


class ColInsightFactory:

    @classmethod
    def initiate_insight(
        cls, column_name: str, db_table: str, db_schema: str, col_type
    ) -> ColInsight:
        if col_type == int or col_type == float:
            return NumericColInsight(column_name, db_table, db_schema)
        else:
            raise ValueError("Column type not supported for insights generation")

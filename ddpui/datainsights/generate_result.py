from sqlalchemy.sql.selectable import Select
from ddpui.datainsights.insights.insight_interface import ColInsight
from ddpui.datainsights.warehouse.warehouse_interface import Warehouse
from ddpui.datainsights.insights.insight_interface import DataTypeColInsights


class GenerateResult:
    """
    Class that generates result by executing the insight(s) queries
    """

    @classmethod
    def generate_col_insights(
        cls, insight: DataTypeColInsights, wclient: Warehouse
    ) -> dict:
        """
        Generates insights for the given list of insights
        """
        sql_queries: list[Select] = insight.generate_sqls()
        output = []
        for query in sql_queries:
            query = query.compile(
                bind=wclient.engine, compile_kwargs={"literal_binds": True}
            )
            results = wclient.execute(query)
            output.append(results)

        return insight.merge_output(output)

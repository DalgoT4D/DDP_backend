from ddpui.datainsights.insights.insight_interface import ColInsight
from ddpui.datainsights.warehouse.warehouse_interface import Warehouse


class GenerateResult:
    """
    Class that generates result by executing the insight(s) queries
    """

    @classmethod
    def generate_insight(cls, insight: ColInsight, wclient: Warehouse) -> dict:
        """
        Generates insights for the given list of insights
        """
        sql = insight.generate_sql()
        results = wclient.execute(sql)
        parsed_results = insight.parse_results(results)
        return parsed_results

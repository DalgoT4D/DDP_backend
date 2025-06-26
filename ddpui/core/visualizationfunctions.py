from ninja.errors import HttpError
from sqlalchemy.sql.expression import column

from ddpui.models.org import OrgWarehouse
from ddpui.utils import secretsmanager
from ddpui.dbt_automation.utils.warehouseclient import get_client
from ddpui.datainsights.query_builder import AggQueryBuilder
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui")


def generate_chart_data_service(
    org_warehouse: OrgWarehouse,
    schema_name: str,
    table_name: str,
    xaxis_col: str,
    yaxis_col: str,
    offset: int = 0,
    limit: int = 10,
):
    """
    Service function to generate chart data by running a custom query on the warehouse.
    This can be used by both direct chart generation and saved chart APIs.
    """
    try:
        credentials = secretsmanager.retrieve_warehouse_credentials(org_warehouse)
        wclient = get_client(org_warehouse.wtype, credentials, org_warehouse.bq_location)

        # Use AggQueryBuilder to build the query
        builder = AggQueryBuilder()
        builder.add_column(column(xaxis_col))
        builder.add_column(column(yaxis_col))
        builder.fetch_from(table_name, schema_name)
        builder.offset_rows(offset)
        builder.limit_rows(limit)
        stmt = builder.build()
        sql = str(stmt)

        result = wclient.execute(sql)
        data = list(result)

        return data
    except Exception as e:
        logger.error(f"Failed to generate chart data: {e}")
        raise HttpError(500, f"Failed to fetch chart data: {str(e)}")

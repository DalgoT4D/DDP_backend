from ninja.errors import HttpError
from sqlalchemy.sql.expression import column

from ddpui.models.org import OrgWarehouse
from ddpui.utils import secretsmanager
from ddpui.dbt_automation.utils.warehouseclient import get_client
from ddpui.datainsights.warehouse.warehouse_factory import WarehouseFactory
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
        logger.info(credentials)
        wclient = WarehouseFactory.connect(credentials, wtype=org_warehouse.wtype)

        # Use AggQueryBuilder to build the query
        builder = AggQueryBuilder()
        builder.add_column(column(xaxis_col))
        builder.add_column(column(yaxis_col))
        builder.fetch_from(table_name, schema_name)
        builder.offset_rows(offset)
        builder.limit_rows(limit)
        stmt = builder.build()

        stmt = stmt.compile(bind=wclient.engine, compile_kwargs={"literal_binds": True})
        sql = str(stmt)

        logger.info(f"Generated SQL query: {sql}")

        rows = wclient.execute(sql)

        return rows
    except Exception as e:
        logger.error(f"Failed to generate chart data: {e}")
        raise HttpError(500, f"Failed to fetch chart data: {str(e)}")

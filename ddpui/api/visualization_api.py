from ninja import Router, Schema
from ninja.errors import HttpError
from typing import List, Optional, Any, Dict
from sqlalchemy.sql.expression import column

from ddpui.auth import has_permission
from ddpui.models.org_user import OrgUser
from ddpui.models.org import OrgWarehouse
from ddpui.utils import secretsmanager
from ddpui.dbt_automation.utils.warehouseclient import get_client
from ddpui.datainsights.query_builder import AggQueryBuilder
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui")

visualization_router = Router()


class RawChartQueryRequest(Schema):
    chart_type: str
    schema_name: str
    table_name: str
    xaxis_col: str
    yaxis_col: str
    offset: int
    limit: int = 10


class ChartQueryResponse(Schema):
    data: List[Dict[str, Any]]


@visualization_router.post("/generate_chart/", response=ChartQueryResponse)
@has_permission(["can_view_warehouse_data"])
def generate_chart_data(request, payload: RawChartQueryRequest):
    """
    Generate chart data by running a custom query on the warehouse.
    Does not save any chart entry to the DB.
    """
    orguser: OrgUser = request.orguser
    org = orguser.org
    org_warehouse = OrgWarehouse.objects.filter(org=org).first()
    if not org_warehouse:
        raise HttpError(404, "Please set up your warehouse first")

    credentials = secretsmanager.retrieve_warehouse_credentials(org_warehouse)
    wclient = get_client(org_warehouse.wtype, credentials, org_warehouse.bq_location)

    # Use AggQueryBuilder to build the query
    builder = AggQueryBuilder()
    builder.add_column(column(payload.xaxis_col))
    builder.add_column(column(payload.yaxis_col))
    builder.fetch_from(payload.table_name, payload.schema_name)
    builder.offset_rows(payload.offset)
    builder.limit_rows(payload.limit)
    stmt = builder.build()
    sql = str(stmt)

    try:
        result = wclient.execute(sql)

        data = list(result)
    except Exception as e:
        logger.error(e)
        raise HttpError(500, f"Failed to fetch chart data: {str(e)}")

    return {"data": data}

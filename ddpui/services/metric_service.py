"""Metric service for business logic

Encapsulates all metric-related business logic,
separating it from the API layer for better testability.
"""

from typing import Optional, Tuple, List, Any

from django.db.models import Q
from sqlalchemy import literal_column

from ddpui.models.metric import Metric, AGGREGATION_CHOICES
from ddpui.models.org import Org, OrgWarehouse
from ddpui.models.org_user import OrgUser
from ddpui.models.visualization import Chart
from ddpui.core.datainsights.query_builder import AggQueryBuilder
from ddpui.utils.warehouse.client.warehouse_factory import WarehouseFactory
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui.metric_service")

VALID_AGGREGATIONS = [choice[0] for choice in AGGREGATION_CHOICES]


# ── Exceptions ──────────────────────────────────────────────────────────────


class MetricServiceError(Exception):
    def __init__(self, message: str, error_code: str = "METRIC_ERROR"):
        self.message = message
        self.error_code = error_code
        super().__init__(self.message)


class MetricNotFoundError(MetricServiceError):
    def __init__(self, metric_id: int):
        super().__init__(f"Metric with id {metric_id} not found", "METRIC_NOT_FOUND")
        self.metric_id = metric_id


class MetricValidationError(MetricServiceError):
    def __init__(self, message: str):
        super().__init__(message, "VALIDATION_ERROR")


class MetricDeleteBlockedError(MetricServiceError):
    def __init__(self, message: str, consumers: dict):
        super().__init__(message, "DELETE_BLOCKED")
        self.consumers = consumers


# ── Service ─────────────────────────────────────────────────────────────────


class MetricService:
    """Service class for metric-related operations"""

    @staticmethod
    def get_metric(metric_id: int, org: Org) -> Metric:
        try:
            return Metric.objects.get(id=metric_id, org=org)
        except Metric.DoesNotExist:
            raise MetricNotFoundError(metric_id)

    @staticmethod
    def list_metrics(
        org: Org,
        page: int = 1,
        page_size: int = 10,
        search: Optional[str] = None,
        schema_name: Optional[str] = None,
        table_name: Optional[str] = None,
    ) -> Tuple[List[Metric], int]:
        query = Q(org=org)

        if search:
            query &= Q(name__icontains=search) | Q(description__icontains=search)

        if schema_name:
            query &= Q(schema_name=schema_name)

        if table_name:
            query &= Q(table_name=table_name)

        queryset = Metric.objects.filter(query).order_by("-updated_at")
        total = queryset.count()

        offset = (page - 1) * page_size
        metrics = list(queryset[offset : offset + page_size])

        return metrics, total

    @staticmethod
    def validate_metric_definition(
        column: Optional[str],
        aggregation: Optional[str],
        column_expression: Optional[str],
    ):
        """Validate that exactly one definition path is provided."""
        has_simple = column is not None or aggregation is not None
        has_expression = column_expression is not None and column_expression.strip() != ""

        if has_simple and has_expression:
            raise MetricValidationError(
                "Provide either (column + aggregation) or column_expression, not both"
            )

        if not has_simple and not has_expression:
            raise MetricValidationError(
                "Provide either (column + aggregation) or column_expression"
            )

        if has_simple:
            if aggregation is None:
                raise MetricValidationError("aggregation is required for simple metrics")
            if aggregation not in VALID_AGGREGATIONS:
                raise MetricValidationError(
                    f"Invalid aggregation '{aggregation}'. Must be one of: {VALID_AGGREGATIONS}"
                )
            # column can be None for COUNT(*)
            if aggregation != "count" and column is None:
                raise MetricValidationError(f"column is required for aggregation '{aggregation}'")

    @staticmethod
    def validate_metric_against_warehouse(metric: Metric, org_warehouse: OrgWarehouse) -> None:
        """Execute a test query against the warehouse to validate the metric definition."""
        try:
            warehouse = WarehouseFactory.get_warehouse_client(org_warehouse)
            qb = AggQueryBuilder()
            qb.fetch_from(metric.table_name, metric.schema_name)

            if metric.column_expression:
                qb.add_column(literal_column(metric.column_expression).label("metric_value"))
            else:
                qb.add_aggregate_column(metric.column, metric.aggregation, alias="metric_value")

            qb.limit_rows(1)
            sql_stmt = qb.build()
            compiled = sql_stmt.compile(
                bind=warehouse.engine, compile_kwargs={"literal_binds": True}
            )
            warehouse.execute(compiled)
            logger.info(f"Metric validation query succeeded for '{metric.name}'")
        except Exception as e:
            error_msg = str(e)
            # Extract the useful part from psycopg2 errors
            if "\\n" in error_msg:
                error_msg = error_msg.split("\\n")[0]
            if "\n" in error_msg:
                error_msg = error_msg.split("\n")[0]
            raise MetricValidationError(f"Metric definition is invalid: {error_msg}")

    @staticmethod
    def create_metric(
        name: str,
        description: Optional[str],
        schema_name: str,
        table_name: str,
        column: Optional[str],
        aggregation: Optional[str],
        column_expression: Optional[str],
        orguser: OrgUser,
    ) -> Metric:
        MetricService.validate_metric_definition(column, aggregation, column_expression)

        # Check uniqueness
        if Metric.objects.filter(org=orguser.org, name=name).exists():
            raise MetricValidationError(f"A metric named '{name}' already exists")

        metric = Metric(
            name=name,
            description=description,
            schema_name=schema_name,
            table_name=table_name,
            column=column,
            aggregation=aggregation,
            column_expression=column_expression,
            org=orguser.org,
            created_by=orguser,
        )

        # Validate against warehouse before saving
        org_warehouse = OrgWarehouse.objects.filter(org=orguser.org).first()
        if org_warehouse:
            MetricService.validate_metric_against_warehouse(metric, org_warehouse)

        metric.save()
        logger.info(f"Created metric {metric.id} '{metric.name}' for org {orguser.org.id}")
        return metric

    @staticmethod
    def update_metric(
        metric_id: int,
        org: Org,
        orguser: OrgUser,
        **fields,
    ) -> Metric:
        metric = MetricService.get_metric(metric_id, org)

        definition_changed = False
        for field_name in [
            "column",
            "aggregation",
            "column_expression",
            "schema_name",
            "table_name",
        ]:
            if field_name in fields and fields[field_name] is not None:
                setattr(metric, field_name, fields[field_name])
                if field_name in ("column", "aggregation", "column_expression"):
                    definition_changed = True

        if "name" in fields and fields["name"] is not None:
            if Metric.objects.filter(org=org, name=fields["name"]).exclude(id=metric_id).exists():
                raise MetricValidationError(f"A metric named '{fields['name']}' already exists")
            metric.name = fields["name"]

        if "description" in fields and fields["description"] is not None:
            metric.description = fields["description"]

        if definition_changed:
            MetricService.validate_metric_definition(
                metric.column, metric.aggregation, metric.column_expression
            )
            org_warehouse = OrgWarehouse.objects.filter(org=org).first()
            if org_warehouse:
                MetricService.validate_metric_against_warehouse(metric, org_warehouse)

        metric.save()
        logger.info(f"Updated metric {metric.id}")
        return metric

    @staticmethod
    def delete_metric(metric_id: int, org: Org, orguser: OrgUser) -> bool:
        metric = MetricService.get_metric(metric_id, org)
        consumers = MetricService.get_metric_consumers(metric_id, org)

        if consumers["charts"] or consumers["kpis"]:
            raise MetricDeleteBlockedError(
                "Cannot delete metric: it is referenced by charts or KPIs",
                consumers,
            )

        metric_name = metric.name
        metric.delete()
        logger.info(f"Deleted metric '{metric_name}' (id={metric_id}) by {orguser.user.email}")
        return True

    @staticmethod
    def preview_metric_value(metric_id: int, org: Org) -> dict:
        """Compute the current value of a metric."""
        metric = MetricService.get_metric(metric_id, org)
        org_warehouse = OrgWarehouse.objects.filter(org=org).first()
        if not org_warehouse:
            return {"value": None, "error": "Warehouse not configured"}

        try:
            value = MetricService.compute_metric_value(metric, org_warehouse)
            return {"value": value, "error": None}
        except Exception as e:
            logger.error(f"Error computing metric {metric_id} value: {str(e)}")
            return {"value": None, "error": str(e)}

    @staticmethod
    def compute_metric_value(metric: Metric, org_warehouse: OrgWarehouse) -> Any:
        """Build and execute a query to compute the metric's scalar value."""
        warehouse = WarehouseFactory.get_warehouse_client(org_warehouse)
        qb = AggQueryBuilder()
        qb.fetch_from(metric.table_name, metric.schema_name)

        if metric.column_expression:
            qb.add_column(literal_column(metric.column_expression).label("metric_value"))
        else:
            qb.add_aggregate_column(metric.column, metric.aggregation, alias="metric_value")

        sql_stmt = qb.build()
        compiled = sql_stmt.compile(bind=warehouse.engine, compile_kwargs={"literal_binds": True})
        results = warehouse.execute(compiled)

        if results and len(results) > 0:
            row = results[0]
            value = row.get("metric_value") if isinstance(row, dict) else row[0]
            return float(value) if value is not None else None
        return None

    @staticmethod
    def get_metric_consumers(metric_id: int, org: Org) -> dict:
        """Find charts and KPIs that reference this metric."""
        # Verify metric exists
        MetricService.get_metric(metric_id, org)

        from ddpui.models.metric import KPI

        # KPIs via FK
        kpis = list(KPI.objects.filter(metric_id=metric_id, org=org).values("id", "name"))

        # Charts via saved_metric_id in extra_config
        charts = []
        for chart in Chart.objects.filter(org=org):
            ec = chart.extra_config or {}
            metrics_list = ec.get("metrics", [])
            for m in metrics_list:
                if isinstance(m, dict) and m.get("saved_metric_id") == metric_id:
                    charts.append(
                        {"id": chart.id, "title": chart.title, "chart_type": chart.chart_type}
                    )
                    break

        return {"charts": charts, "kpis": kpis}

"""KPI service for business logic"""

from typing import Optional, List, Any

from django.db.models import Q
from sqlalchemy import column, literal_column

from ddpui.models.metric import Metric, KPI
from ddpui.models.org import Org, OrgWarehouse
from ddpui.models.org_user import OrgUser
from ddpui.models.dashboard import Dashboard
from ddpui.core.datainsights.query_builder import AggQueryBuilder
from ddpui.core.charts.charts_service import apply_time_grain, format_time_grain_label
from ddpui.services.metric_service import MetricService
from ddpui.schemas.kpi_schema import KPICreate, KPIUpdate
from ddpui.utils.warehouse.client.warehouse_factory import WarehouseFactory
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui.kpi_service")

# Map KPI time_grain values to SQL DATE_TRUNC grain
TIME_GRAIN_TO_SQL = {
    "daily": "day",
    "weekly": "week",
    "monthly": "month",
    "quarterly": "quarter",
    "yearly": "year",
}

VALID_DIRECTIONS = ["increase", "decrease"]
VALID_TIME_GRAINS = list(TIME_GRAIN_TO_SQL.keys())
VALID_METRIC_TYPE_TAGS = ["input", "output", "outcome", "impact"]


# ── RAG Computation (pure function) ────────────────────────────────────────


def compute_rag_status(
    current_value: Optional[float],
    target_value: Optional[float],
    direction: str,
    green_pct: float,
    amber_pct: float,
) -> Optional[str]:
    """Compute RAG status. Returns 'green', 'amber', 'red', or None."""
    if target_value is None or current_value is None:
        return None
    if target_value == 0:
        return None
    achievement = (current_value / target_value) * 100
    if direction == "increase":
        if achievement >= green_pct:
            return "green"
        if achievement >= amber_pct:
            return "amber"
        return "red"
    else:  # decrease — lower is better
        if achievement <= green_pct:
            return "green"
        if achievement <= amber_pct:
            return "amber"
        return "red"


# ── Exceptions ──────────────────────────────────────────────────────────────


class KPIServiceError(Exception):
    def __init__(self, message: str, error_code: str = "KPI_ERROR"):
        self.message = message
        self.error_code = error_code
        super().__init__(self.message)


class KPINotFoundError(KPIServiceError):
    def __init__(self, kpi_id: int):
        super().__init__(f"KPI with id {kpi_id} not found", "KPI_NOT_FOUND")
        self.kpi_id = kpi_id


class KPIValidationError(KPIServiceError):
    def __init__(self, message: str):
        super().__init__(message, "VALIDATION_ERROR")


# ── Service ─────────────────────────────────────────────────────────────────


class KPIService:
    @staticmethod
    def _validate_fields(direction: str, time_grain: str, metric_type_tag: Optional[str]):
        if direction not in VALID_DIRECTIONS:
            raise KPIValidationError(
                f"Invalid direction '{direction}'. Must be one of: {VALID_DIRECTIONS}"
            )
        if time_grain not in VALID_TIME_GRAINS:
            raise KPIValidationError(
                f"Invalid time_grain '{time_grain}'. Must be one of: {VALID_TIME_GRAINS}"
            )
        if metric_type_tag and metric_type_tag not in VALID_METRIC_TYPE_TAGS:
            raise KPIValidationError(
                f"Invalid metric_type_tag '{metric_type_tag}'. Must be one of: {VALID_METRIC_TYPE_TAGS}"
            )

    @staticmethod
    def get_kpi(kpi_id: int, org: Org) -> KPI:
        try:
            return KPI.objects.select_related("metric").get(id=kpi_id, org=org)
        except KPI.DoesNotExist:
            raise KPINotFoundError(kpi_id)

    @staticmethod
    def list_kpis(
        org: Org,
        page: int = 1,
        page_size: int = 10,
        search: Optional[str] = None,
        program_tag: Optional[str] = None,
        metric_type: Optional[str] = None,
    ) -> tuple:
        query = Q(org=org)

        if search:
            query &= Q(name__icontains=search)
        if program_tag:
            query &= Q(program_tags__contains=[program_tag])
        if metric_type:
            query &= Q(metric_type_tag=metric_type)

        queryset = (
            KPI.objects.filter(query)
            .select_related("metric")
            .order_by("display_order", "-updated_at")
        )
        total = queryset.count()

        offset = (page - 1) * page_size
        kpis = list(queryset[offset : offset + page_size])
        return kpis, total

    @staticmethod
    def create_kpi(payload: KPICreate, orguser: OrgUser) -> KPI:
        KPIService._validate_fields(payload.direction, payload.time_grain, payload.metric_type_tag)

        # Verify metric exists and belongs to org
        metric = MetricService.get_metric(payload.metric_id, orguser.org)

        kpi = KPI.objects.create(
            metric=metric,
            name=payload.name or metric.name,
            target_value=payload.target_value,
            direction=payload.direction,
            green_threshold_pct=payload.green_threshold_pct,
            amber_threshold_pct=payload.amber_threshold_pct,
            time_grain=payload.time_grain,
            time_dimension_column=payload.time_dimension_column,
            trend_periods=payload.trend_periods,
            metric_type_tag=payload.metric_type_tag,
            program_tags=payload.program_tags,
            org=orguser.org,
            created_by=orguser,
        )

        logger.info(f"Created KPI {kpi.id} '{kpi.name}' for org {orguser.org.id}")
        return kpi

    @staticmethod
    def update_kpi(kpi_id: int, org: Org, orguser: OrgUser, payload: KPIUpdate) -> KPI:
        kpi = KPIService.get_kpi(kpi_id, org)

        update_data = payload.dict(exclude_unset=True)

        # Validate changed fields
        direction = update_data.get("direction", kpi.direction)
        time_grain = update_data.get("time_grain", kpi.time_grain)
        metric_type_tag = update_data.get("metric_type_tag", kpi.metric_type_tag)
        KPIService._validate_fields(direction, time_grain, metric_type_tag)

        for field_name, value in update_data.items():
            setattr(kpi, field_name, value)

        kpi.save()
        logger.info(f"Updated KPI {kpi.id}")
        return kpi

    @staticmethod
    def delete_kpi(kpi_id: int, org: Org, orguser: OrgUser) -> bool:
        kpi = KPIService.get_kpi(kpi_id, org)

        # Remove from any dashboard components
        for dashboard in Dashboard.objects.filter(org=org):
            if not dashboard.components:
                continue
            keys_to_remove = [
                comp_id
                for comp_id, comp in dashboard.components.items()
                if comp.get("type") == "kpi" and comp.get("config", {}).get("kpiId") == kpi_id
            ]
            if keys_to_remove:
                for key in keys_to_remove:
                    del dashboard.components[key]
                    dashboard.layout_config = [
                        item for item in (dashboard.layout_config or []) if item.get("i") != key
                    ]
                dashboard.save()

        kpi_name = kpi.name
        kpi.delete()
        logger.info(f"Deleted KPI '{kpi_name}' (id={kpi_id}) by {orguser.user.email}")
        return True

    @staticmethod
    def get_kpi_summary(org: Org) -> List[dict]:
        """Batch compute all KPIs with current values + RAG for the KPI page."""
        kpis = (
            KPI.objects.filter(org=org)
            .select_related("metric")
            .order_by("display_order", "-updated_at")
        )
        org_warehouse = OrgWarehouse.objects.filter(org=org).first()

        results = []
        for kpi in kpis:
            current_value = None
            if org_warehouse:
                try:
                    current_value = MetricService.compute_metric_value(kpi.metric, org_warehouse)
                except Exception as e:
                    logger.error(f"Error computing value for KPI {kpi.id}: {e}")

            rag_status = compute_rag_status(
                current_value,
                kpi.target_value,
                kpi.direction,
                kpi.green_threshold_pct,
                kpi.amber_threshold_pct,
            )

            achievement_pct = None
            if current_value is not None and kpi.target_value and kpi.target_value != 0:
                achievement_pct = round((current_value / kpi.target_value) * 100, 1)

            # Period-over-period change
            pop_change = None
            if org_warehouse and kpi.time_dimension_column:
                try:
                    trend = KPIService._compute_trend(kpi, org_warehouse, limit=2)
                    if (
                        len(trend) >= 2
                        and trend[-1]["value"] is not None
                        and trend[-2]["value"] is not None
                        and trend[-2]["value"] != 0
                    ):
                        pop_change = round(
                            ((trend[-1]["value"] - trend[-2]["value"]) / abs(trend[-2]["value"]))
                            * 100,
                            1,
                        )
                except Exception as e:
                    logger.error(f"Error computing trend for KPI {kpi.id}: {e}")

            results.append(
                {
                    "id": kpi.id,
                    "name": kpi.name,
                    "metric_name": kpi.metric.name,
                    "current_value": current_value,
                    "target_value": kpi.target_value,
                    "direction": kpi.direction,
                    "rag_status": rag_status,
                    "achievement_pct": achievement_pct,
                    "period_over_period_change": pop_change,
                    "time_grain": kpi.time_grain,
                    "metric_type_tag": kpi.metric_type_tag,
                    "program_tags": kpi.program_tags,
                    "updated_at": kpi.updated_at,
                }
            )

        return results

    @staticmethod
    def get_kpi_data(kpi_id: int, org: Org) -> dict:
        """Get KPI chart data + echarts config.

        Same pattern as chart data endpoint: returns {data, echarts_config}.
        """
        from ddpui.core.charts.echarts_config_generator import EChartsConfigGenerator

        kpi = KPIService.get_kpi(kpi_id, org)

        empty_result = {"data": {}, "echarts_config": {}}

        org_warehouse = OrgWarehouse.objects.filter(org=org).first()
        if not org_warehouse:
            return empty_result

        # Compute current value
        current_value = None
        try:
            current_value = MetricService.compute_metric_value(kpi.metric, org_warehouse)
        except Exception as e:
            logger.error(f"Error computing value for KPI {kpi.id}: {e}")

        # Compute RAG
        rag_status = compute_rag_status(
            current_value,
            kpi.target_value,
            kpi.direction,
            kpi.green_threshold_pct,
            kpi.amber_threshold_pct,
        )

        # Compute trend periods
        periods = []
        if kpi.time_dimension_column:
            try:
                periods = KPIService._compute_trend(kpi, org_warehouse)
            except Exception as e:
                logger.error(f"Error computing trend for KPI {kpi.id}: {e}")

        # Build data payload
        data = {
            "current_value": current_value,
            "target_value": kpi.target_value,
            "direction": kpi.direction,
            "rag_status": rag_status,
            "time_grain": kpi.time_grain,
            "periods": periods,
        }

        # Generate echarts config
        kpi_meta = {
            "name": kpi.name,
            "target_value": kpi.target_value,
            "direction": kpi.direction,
            "rag_status": rag_status,
            "current_value": current_value,
        }
        echarts_config = EChartsConfigGenerator.generate_kpi_trend_config(
            periods, kpi_meta, compact=False
        )

        return {"data": data, "echarts_config": echarts_config}

    @staticmethod
    def _compute_trend(
        kpi: KPI, org_warehouse: OrgWarehouse, limit: Optional[int] = None
    ) -> List[dict]:
        """Build and execute a time-series query for the KPI's metric."""
        metric = kpi.metric
        warehouse = WarehouseFactory.get_warehouse_client(org_warehouse)
        warehouse_type = org_warehouse.wtype.lower() if org_warehouse.wtype else "postgres"
        sql_grain = TIME_GRAIN_TO_SQL.get(kpi.time_grain, "month")

        qb = AggQueryBuilder()
        qb.fetch_from(metric.table_name, metric.schema_name)

        # Time dimension with grain
        time_col = column(kpi.time_dimension_column)
        time_expr = apply_time_grain(time_col, sql_grain, warehouse_type)
        time_col_labeled = time_expr.label("period")
        qb.add_column(time_col_labeled)
        qb.group_cols_by(time_col_labeled)

        # Metric aggregation
        if metric.column_expression:
            qb.add_column(literal_column(metric.column_expression).label("value"))
        else:
            qb.add_aggregate_column(metric.column, metric.aggregation, alias="value")

        # Order by period ascending, limit to trend_periods
        qb.order_cols_by([("period", "asc")])
        qb.limit_rows(limit or kpi.trend_periods)

        sql_stmt = qb.build()
        compiled = sql_stmt.compile(bind=warehouse.engine, compile_kwargs={"literal_binds": True})
        results = warehouse.execute(compiled)

        periods = []
        for row in results:
            period_val = row.get("period") if isinstance(row, dict) else row[0]
            value_val = row.get("value") if isinstance(row, dict) else row[1]
            period_label = format_time_grain_label(period_val, sql_grain)
            periods.append(
                {
                    "period": period_label,
                    "value": float(value_val) if value_val is not None else None,
                }
            )

        return periods

"""KPI service for business logic"""

from typing import Optional, List, Any
from datetime import date as date_type, datetime, timezone

from django.db.models import Q
from sqlalchemy import column, literal_column, func, and_

from ddpui.models.metric import Metric, KPI
from ddpui.models.org import Org, OrgWarehouse
from ddpui.models.org_user import OrgUser
from ddpui.models.dashboard import Dashboard, DashboardFilter
from ddpui.core.datainsights.query_builder import AggQueryBuilder
from ddpui.core.charts.charts_service import (
    apply_time_grain,
    format_time_grain_label,
    apply_dashboard_filters,
)
from ddpui.core.charts.echarts_config_generator import EChartsConfigGenerator
from ddpui.core.metric.metric_service import MetricService
from ddpui.services.dashboard_service import DashboardService
from ddpui.schemas.kpi_schema import (
    KPICreate,
    KPIUpdate,
    KPIResponse,
    KPIExtraConfig,
    AnnotationEntryResponse,
)
from ddpui.schemas.metric_schema import MetricResponse
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
    else:  # decrease — lower is better, check worst first
        if achievement > amber_pct:
            return "red"
        if achievement > green_pct:
            return "amber"
        return "green"


# ── Exceptions ──────────────────────────────────────────────────────────────


from ddpui.core.kpi.exceptions import (
    KPIServiceError,
    KPINotFoundError,
    KPIValidationError,
    KPIPermissionError,
)
from ddpui.core.ownership import can_delete_resource


# ── Service ─────────────────────────────────────────────────────────────────


class KPIService:
    @staticmethod
    def _validate_fields(
        direction: str,
        time_grain: str,
        metric_type_tag: Optional[str],
        green_pct: float,
        amber_pct: float,
    ):
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
        # RAG bands assume green is the easier target to clear and amber sits below it.
        # For increase, achievement is checked green-first (≥ green → green, ≥ amber → amber),
        # so green_pct must be ≥ amber_pct. For decrease, the comparisons invert.
        if direction == "increase" and green_pct < amber_pct:
            raise KPIValidationError(
                f"For direction 'increase', green_threshold_pct ({green_pct}) "
                f"must be ≥ amber_threshold_pct ({amber_pct})"
            )
        if direction == "decrease" and amber_pct < green_pct:
            raise KPIValidationError(
                f"For direction 'decrease', amber_threshold_pct ({amber_pct}) "
                f"must be ≥ green_threshold_pct ({green_pct})"
            )

    @staticmethod
    def kpi_to_response(kpi: KPI) -> KPIResponse:
        """Convert a KPI model instance to KPIResponse schema."""
        m = kpi.metric
        return KPIResponse(
            id=kpi.id,
            name=kpi.name,
            metric=MetricResponse(
                id=m.id,
                name=m.name,
                description=m.description,
                schema_name=m.schema_name,
                table_name=m.table_name,
                column=m.column,
                aggregation=m.aggregation,
                column_expression=m.column_expression,
                created_by=m.created_by.user.email,
                created_at=m.created_at,
                updated_at=m.updated_at,
            ),
            target_value=kpi.target_value,
            direction=kpi.direction,
            green_threshold_pct=kpi.green_threshold_pct,
            amber_threshold_pct=kpi.amber_threshold_pct,
            time_grain=kpi.time_grain,
            time_dimension_column=kpi.time_dimension_column,
            metric_type_tag=kpi.metric_type_tag,
            program_tags=kpi.program_tags,
            display_order=kpi.display_order,
            extra_config=KPIExtraConfig(**(kpi.extra_config or {})),
            created_by=kpi.created_by.user.email,
            created_at=kpi.created_at,
            updated_at=kpi.updated_at,
        )

    @staticmethod
    def get_kpi(kpi_id: int, org: Org) -> KPI:
        try:
            return KPI.objects.select_related("metric__created_by__user", "created_by__user").get(
                id=kpi_id, org=org
            )
        except KPI.DoesNotExist:
            raise KPINotFoundError(kpi_id)

    @staticmethod
    def get_all_program_tags(org: Org) -> List[str]:
        """Get all unique program tags across KPIs for the org."""
        tags = set()
        for kpi in KPI.objects.filter(org=org).values_list("program_tags", flat=True):
            for tag in kpi or []:
                tags.add(tag)
        return sorted(tags)

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
            query &= Q(name__icontains=search) | Q(program_tags__icontains=search)
        if program_tag:
            query &= Q(program_tags__contains=[program_tag])
        if metric_type:
            query &= Q(metric_type_tag=metric_type)

        queryset = (
            KPI.objects.filter(query)
            .select_related("metric__created_by__user", "created_by__user")
            .order_by("display_order", "-updated_at")
        )
        total = queryset.count()

        offset = (page - 1) * page_size
        kpis = list(queryset[offset : offset + page_size])
        return kpis, total

    @staticmethod
    def create_kpi(payload: KPICreate, orguser: OrgUser) -> KPI:
        KPIService._validate_fields(
            payload.direction,
            payload.time_grain,
            payload.metric_type_tag,
            payload.green_threshold_pct,
            payload.amber_threshold_pct,
        )

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
            metric_type_tag=payload.metric_type_tag,
            program_tags=payload.program_tags,
            extra_config=payload.extra_config.model_dump(),
            org=orguser.org,
            created_by=orguser,
            last_modified_by=orguser,
        )

        logger.info(f"Created KPI {kpi.id} '{kpi.name}' for org {orguser.org.id}")
        # Reload with relation chains prefetched so kpi_to_response (which reads
        # created_by.user.email and metric.created_by.user.email) does not fire lazy queries
        return KPI.objects.select_related("metric__created_by__user", "created_by__user").get(
            id=kpi.id
        )

    @staticmethod
    def update_kpi(kpi_id: int, org: Org, orguser: OrgUser, payload: KPIUpdate) -> KPI:
        kpi = KPIService.get_kpi(kpi_id, org)

        update_data = payload.model_dump(exclude_unset=True)

        # Handle metric change
        if "metric_id" in update_data:
            new_metric = MetricService.get_metric(update_data.pop("metric_id"), org)
            kpi.metric = new_metric

        # Validate changed fields
        direction = update_data.get("direction", kpi.direction)
        time_grain = update_data.get("time_grain", kpi.time_grain)
        metric_type_tag = update_data.get("metric_type_tag", kpi.metric_type_tag)
        green_pct = update_data.get("green_threshold_pct", kpi.green_threshold_pct)
        amber_pct = update_data.get("amber_threshold_pct", kpi.amber_threshold_pct)
        KPIService._validate_fields(direction, time_grain, metric_type_tag, green_pct, amber_pct)

        for field_name, value in update_data.items():
            setattr(kpi, field_name, value)

        kpi.last_modified_by = orguser
        kpi.save()
        logger.info(f"Updated KPI {kpi.id}")
        # Reload with relation chains prefetched (the in-memory instance may have a
        # reassigned metric); avoids lazy queries in kpi_to_response's created_by reads
        return KPI.objects.select_related("metric__created_by__user", "created_by__user").get(
            id=kpi.id
        )

    @staticmethod
    def get_kpi_consumers(kpi_id: int, org: Org) -> dict:
        """Find dashboards and alerts that reference this KPI.

        Alerts CASCADE on KPI delete — included for UI visibility (so the user
        sees what will be removed alongside) but do not contribute to delete-
        blocking.
        """
        KPIService.get_kpi(kpi_id, org)
        dashboards = KPIService.get_kpi_dashboards(kpi_id, org)

        from ddpui.models.alert import Alert  # local import to avoid circular

        alerts = list(
            Alert.objects.filter(kpi_id=kpi_id, org=org).values("id", "name", "alert_type")
        )
        return {"dashboards": dashboards, "alerts": alerts}

    @staticmethod
    def get_kpi_dashboards(kpi_id: int, org: Org) -> List[dict]:
        """Get list of dashboards that use this KPI."""
        KPIService.get_kpi(kpi_id, org)
        dashboards_with_kpi = []
        for dashboard in Dashboard.objects.filter(org=org):
            for tab in dashboard.tabs or []:
                for comp in (tab.get("components") or {}).values():
                    if comp.get("type") == "kpi" and comp.get("config", {}).get("kpiId") == kpi_id:
                        dashboards_with_kpi.append(
                            {
                                "id": dashboard.id,
                                "title": dashboard.title,
                                "dashboard_type": dashboard.dashboard_type,
                            }
                        )
                        break
                else:
                    continue
                break
        return dashboards_with_kpi

    @staticmethod
    def delete_kpi(kpi_id: int, org: Org, orguser: OrgUser) -> bool:
        kpi = KPIService.get_kpi(kpi_id, org)

        dashboards = KPIService.get_kpi_dashboards(kpi_id, org)
        if dashboards:
            names = ", ".join(d["title"] for d in dashboards)
            raise KPIValidationError(
                f"Cannot delete KPI '{kpi.name}' — it is used in: {names}. "
                "Remove it from these dashboards first."
            )

        if not can_delete_resource(orguser, kpi):
            raise KPIPermissionError("Only the owner or an admin can delete this KPI.")

        kpi_name = kpi.name
        kpi.delete()
        logger.info(f"Deleted KPI '{kpi_name}' (id={kpi_id}) by {orguser.user.email}")
        return True

    @staticmethod
    def get_kpi_summary(org: Org) -> List[dict]:
        """Batch compute all KPIs with current values + RAG for the KPI page."""
        kpis = (
            KPI.objects.filter(org=org)
            .select_related("metric__created_by__user", "created_by__user")
            .order_by("display_order", "-updated_at")
        )
        org_warehouse = OrgWarehouse.objects.filter(org=org).first()

        results = []
        for kpi in kpis:
            kpi_response = KPIService.kpi_to_response(kpi)

            current_value = None
            pop_change = None

            if org_warehouse:
                try:
                    trend = KPIService._compute_trend(kpi_response, org_warehouse, limit=2)
                    if trend:
                        current_value = trend[-1]["value"]
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
    def _compute_trend(
        kpi_response: KPIResponse,
        org_warehouse: OrgWarehouse,
        date_filter: Optional[dict] = None,
        limit: Optional[int] = None,
        dashboard_filters: Optional[List[dict]] = None,
    ) -> List[dict]:
        """Compute trend periods for a KPI. Single source of truth for trend queries.

        Returns list of {period: str, value: float|None} ordered ascending.
        """
        metric = kpi_response.metric
        if not kpi_response.time_dimension_column or not kpi_response.time_grain:
            return []

        warehouse = WarehouseFactory.get_warehouse_client(org_warehouse)
        warehouse_type = org_warehouse.wtype.lower() if org_warehouse.wtype else "postgres"
        sql_grain = TIME_GRAIN_TO_SQL.get(kpi_response.time_grain, "month")

        qb = AggQueryBuilder()
        qb.fetch_from(metric.table_name, metric.schema_name)

        time_col = column(kpi_response.time_dimension_column)
        time_expr = apply_time_grain(time_col, sql_grain, warehouse_type)
        time_col_labeled = time_expr.label("period")
        qb.add_column(time_col_labeled)
        qb.group_cols_by(time_col_labeled)

        if metric.column_expression:
            qb.add_column(literal_column(metric.column_expression).label("value"))
        else:
            qb.add_aggregate_column(metric.column, metric.aggregation, alias="value")

        if dashboard_filters:
            qb = apply_dashboard_filters(qb, dashboard_filters)

        qb.order_cols_by([("period", "asc")])
        if limit:
            qb.limit_rows(limit)

        sql_stmt = qb.build()
        compiled = sql_stmt.compile(bind=warehouse.engine, compile_kwargs={"literal_binds": True})
        results = warehouse.execute(compiled)

        periods = []
        for row in results:
            period_val = row.get("period")
            value_val = row.get("value")
            period_label = format_time_grain_label(period_val, sql_grain)
            periods.append(
                {
                    "period": period_label,
                    "period_date": str(period_val) if period_val is not None else None,
                    "value": float(value_val) if value_val is not None else None,
                }
            )

        # Filter periods by date range after grouping (preserves complete periods)
        if date_filter:
            start = datetime.fromisoformat(date_filter["start"]).date()
            end = datetime.fromisoformat(date_filter["end"]).date()
            filtered = []
            for p in periods:
                if not p["period_date"]:
                    continue
                try:
                    pd = datetime.fromisoformat(p["period_date"]).date()
                except (ValueError, TypeError) as e:
                    logger.warning(f"Skipping period with invalid date '{p['period_date']}': {e}")
                    continue
                if start <= pd <= end:
                    filtered.append(p)
            periods = filtered

        return periods

    @staticmethod
    def compute_latest_value(
        kpi: KPI, org_warehouse: OrgWarehouse
    ) -> tuple[Optional[float], str, Optional[str]]:
        """Most-recent-period value for the KPI + SQL string + RAG status.

        Used by the alert evaluator and the dry-run preview. Mirrors the
        "current value" shown in the KPI detail drawer: aggregates the
        underlying metric by the KPI's time_grain and returns the most recent
        complete period.

        If the KPI has no time_dimension_column/time_grain, falls back to a
        whole-dataset aggregate via MetricService.
        """
        metric = kpi.metric

        # Fallback: KPI without time grain → whole-dataset aggregate
        if not kpi.time_dimension_column or not kpi.time_grain:
            value, sql_str = MetricService.compute_metric_value_with_sql(metric, org_warehouse)
            rag = compute_rag_status(
                value,
                kpi.target_value,
                kpi.direction,
                kpi.green_threshold_pct,
                kpi.amber_threshold_pct,
            )
            return value, sql_str, rag

        warehouse = WarehouseFactory.get_warehouse_client(org_warehouse)
        warehouse_type = (org_warehouse.wtype or "postgres").lower()
        sql_grain = TIME_GRAIN_TO_SQL.get(kpi.time_grain, "month")

        qb = AggQueryBuilder()
        qb.fetch_from(metric.table_name, metric.schema_name)
        time_col = column(kpi.time_dimension_column)
        time_expr = apply_time_grain(time_col, sql_grain, warehouse_type).label("period")
        qb.add_column(time_expr)
        qb.group_cols_by(time_expr)
        if metric.column_expression:
            qb.add_column(literal_column(metric.column_expression).label("value"))
        else:
            qb.add_aggregate_column(metric.column, metric.aggregation, alias="value")
        qb.order_cols_by([("period", "desc")])
        qb.limit_rows(1)

        sql_stmt = qb.build()
        compiled = sql_stmt.compile(bind=warehouse.engine, compile_kwargs={"literal_binds": True})
        try:
            sql_str = str(compiled).strip()
        except Exception:
            sql_str = "<sql unavailable>"

        results = warehouse.execute(compiled)
        value: Optional[float] = None
        if results and len(results) > 0:
            row = results[0]
            raw = row.get("value")
            if raw is not None:
                try:
                    value = float(raw)
                except (TypeError, ValueError):
                    value = None

        rag = compute_rag_status(
            value,
            kpi.target_value,
            kpi.direction,
            kpi.green_threshold_pct,
            kpi.amber_threshold_pct,
        )
        return value, sql_str, rag

    @staticmethod
    def compute_kpi_data(
        kpi_response: KPIResponse,
        org: Org,
        date_filter: Optional[dict] = None,
        dashboard_filters: Optional[List[dict]] = None,
    ) -> dict:
        """Compute KPI data + echarts config.

        Used by:
          - Live KPI API (via get_kpi_data)
          - Report service (builds KPIResponse from frozen config)
        """
        empty_result = {"data": {}, "echarts_config": {}}

        org_warehouse = OrgWarehouse.objects.filter(org=org).first()
        if not org_warehouse:
            return empty_result

        if not kpi_response.metric.schema_name or not kpi_response.metric.table_name:
            return empty_result

        # Compute trend — current value is derived from the latest period
        periods = []
        try:
            periods = KPIService._compute_trend(
                kpi_response, org_warehouse, date_filter, dashboard_filters=dashboard_filters
            )
        except Exception as e:
            logger.error(f"Error computing KPI trend: {e}")

        current_value = periods[-1]["value"] if periods else None

        # RAG
        rag_status = compute_rag_status(
            current_value,
            kpi_response.target_value,
            kpi_response.direction,
            kpi_response.green_threshold_pct,
            kpi_response.amber_threshold_pct,
        )

        # Data last date: derive from last trend period label (no extra query)
        data_last_date = periods[-1]["period"] if periods else None

        # Build data
        data = {
            "current_value": current_value,
            "target_value": kpi_response.target_value,
            "direction": kpi_response.direction,
            "rag_status": rag_status,
            "time_grain": kpi_response.time_grain,
            "periods": periods,
            "data_last_date": data_last_date,
            # Display customizations — surfaced so the dashboard widget and
            # snapshot viewer can format the current value without needing a
            # second fetch for the KPI's extra_config.
            "customizations": (
                kpi_response.extra_config.customizations.model_dump()
                if kpi_response.extra_config and kpi_response.extra_config.customizations
                else None
            ),
        }

        # Generate echarts config
        kpi_meta = {
            "name": kpi_response.name,
            "target_value": kpi_response.target_value,
            "direction": kpi_response.direction,
            "rag_status": rag_status,
            "current_value": current_value,
        }
        echarts_config = (
            EChartsConfigGenerator.generate_kpi_trend_config(periods, kpi_meta, compact=False)
            if periods
            else {}
        )

        return {"data": data, "echarts_config": echarts_config}

    @staticmethod
    def get_kpi_data(
        kpi_id: int,
        org: Org,
        time_grain_override: Optional[str] = None,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        dashboard_filters: Optional[dict] = None,
    ) -> dict:
        """Get KPI chart data + echarts config (live from DB).

        Optional overrides:
          - time_grain_override: use this time grain instead of the KPI's default
          - date_from/date_to: filter trend data to this date range
          - dashboard_filters: raw {filter_id: value} dict from dashboard
        """
        kpi = KPIService.get_kpi(kpi_id, org)
        kpi_response = KPIService.kpi_to_response(kpi)

        # Override time grain if provided
        if time_grain_override and time_grain_override in VALID_TIME_GRAINS:
            kpi_response.time_grain = time_grain_override

        # Build date filter from query params
        date_filter = None
        if date_from and date_to and kpi.time_dimension_column:
            date_filter = {
                "column_name": kpi.time_dimension_column,
                "start": date_from,
                "end": date_to,
            }

        # Resolve dashboard filters (same as charts)
        resolved_dashboard_filters = None
        if dashboard_filters:
            metric = kpi.metric
            org_warehouse = OrgWarehouse.objects.filter(org=org).first()
            warehouse_client = (
                WarehouseFactory.get_warehouse_client(org_warehouse) if org_warehouse else None
            )
            filter_defs = DashboardFilter.objects.filter(id__in=dashboard_filters.keys())
            resolved_dashboard_filters = DashboardService.resolve_dashboard_filters_for_chart(
                dashboard_filters,
                [f.to_json() for f in filter_defs],
                metric.schema_name,
                metric.table_name,
                warehouse_client,
            )

        return KPIService.compute_kpi_data(
            kpi_response, org, date_filter=date_filter, dashboard_filters=resolved_dashboard_filters
        )

    # ── Annotation methods ──────────────────────────────────────────────

    @staticmethod
    def _annotation_entry_to_response(entry: dict) -> "AnnotationEntryResponse":
        """Convert an annotation dict to response schema."""
        return AnnotationEntryResponse(
            id=entry["id"],
            note_type=entry["note_type"],
            period_key=entry["period_key"],
            period_date=entry.get("period_date"),
            content=entry["content"],
            snapshot_value=entry.get("snapshot_value"),
            snapshot_pop_change=entry.get("snapshot_pop_change"),
            created_by_email=entry.get("created_by_email", ""),
            last_modified_by_email=entry.get(
                "last_modified_by_email", entry.get("created_by_email", "")
            ),
            created_at=entry["created_at"],
            updated_at=entry["updated_at"],
        )

    @staticmethod
    def list_annotations(kpi_id: int, org: Org) -> list:
        """List all annotation entries for a KPI, ordered by period_date descending."""
        kpi = KPIService.get_kpi(kpi_id, org)
        annotations = kpi.annotations or []
        annotations = sorted(annotations, key=lambda e: e.get("period_date") or "", reverse=True)
        return [KPIService._annotation_entry_to_response(e) for e in annotations]

    @staticmethod
    def create_annotation(kpi_id: int, org: Org, orguser: OrgUser, payload):
        """Create an annotation entry. Snapshot values come from the frontend."""

        kpi = KPIService.get_kpi(kpi_id, org)
        annotations = list(kpi.annotations or [])

        max_id = max((e["id"] for e in annotations), default=0)
        now = datetime.now(timezone.utc).isoformat()

        entry = {
            "id": max_id + 1,
            "note_type": payload.note_type,
            "period_key": payload.period_key,
            "period_date": payload.period_date,
            "content": payload.content,
            "snapshot_value": payload.snapshot_value,
            "snapshot_pop_change": payload.snapshot_pop_change,
            "created_by_email": orguser.user.email,
            "last_modified_by_email": orguser.user.email,
            "created_at": now,
            "updated_at": now,
        }
        annotations.append(entry)
        kpi.annotations = annotations
        kpi.save(update_fields=["annotations"])

        return KPIService._annotation_entry_to_response(entry)

    @staticmethod
    def update_annotation(kpi_id: int, entry_id: int, org: Org, orguser: OrgUser, payload):
        """Update an annotation entry."""

        kpi = KPIService.get_kpi(kpi_id, org)
        annotations = list(kpi.annotations or [])

        entry = next((e for e in annotations if e["id"] == entry_id), None)
        if not entry:
            raise KPINotFoundError(entry_id)

        if payload.content is not None:
            entry["content"] = payload.content
        if payload.note_type is not None:
            entry["note_type"] = payload.note_type
        if payload.period_key is not None:
            entry["period_key"] = payload.period_key
        if payload.period_date is not None:
            entry["period_date"] = payload.period_date
        if payload.snapshot_value is not None:
            entry["snapshot_value"] = payload.snapshot_value
        if payload.snapshot_pop_change is not None:
            entry["snapshot_pop_change"] = payload.snapshot_pop_change

        entry["last_modified_by_email"] = orguser.user.email
        entry["updated_at"] = datetime.now(timezone.utc).isoformat()

        kpi.annotations = annotations
        kpi.save(update_fields=["annotations"])

        return KPIService._annotation_entry_to_response(entry)

    @staticmethod
    def delete_annotation(kpi_id: int, entry_id: int, org: Org, orguser: OrgUser) -> bool:
        """Delete an annotation entry. Only the creator can delete."""
        kpi = KPIService.get_kpi(kpi_id, org)
        annotations = list(kpi.annotations or [])

        entry = next((e for e in annotations if e["id"] == entry_id), None)
        if not entry:
            raise KPINotFoundError(entry_id)

        if entry.get("created_by_email") != orguser.user.email:
            raise KPIValidationError("Only the creator can delete this note")

        annotations = [e for e in annotations if e["id"] != entry_id]

        kpi.annotations = annotations
        kpi.save(update_fields=["annotations"])

        return True

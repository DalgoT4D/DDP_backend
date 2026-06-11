"""Alert SQL building + execution.

The evaluator and dry-run both call `compute(alert, org_warehouse)` (or the
dry-run-friendly `compute_from_config(...)`) to:

1. Build SQL appropriate to the alert type
2. Execute against the org's warehouse
3. Return (value, sql_string, rag_status_or_None)

For each alert type:
  - metric_threshold:  one aggregate (sum/avg/etc.) over the metric's
    table+column. Delegates to MetricService for parity with the wizard preview.
  - kpi_rag: same shape as the underlying KPI's trend query but limited to the
    most-recent period (matches dashboard semantics). RAG status is then
    computed from the value via `compute_rag_status`.
  - standalone: AggQueryBuilder driven by the alert's stored standalone_config.

Errors bubble up — callers wrap in their own try/except (evaluator records the
exception into the AlertLog snapshot; dry-run surfaces it in the response).
"""

from __future__ import annotations

from typing import Any, Optional, Tuple

from sqlalchemy import column, literal_column

from ddpui.core.alerts.exceptions import AlertValidationError
from ddpui.core.charts.charts_service import apply_time_grain
from ddpui.core.datainsights.query_builder import AggQueryBuilder
from ddpui.core.kpi.kpi_service import TIME_GRAIN_TO_SQL, compute_rag_status
from ddpui.models.alert import Alert, AlertType
from ddpui.models.metric import KPI, Metric
from ddpui.models.org import OrgWarehouse
from ddpui.utils.warehouse.client.warehouse_factory import WarehouseFactory


def _sql_string(compiled) -> str:
    """Pretty-stringify a SQLAlchemy compiled statement for AlertLog audit."""
    try:
        return str(compiled).strip()
    except Exception:  # pragma: no cover — defensive
        return "<sql unavailable>"


# ── Per-type executors ──────────────────────────────────────────────────────


def _execute_metric(metric: Metric, org_warehouse: OrgWarehouse) -> Tuple[Optional[float], str]:
    """Aggregate the metric column over the whole table — same as the wizard preview."""
    warehouse = WarehouseFactory.get_warehouse_client(org_warehouse)
    qb = AggQueryBuilder()
    qb.fetch_from(metric.table_name, metric.schema_name)
    if metric.column_expression:
        qb.add_column(literal_column(metric.column_expression).label("metric_value"))
    else:
        qb.add_aggregate_column(metric.column, metric.aggregation, alias="metric_value")

    sql_stmt = qb.build()
    compiled = sql_stmt.compile(bind=warehouse.engine, compile_kwargs={"literal_binds": True})
    sql_str = _sql_string(compiled)

    results = warehouse.execute(compiled)
    value = _extract_scalar(results, "metric_value")
    return value, sql_str


def _execute_kpi(
    kpi: KPI, org_warehouse: OrgWarehouse
) -> Tuple[Optional[float], str, Optional[str]]:
    """Most-recent-period value for the KPI's metric, then derive RAG status.

    Mirrors `KPIService._compute_trend` with limit=1 + descending order so we
    pick up the most recent complete period — matches the value shown in the
    KPI drawer.
    """
    metric = kpi.metric
    if not kpi.time_dimension_column or not kpi.time_grain:
        # Fall back to whole-dataset aggregate when the KPI has no time grain
        value, sql_str = _execute_metric(metric, org_warehouse)
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
    sql_str = _sql_string(compiled)

    results = warehouse.execute(compiled)
    value = _extract_scalar(results, "value")
    rag = compute_rag_status(
        value,
        kpi.target_value,
        kpi.direction,
        kpi.green_threshold_pct,
        kpi.amber_threshold_pct,
    )
    return value, sql_str, rag


def _execute_standalone(
    standalone_config: dict, org_warehouse: OrgWarehouse
) -> Tuple[Optional[float], str]:
    """Standalone alerts compute an aggregate over an arbitrary dataset."""
    if not standalone_config:
        raise AlertValidationError("standalone_config is required for standalone alerts")
    schema_name = standalone_config.get("schema_name")
    table_name = standalone_config.get("table_name")
    column_name = standalone_config.get("column")
    aggregation = standalone_config.get("aggregation")
    column_expression = standalone_config.get("column_expression")
    if not schema_name or not table_name:
        raise AlertValidationError("schema_name and table_name are required")
    if not column_expression and not aggregation:
        raise AlertValidationError("aggregation is required when column_expression is empty")

    warehouse = WarehouseFactory.get_warehouse_client(org_warehouse)
    qb = AggQueryBuilder()
    qb.fetch_from(table_name, schema_name)
    if column_expression:
        qb.add_column(literal_column(column_expression).label("value"))
    else:
        # `column` may be empty for COUNT(*) — AggQueryBuilder handles None.
        col_arg = column_name if column_name else None
        qb.add_aggregate_column(col_arg, aggregation, alias="value")

    # Filters: list of {column, operator, value}. v1 supports the basic ops.
    for f in standalone_config.get("filters") or []:
        fcol = f.get("column")
        op = (f.get("operator") or "").lower()
        val = f.get("value")
        if not fcol or not op:
            continue
        c = column(fcol)
        if op == "eq":
            qb.where_clause(c == val)
        elif op == "neq":
            qb.where_clause(c != val)
        elif op == "gt":
            qb.where_clause(c > val)
        elif op == "lt":
            qb.where_clause(c < val)
        elif op == "gte":
            qb.where_clause(c >= val)
        elif op == "lte":
            qb.where_clause(c <= val)
        # unknown operators silently ignored — schema validation prevents these

    sql_stmt = qb.build()
    compiled = sql_stmt.compile(bind=warehouse.engine, compile_kwargs={"literal_binds": True})
    sql_str = _sql_string(compiled)
    results = warehouse.execute(compiled)
    value = _extract_scalar(results, "value")
    return value, sql_str


# ── Public API ──────────────────────────────────────────────────────────────


def compute(
    alert: Alert, org_warehouse: OrgWarehouse
) -> Tuple[Optional[float], str, Optional[str]]:
    """Compute (value, sql_string, rag_status_or_None) for a saved alert."""
    if alert.alert_type == AlertType.METRIC_THRESHOLD:
        if not alert.metric_id or not alert.metric:
            raise AlertValidationError("metric is required for metric_threshold alerts")
        value, sql_str = _execute_metric(alert.metric, org_warehouse)
        return value, sql_str, None

    if alert.alert_type == AlertType.KPI_RAG:
        if not alert.kpi_id or not alert.kpi:
            raise AlertValidationError("kpi is required for kpi_rag alerts")
        return _execute_kpi(alert.kpi, org_warehouse)

    if alert.alert_type == AlertType.STANDALONE:
        value, sql_str = _execute_standalone(alert.standalone_config or {}, org_warehouse)
        return value, sql_str, None

    raise AlertValidationError(f"unknown alert_type {alert.alert_type!r}")


def compute_from_config(
    alert_type: str,
    org_warehouse: OrgWarehouse,
    *,
    metric_id: Optional[int] = None,
    kpi_id: Optional[int] = None,
    standalone_config: Optional[dict] = None,
) -> Tuple[Optional[float], str, Optional[str]]:
    """Same as `compute` but takes raw inputs — used by the dry-run endpoint
    before an Alert row exists."""
    if alert_type == AlertType.METRIC_THRESHOLD:
        if not metric_id:
            raise AlertValidationError("metric_id is required for metric_threshold alerts")
        metric = Metric.objects.filter(id=metric_id).first()
        if not metric:
            raise AlertValidationError(f"metric {metric_id} not found")
        value, sql_str = _execute_metric(metric, org_warehouse)
        return value, sql_str, None

    if alert_type == AlertType.KPI_RAG:
        if not kpi_id:
            raise AlertValidationError("kpi_id is required for kpi_rag alerts")
        kpi = KPI.objects.select_related("metric").filter(id=kpi_id).first()
        if not kpi:
            raise AlertValidationError(f"kpi {kpi_id} not found")
        return _execute_kpi(kpi, org_warehouse)

    if alert_type == AlertType.STANDALONE:
        if not standalone_config:
            raise AlertValidationError("standalone_config is required")
        value, sql_str = _execute_standalone(standalone_config, org_warehouse)
        return value, sql_str, None

    raise AlertValidationError(f"unknown alert_type {alert_type!r}")


# ── Helpers ─────────────────────────────────────────────────────────────────


def _extract_scalar(results: Any, key: str) -> Optional[float]:
    if not results:
        return None
    row = results[0]
    value = row.get(key) if isinstance(row, dict) else row[0]
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None

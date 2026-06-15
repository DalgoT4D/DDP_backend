"""Alert SQL building + execution.

The evaluator and dry-run both call `compute(alert, org_warehouse)` (or the
dry-run-friendly `compute_from_config(...)`) to:

1. Build SQL appropriate to the alert type
2. Execute against the org's warehouse
3. Return (value, sql_string, rag_status_or_None)

For each alert type:
  - metric_threshold: delegates to `MetricService.compute_metric_value_with_sql`
    so the alert sees exactly what the metric service computes elsewhere.
  - kpi_rag: delegates to `KPIService.compute_latest_value` which returns the
    most-recent-period value, the SQL it ran, and the derived RAG status.
  - standalone: AggQueryBuilder driven by the alert's stored standalone_config.
    No service exists for this — alerts own the standalone case end-to-end.

Errors bubble up — callers wrap in their own try/except (evaluator records the
exception into the AlertLog snapshot; dry-run surfaces it in the response).
"""

from __future__ import annotations

from typing import Any, Optional, Tuple

from sqlalchemy import column, literal_column

from ddpui.core.alerts.exceptions import AlertValidationError
from ddpui.core.datainsights.query_builder import AggQueryBuilder
from ddpui.core.kpi.kpi_service import KPIService
from ddpui.core.metric.metric_service import MetricService
from ddpui.models.alert import Alert, AlertType
from ddpui.models.metric import KPI, Metric
from ddpui.models.org import OrgWarehouse
from ddpui.schemas.alert_schema import StandaloneConfig
from ddpui.utils.warehouse.client.warehouse_factory import WarehouseFactory


def _sql_string(compiled) -> str:
    """Pretty-stringify a SQLAlchemy compiled statement for AlertLog audit."""
    try:
        return str(compiled).strip()
    except Exception:  # pragma: no cover — defensive
        return "<sql unavailable>"


def _execute_standalone(
    standalone_config: StandaloneConfig, org_warehouse: OrgWarehouse
) -> Tuple[Optional[float], str]:
    """Standalone alerts compute an aggregate over an arbitrary dataset.

    Cross-field rule: either `column_expression` (Calculated) or `aggregation`
    (Simple) must be set. The Pydantic schema makes them both optional so we
    enforce this here rather than at the schema layer.
    """
    if not standalone_config.column_expression and not standalone_config.aggregation:
        raise AlertValidationError("aggregation is required when column_expression is empty")

    warehouse = WarehouseFactory.get_warehouse_client(org_warehouse)
    qb = AggQueryBuilder()
    qb.fetch_from(standalone_config.table_name, standalone_config.schema_name)
    if standalone_config.column_expression:
        qb.add_column(literal_column(standalone_config.column_expression).label("value"))
    else:
        # `column` may be empty for COUNT(*) — AggQueryBuilder handles None.
        col_arg = standalone_config.column or None
        qb.add_aggregate_column(col_arg, standalone_config.aggregation, alias="value")

    # Filters: typed FilterClause list. v1 supports the basic ops.
    for f in standalone_config.filters:
        op = (f.operator or "").lower()
        if not f.column or not op:
            continue
        c = column(f.column)
        if op == "eq":
            qb.where_clause(c == f.value)
        elif op == "neq":
            qb.where_clause(c != f.value)
        elif op == "gt":
            qb.where_clause(c > f.value)
        elif op == "lt":
            qb.where_clause(c < f.value)
        elif op == "gte":
            qb.where_clause(c >= f.value)
        elif op == "lte":
            qb.where_clause(c <= f.value)
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
        value, sql_str = MetricService.compute_metric_value_with_sql(alert.metric, org_warehouse)
        return value, sql_str, None

    if alert.alert_type == AlertType.KPI_RAG:
        if not alert.kpi_id or not alert.kpi:
            raise AlertValidationError("kpi is required for kpi_rag alerts")
        return KPIService.compute_latest_value(alert.kpi, org_warehouse)

    if alert.alert_type == AlertType.STANDALONE:
        if not alert.standalone_config:
            raise AlertValidationError("standalone_config is required for standalone alerts")
        cfg = StandaloneConfig(**alert.standalone_config)
        value, sql_str = _execute_standalone(cfg, org_warehouse)
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
        value, sql_str = MetricService.compute_metric_value_with_sql(metric, org_warehouse)
        return value, sql_str, None

    if alert_type == AlertType.KPI_RAG:
        if not kpi_id:
            raise AlertValidationError("kpi_id is required for kpi_rag alerts")
        kpi = KPI.objects.select_related("metric").filter(id=kpi_id).first()
        if not kpi:
            raise AlertValidationError(f"kpi {kpi_id} not found")
        return KPIService.compute_latest_value(kpi, org_warehouse)

    if alert_type == AlertType.STANDALONE:
        if not standalone_config:
            raise AlertValidationError("standalone_config is required")
        cfg = StandaloneConfig(**standalone_config)
        value, sql_str = _execute_standalone(cfg, org_warehouse)
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

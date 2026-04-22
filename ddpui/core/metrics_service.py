"""
Metric + KPI warehouse service — Batch 1 rewrite.

Handles:
  - Compiling a Metric's value expression (Simple multi-term formula, or Calculated SQL)
  - Validating Calculated-SQL for safety (no DML, no multi-statement)
  - Fetching current value + trend for a Metric
  - Computing RAG for a KPI (wraps a Metric + target + thresholds + direction)
  - Bulk fetch for /api/metrics/data/ and /api/kpis/data/
"""

import re
import traceback
from typing import List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

from sqlalchemy import column, literal_column, cast, and_, or_
from sqlalchemy.dialects.postgresql import TIMESTAMP as PG_TIMESTAMP

from ddpui.models.org import OrgWarehouse
from ddpui.models.metrics import Metric, KPI
from ddpui.core.datainsights.query_builder import AggQueryBuilder
from ddpui.core.charts.charts_service import apply_time_grain
from ddpui.utils.warehouse.client.warehouse_factory import WarehouseFactory
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui")


# ═══════════════════════════════════════════════════════════════════════════
# Safety / validation
# ═══════════════════════════════════════════════════════════════════════════

_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_TERM_ID_RE = re.compile(r"^t\d+$")
_FORMULA_ALLOWED_RE = re.compile(r"^[A-Za-z0-9_+\-*/()\s.]*$")
_FORMULA_TOKEN_RE = re.compile(r"t\d+")

_SQL_FORBIDDEN_RE = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|TRUNCATE|ALTER|CREATE|GRANT|REVOKE|REPLACE|MERGE|CALL|EXEC|EXECUTE)\b",
    re.IGNORECASE,
)

_AGG_SQL = {
    "sum": "SUM",
    "avg": "AVG",
    "count": "COUNT",
    "min": "MIN",
    "max": "MAX",
    "count_distinct": "COUNT",  # wrapped as COUNT(DISTINCT col)
}


class MetricCompileError(Exception):
    """Raised when a Metric's definition can't be compiled into safe SQL."""


def _validate_identifier(name: str, kind: str) -> str:
    if not name or not _IDENT_RE.match(name):
        raise MetricCompileError(f"Invalid {kind}: {name!r}")
    return name


def validate_sql_expression(expr: str) -> None:
    """Raise MetricCompileError if a Calculated-SQL expression fails safety checks.

    Guards (not a full parser — defense-in-depth):
      - No DML / DDL keywords
      - No semicolons (prevents statement injection)
      - No SQL comment markers
    """
    if not expr or not expr.strip():
        raise MetricCompileError("SQL expression is empty")
    if ";" in expr:
        raise MetricCompileError("SQL expression may not contain ';'")
    if "--" in expr or "/*" in expr or "*/" in expr:
        raise MetricCompileError("SQL expression may not contain comments")
    if _SQL_FORBIDDEN_RE.search(expr):
        raise MetricCompileError(
            "SQL expression contains a forbidden keyword (INSERT/UPDATE/DELETE/DROP/ALTER/…)"
        )


def compile_simple_expression(terms: list, formula: str) -> str:
    """Turn a Simple-mode Metric's typed terms + formula into a safe SQL expression string.

    Example:
        terms = [{"id": "t1", "agg": "avg", "column": "a"},
                 {"id": "t2", "agg": "avg", "column": "b"}]
        formula = "t1 - t2"
        → "AVG(\"a\") - AVG(\"b\")"
    """
    if not terms:
        raise MetricCompileError("Simple metric needs at least one term")
    formula = (formula or "").strip()
    if not formula:
        # Default: if a single term and no formula, treat as just that term.
        if len(terms) == 1:
            formula = terms[0]["id"]
        else:
            raise MetricCompileError("Simple metric with multiple terms requires a formula")

    if not _FORMULA_ALLOWED_RE.match(formula):
        raise MetricCompileError(
            "Formula may only contain term IDs, numbers, parentheses, and + - * /"
        )

    # Validate each term, build a substitution map.
    subs = {}
    for t in terms:
        tid = t.get("id", "")
        if not _TERM_ID_RE.match(tid):
            raise MetricCompileError(f"Invalid term id: {tid!r}")
        agg = t.get("agg", "").lower()
        if agg not in _AGG_SQL:
            raise MetricCompileError(f"Unknown aggregation: {agg!r}")
        col_name = _validate_identifier(t.get("column", ""), "column")
        if agg == "count_distinct":
            subs[tid] = f'COUNT(DISTINCT "{col_name}")'
        elif agg == "count" and not col_name:
            subs[tid] = "COUNT(*)"
        else:
            subs[tid] = f'{_AGG_SQL[agg]}("{col_name}")'

    # Ensure every tN in the formula has a term.
    used = set(_FORMULA_TOKEN_RE.findall(formula))
    missing = used - subs.keys()
    if missing:
        raise MetricCompileError(f"Formula references undefined terms: {sorted(missing)}")

    # Substitute longest-first to avoid "t1" clobbering "t10".
    expr = formula
    for tid in sorted(subs.keys(), key=len, reverse=True):
        expr = re.sub(r"\b" + re.escape(tid) + r"\b", subs[tid], expr)
    return expr


def compile_metric_value_expression(metric: Metric) -> str:
    """Return the SQL value expression (without SELECT/FROM) for a Metric."""
    if metric.creation_mode == "sql":
        validate_sql_expression(metric.sql_expression or "")
        return f"({metric.sql_expression})"
    return compile_simple_expression(metric.simple_terms or [], metric.simple_formula or "")


# ═══════════════════════════════════════════════════════════════════════════
# Filter building
# ═══════════════════════════════════════════════════════════════════════════

_FILTER_OPERATORS = {"=", "!=", ">", "<", ">=", "<=", "contains", "not contains"}


def _apply_filters(builder: AggQueryBuilder, filters: list) -> None:
    """Attach filters to an AggQueryBuilder via where_clause()."""
    for f in filters or []:
        col_name = _validate_identifier(f.get("column", ""), "filter column")
        op = f.get("operator", "=")
        if op not in _FILTER_OPERATORS:
            raise MetricCompileError(f"Unsupported filter operator: {op!r}")
        val = f.get("value", "")
        col_expr = column(col_name)

        if op == "=":
            builder.where_clause(col_expr == val)
        elif op == "!=":
            builder.where_clause(col_expr != val)
        elif op == ">":
            builder.where_clause(col_expr > val)
        elif op == "<":
            builder.where_clause(col_expr < val)
        elif op == ">=":
            builder.where_clause(col_expr >= val)
        elif op == "<=":
            builder.where_clause(col_expr <= val)
        elif op == "contains":
            builder.where_clause(col_expr.contains(val))
        elif op == "not contains":
            builder.where_clause(~col_expr.contains(val))


# ═══════════════════════════════════════════════════════════════════════════
# Query execution
# ═══════════════════════════════════════════════════════════════════════════


def _compile_and_execute(warehouse_client, query):
    compiled = query.compile(
        bind=warehouse_client.engine,
        compile_kwargs={"literal_binds": True},
    )
    logger.info(f"Executing SQL: {compiled}")
    return warehouse_client.execute(compiled)


def fetch_current_value(warehouse_client, metric: Metric) -> Tuple[Optional[float], Optional[str]]:
    """Run SELECT <metric_expr> FROM schema.table [WHERE filters]. Returns (value, error)."""
    try:
        value_expr = compile_metric_value_expression(metric)
        builder = AggQueryBuilder()
        builder.fetch_from(metric.table_name, metric.schema_name)
        builder.add_column(literal_column(value_expr).label("metric_value"))
        _apply_filters(builder, metric.filters)
        query = builder.build()

        rows = _compile_and_execute(warehouse_client, query)
        if rows:
            row = rows[0] if isinstance(rows[0], dict) else dict(rows[0])
            val = row.get("metric_value")
            if val is not None:
                return float(val), None
            return None, "Query returned null"
        return None, "Query returned no rows"
    except MetricCompileError as e:
        return None, f"CompileError: {e}"
    except Exception as e:
        logger.error(
            f"Metric {metric.id} current-value failed: {e}\n{traceback.format_exc()}"
        )
        return None, f"{type(e).__name__}: {e}"


def fetch_trend_data(
    warehouse_client,
    metric: Metric,
    warehouse_type: str,
    *,
    time_grain: Optional[str] = None,
    trend_periods: int = 12,
) -> Tuple[list, Optional[str]]:
    """Return (list_of_trend_points, error). Empty list if the metric has no time_column."""
    if not metric.time_column:
        return [], None

    grain = time_grain or metric.default_time_grain or "month"

    try:
        _validate_identifier(metric.time_column, "time_column")
        value_expr = compile_metric_value_expression(metric)

        time_col = column(metric.time_column)
        if warehouse_type.lower() in {"postgres", "postgresql"}:
            time_col = cast(time_col, PG_TIMESTAMP)
        time_expr = apply_time_grain(time_col, grain, warehouse_type)

        builder = AggQueryBuilder()
        builder.fetch_from(metric.table_name, metric.schema_name)
        builder.add_column(time_expr.label("period"))
        builder.add_column(literal_column(value_expr).label("metric_value"))
        _apply_filters(builder, metric.filters)
        builder.group_cols_by(time_expr)
        # Most recent N periods, returned newest-first; API layer flips for display.
        builder.order_by_clauses.append(literal_column("period").desc())
        builder.limit_rows(trend_periods)
        query = builder.build()

        rows = _compile_and_execute(warehouse_client, query)
        trend = []
        for raw in rows:
            row = raw if isinstance(raw, dict) else dict(raw)
            period_val = row.get("period")
            if period_val is None:
                period_str = "Unknown"
            elif grain == "month":
                try:
                    period_str = period_val.strftime("%Y-%m")
                except AttributeError:
                    period_str = str(period_val)[:7]
            elif grain == "quarter":
                try:
                    q = (period_val.month - 1) // 3 + 1
                    period_str = f"{period_val.year}-Q{q}"
                except AttributeError:
                    period_str = str(period_val)
            elif grain == "year":
                try:
                    period_str = str(period_val.year)
                except AttributeError:
                    period_str = str(period_val)[:4]
            else:
                period_str = str(period_val)
            val = row.get("metric_value")
            trend.append(
                {
                    "period": period_str,
                    "value": float(val) if val is not None else None,
                }
            )
        # Reverse to oldest → newest for chart rendering.
        trend.reverse()
        return trend, None
    except MetricCompileError as e:
        return [], f"CompileError: {e}"
    except Exception as e:
        logger.error(
            f"Metric {metric.id} trend fetch failed: {e}\n{traceback.format_exc()}"
        )
        return [], f"{type(e).__name__}: {e}"


# ═══════════════════════════════════════════════════════════════════════════
# RAG + period-over-period
# ═══════════════════════════════════════════════════════════════════════════


def compute_rag_status(
    current_value: Optional[float],
    target_value: Optional[float],
    amber_pct: float,
    green_pct: float,
    direction: str,
) -> tuple:
    """Returns (rag_status, achievement_pct)."""
    if target_value is None or target_value == 0 or current_value is None:
        return "grey", None

    achievement_pct = round((current_value / target_value) * 100, 1)

    if direction == "decrease":
        if achievement_pct <= green_pct:
            return "green", achievement_pct
        if achievement_pct <= amber_pct:
            return "amber", achievement_pct
        return "red", achievement_pct

    if achievement_pct >= green_pct:
        return "green", achievement_pct
    if achievement_pct >= amber_pct:
        return "amber", achievement_pct
    return "red", achievement_pct


def compute_period_over_period(trend: list) -> tuple:
    """Given a sorted (oldest → newest) trend list, return (delta, pct_change).

    Returns (None, None) when fewer than 2 non-null points are available.
    """
    numeric = [p for p in trend if p.get("value") is not None]
    if len(numeric) < 2:
        return None, None
    prev, curr = numeric[-2]["value"], numeric[-1]["value"]
    delta = curr - prev
    pct = round((delta / prev) * 100, 1) if prev != 0 else None
    return delta, pct


# ═══════════════════════════════════════════════════════════════════════════
# Bulk fetchers (API layer entry points)
# ═══════════════════════════════════════════════════════════════════════════


def fetch_metrics_data(
    org_warehouse: OrgWarehouse,
    metrics: List[Metric],
    *,
    include_trend: bool = False,
) -> list:
    """Return list of {metric_id, current_value, trend, error} for the Metric library."""
    if not metrics:
        return []

    try:
        wclient = WarehouseFactory.get_warehouse_client(org_warehouse)
    except Exception as e:
        error_msg = f"Warehouse connection failed: {type(e).__name__}: {e}"
        logger.error(error_msg)
        return [
            {"metric_id": m.id, "current_value": None, "trend": [], "error": error_msg}
            for m in metrics
        ]

    wtype = org_warehouse.wtype
    results = {}

    def _process(m: Metric):
        errors = []
        value, verr = fetch_current_value(wclient, m)
        if verr:
            errors.append(f"Value: {verr}")

        trend = []
        if include_trend:
            trend, terr = fetch_trend_data(
                wclient, m, wtype,
                time_grain=m.default_time_grain,
                trend_periods=12,
            )
            if terr:
                errors.append(f"Trend: {terr}")
        return {
            "metric_id": m.id,
            "current_value": value,
            "trend": trend,
            "error": "; ".join(errors) if errors else None,
        }

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(_process, m): m for m in metrics}
        for fut in as_completed(futures):
            m = futures[fut]
            try:
                results[m.id] = fut.result()
            except Exception as e:
                results[m.id] = {
                    "metric_id": m.id,
                    "current_value": None,
                    "trend": [],
                    "error": f"{type(e).__name__}: {e}",
                }
    return [results[m.id] for m in metrics if m.id in results]


def fetch_kpis_data(org_warehouse: OrgWarehouse, kpis: List[KPI]) -> list:
    """Return list of {kpi_id, current_value, rag_status, achievement_pct, trend,
    period_over_period_delta, period_over_period_pct, error} for the KPIs page.
    """
    if not kpis:
        return []

    try:
        wclient = WarehouseFactory.get_warehouse_client(org_warehouse)
    except Exception as e:
        error_msg = f"Warehouse connection failed: {type(e).__name__}: {e}"
        logger.error(error_msg)
        return [
            {
                "kpi_id": k.id,
                "current_value": None,
                "rag_status": "grey",
                "achievement_pct": None,
                "trend": [],
                "period_over_period_delta": None,
                "period_over_period_pct": None,
                "error": error_msg,
            }
            for k in kpis
        ]

    wtype = org_warehouse.wtype
    results = {}

    def _process(k: KPI):
        errors = []
        m = k.metric
        value, verr = fetch_current_value(wclient, m)
        if verr:
            errors.append(f"Value: {verr}")
        trend, terr = fetch_trend_data(
            wclient, m, wtype,
            time_grain=k.trend_grain,
            trend_periods=k.trend_periods,
        )
        if terr:
            errors.append(f"Trend: {terr}")
        rag_status, achievement_pct = compute_rag_status(
            value, k.target_value, k.amber_threshold_pct, k.green_threshold_pct, k.direction,
        )
        pop_delta, pop_pct = compute_period_over_period(trend)
        return {
            "kpi_id": k.id,
            "current_value": value,
            "rag_status": rag_status,
            "achievement_pct": achievement_pct,
            "trend": trend,
            "period_over_period_delta": pop_delta,
            "period_over_period_pct": pop_pct,
            "error": "; ".join(errors) if errors else None,
        }

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(_process, k): k for k in kpis}
        for fut in as_completed(futures):
            k = futures[fut]
            try:
                results[k.id] = fut.result()
            except Exception as e:
                results[k.id] = {
                    "kpi_id": k.id,
                    "current_value": None,
                    "rag_status": "grey",
                    "achievement_pct": None,
                    "trend": [],
                    "period_over_period_delta": None,
                    "period_over_period_pct": None,
                    "error": f"{type(e).__name__}: {e}",
                }
    return [results[k.id] for k in kpis if k.id in results]


def validate_sql_against_warehouse(
    org_warehouse: OrgWarehouse,
    schema_name: str,
    table_name: str,
    sql_expression: str,
    filters: list,
) -> dict:
    """Dry-run a Calculated-SQL expression — returns {ok, value, error, query_executed}.

    Used by POST /api/metrics/validate-sql/ so a user writing SQL can see that
    it compiles, runs, and returns a numeric scalar before saving the Metric.
    """
    try:
        validate_sql_expression(sql_expression)
        _validate_identifier(schema_name, "schema")
        _validate_identifier(table_name, "table")

        wclient = WarehouseFactory.get_warehouse_client(org_warehouse)
        builder = AggQueryBuilder()
        builder.fetch_from(table_name, schema_name)
        builder.add_column(literal_column(f"({sql_expression})").label("metric_value"))
        for f in filters or []:
            pass  # filters applied below
        _apply_filters(builder, filters or [])
        query = builder.build()
        compiled_sql = str(
            query.compile(bind=wclient.engine, compile_kwargs={"literal_binds": True})
        )
        rows = wclient.execute(compiled_sql)
        if rows:
            row = rows[0] if isinstance(rows[0], dict) else dict(rows[0])
            val = row.get("metric_value")
            if val is None:
                return {
                    "ok": True,
                    "value": None,
                    "error": None,
                    "query_executed": compiled_sql,
                }
            try:
                return {
                    "ok": True,
                    "value": float(val),
                    "error": None,
                    "query_executed": compiled_sql,
                }
            except (TypeError, ValueError):
                return {
                    "ok": False,
                    "value": None,
                    "error": f"Expression did not return a numeric scalar (got {type(val).__name__})",
                    "query_executed": compiled_sql,
                }
        return {
            "ok": False,
            "value": None,
            "error": "Query returned no rows",
            "query_executed": compiled_sql,
        }
    except MetricCompileError as e:
        return {"ok": False, "value": None, "error": str(e), "query_executed": None}
    except Exception as e:
        logger.error(f"validate_sql_against_warehouse failed: {e}\n{traceback.format_exc()}")
        return {
            "ok": False,
            "value": None,
            "error": f"{type(e).__name__}: {e}",
            "query_executed": None,
        }

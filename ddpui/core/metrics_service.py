"""
Service layer for My Metrics feature.
Handles warehouse queries for metric values and trend data.
"""

import traceback
from typing import List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

from sqlalchemy import column, func, text, literal_column, cast
from sqlalchemy.dialects.postgresql import TIMESTAMP as PG_TIMESTAMP

from ddpui.models.org import OrgWarehouse
from ddpui.models.metrics import MetricDefinition
from ddpui.core.datainsights.query_builder import AggQueryBuilder
from ddpui.core.charts.charts_service import apply_time_grain
from ddpui.utils.warehouse.client.warehouse_factory import WarehouseFactory
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui")


def _compile_and_execute(warehouse_client, query):
    """
    Compile a SQLAlchemy Select and execute it — mirrors the pattern
    in charts_service.execute_query() which is known to work.
    """
    compiled = query.compile(
        bind=warehouse_client.engine,
        compile_kwargs={"literal_binds": True},
    )
    logger.info(f"Executing SQL: {compiled}")
    return warehouse_client.execute(compiled)


def compute_rag_status(
    current_value: Optional[float],
    target_value: Optional[float],
    amber_pct: float,
    green_pct: float,
    direction: str,
) -> tuple:
    """
    Returns (rag_status, achievement_pct).
    """
    if target_value is None or target_value == 0:
        return "grey", None

    if current_value is None:
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


def fetch_current_value(
    warehouse_client,
    metric: MetricDefinition,
) -> tuple:
    """
    Run a single aggregation query to get the current metric value.
    Returns (value, error_string).
    """
    try:
        builder = AggQueryBuilder()
        builder.fetch_from(metric.table_name, metric.schema_name)
        builder.add_aggregate_column(metric.column, metric.aggregation, alias="metric_value")
        query = builder.build()

        logger.info(
            f"Metric {metric.id} ({metric.name}): "
            f"running {metric.aggregation}({metric.column}) "
            f"on {metric.schema_name}.{metric.table_name}"
        )

        rows = _compile_and_execute(warehouse_client, query)

        if rows and len(rows) > 0:
            row = rows[0]
            val = row.get("metric_value") if isinstance(row, dict) else None
            if val is not None:
                result = float(val)
                logger.info(f"Metric {metric.id}: value = {result}")
                return result, None
            # If the alias key isn't found, log all available keys
            keys = list(row.keys()) if isinstance(row, dict) else dir(row)
            logger.warning(
                f"Metric {metric.id}: 'metric_value' not in row keys: {keys}. "
                f"Row content: {row}"
            )
            return None, f"Alias 'metric_value' not in result keys: {keys}"

        return None, "Query returned no rows"

    except Exception as e:
        error_msg = f"{type(e).__name__}: {str(e)}"
        logger.error(
            f"Error fetching current value for metric {metric.id} "
            f"({metric.name}): {error_msg}\n{traceback.format_exc()}"
        )
        return None, error_msg


def fetch_trend_data(
    warehouse_client,
    metric: MetricDefinition,
    warehouse_type: str,
) -> tuple:
    """
    Fetch time-grouped aggregation for trend sparkline.
    Returns (list_of_trend_points, error_string).
    """
    if not metric.time_column:
        return [], None

    try:
        time_col = column(metric.time_column)
        # Cast to timestamp for Postgres so date_trunc works on string/varchar date columns
        if warehouse_type.lower() in ["postgres", "postgresql"]:
            time_col = cast(time_col, PG_TIMESTAMP)
        time_expr = apply_time_grain(time_col, metric.time_grain, warehouse_type)

        builder = AggQueryBuilder()
        builder.fetch_from(metric.table_name, metric.schema_name)
        builder.add_column(time_expr.label("period"))
        builder.add_aggregate_column(metric.column, metric.aggregation, alias="metric_value")
        builder.group_cols_by(time_expr)
        # Use literal_column so SQLAlchemy resolves the SELECT alias
        builder.order_by_clauses.append(literal_column("period").asc())
        builder.limit_rows(metric.trend_periods)

        query = builder.build()

        logger.info(
            f"Metric {metric.id} ({metric.name}): "
            f"fetching trend ({metric.time_grain} over {metric.trend_periods} periods)"
        )

        rows = _compile_and_execute(warehouse_client, query)

        if rows and len(rows) > 0:
            # Log the first row's keys for debugging
            first_row = rows[0]
            keys = list(first_row.keys()) if isinstance(first_row, dict) else []
            logger.info(f"Metric {metric.id}: trend row keys = {keys}")

        trend = []
        for row in rows:
            row_dict = row if isinstance(row, dict) else dict(row)
            period_val = row_dict.get("period")

            # Format period as string
            if period_val is not None:
                if metric.time_grain == "month":
                    try:
                        period_str = period_val.strftime("%Y-%m")
                    except AttributeError:
                        period_str = str(period_val)[:7]
                elif metric.time_grain == "quarter":
                    try:
                        q = (period_val.month - 1) // 3 + 1
                        period_str = f"{period_val.year}-Q{q}"
                    except AttributeError:
                        period_str = str(period_val)
                elif metric.time_grain == "year":
                    try:
                        period_str = str(period_val.year)
                    except AttributeError:
                        period_str = str(period_val)[:4]
                else:
                    period_str = str(period_val)
            else:
                period_str = "Unknown"

            val = row_dict.get("metric_value")
            trend.append(
                {
                    "period": period_str,
                    "value": float(val) if val is not None else None,
                }
            )

        logger.info(f"Metric {metric.id}: got {len(trend)} trend points")
        return trend, None

    except Exception as e:
        error_msg = f"{type(e).__name__}: {str(e)}"
        logger.error(
            f"Error fetching trend for metric {metric.id} "
            f"({metric.name}): {error_msg}\n{traceback.format_exc()}"
        )
        return [], error_msg


def fetch_metrics_data(
    org_warehouse: OrgWarehouse,
    metrics: List[MetricDefinition],
) -> list:
    """
    Fetch current value + trend for a list of metrics.
    Runs queries in parallel using ThreadPoolExecutor.
    Returns list of MetricDataPoint dicts.
    """
    if not metrics:
        return []

    try:
        warehouse_client = WarehouseFactory.get_warehouse_client(org_warehouse)
    except Exception as e:
        error_msg = f"Warehouse connection failed: {type(e).__name__}: {str(e)}"
        logger.error(error_msg)
        return [
            {
                "metric_id": m.id,
                "current_value": None,
                "rag_status": "grey",
                "achievement_pct": None,
                "trend": [],
                "error": error_msg,
            }
            for m in metrics
        ]

    warehouse_type = org_warehouse.wtype
    results = {}

    def _process_metric(metric):
        errors = []

        current_value, val_error = fetch_current_value(warehouse_client, metric)
        if val_error:
            errors.append(f"Value: {val_error}")

        trend, trend_error = fetch_trend_data(warehouse_client, metric, warehouse_type)
        if trend_error:
            errors.append(f"Trend: {trend_error}")

        rag_status, achievement_pct = compute_rag_status(
            current_value,
            metric.target_value,
            metric.amber_threshold_pct,
            metric.green_threshold_pct,
            metric.direction,
        )
        return {
            "metric_id": metric.id,
            "current_value": current_value,
            "rag_status": rag_status,
            "achievement_pct": achievement_pct,
            "trend": trend,
            "error": "; ".join(errors) if errors else None,
        }

    # Parallelize warehouse queries (max 5 concurrent to be kind to the warehouse)
    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_metric = {executor.submit(_process_metric, m): m for m in metrics}
        for future in as_completed(future_to_metric):
            metric = future_to_metric[future]
            try:
                results[metric.id] = future.result()
            except Exception as e:
                error_msg = f"{type(e).__name__}: {str(e)}"
                logger.error(f"Error processing metric {metric.id}: {error_msg}")
                results[metric.id] = {
                    "metric_id": metric.id,
                    "current_value": None,
                    "rag_status": "grey",
                    "achievement_pct": None,
                    "trend": [],
                    "error": error_msg,
                }

    # Return in the order the metrics were requested
    return [results[m.id] for m in metrics if m.id in results]

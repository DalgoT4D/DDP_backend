"""
Service layer for My Metrics feature.
Handles warehouse queries for metric values and trend data.
"""

from typing import List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

from sqlalchemy import column, func, text

from ddpui.models.org import OrgWarehouse
from ddpui.models.metrics import MetricDefinition
from ddpui.core.datainsights.query_builder import AggQueryBuilder
from ddpui.core.charts.charts_service import apply_time_grain
from ddpui.utils.warehouse.client.warehouse_factory import WarehouseFactory
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui")


def _get_agg_func(agg_name: str):
    """Map aggregation name to sqlalchemy func"""
    mapping = {
        "sum": func.sum,
        "avg": func.avg,
        "count": func.count,
        "min": func.min,
        "max": func.max,
        "count_distinct": lambda c: func.count(func.distinct(c)),
    }
    return mapping.get(agg_name.lower())


def compute_rag_status(
    current_value: Optional[float],
    target_value: Optional[float],
    amber_pct: float,
    green_pct: float,
) -> tuple:
    """
    Returns (rag_status, achievement_pct).
    """
    if target_value is None or target_value == 0:
        return "grey", None

    if current_value is None:
        return "grey", None

    achievement_pct = round((current_value / target_value) * 100, 1)

    if achievement_pct >= green_pct:
        return "green", achievement_pct
    elif achievement_pct >= amber_pct:
        return "amber", achievement_pct
    else:
        return "red", achievement_pct


def fetch_current_value(
    warehouse_client,
    metric: MetricDefinition,
) -> Optional[float]:
    """Run a single aggregation query to get the current metric value."""
    try:
        builder = AggQueryBuilder()
        builder.fetch_from(metric.table_name, metric.schema_name)
        builder.add_aggregate_column(
            metric.column, metric.aggregation, alias="metric_value"
        )
        query = builder.build()
        rows = warehouse_client.execute(query)

        if rows and len(rows) > 0:
            val = rows[0].get("metric_value")
            return float(val) if val is not None else None
        return None
    except Exception as e:
        logger.error(f"Error fetching current value for metric {metric.id}: {e}")
        return None


def fetch_trend_data(
    warehouse_client,
    metric: MetricDefinition,
    warehouse_type: str,
) -> list:
    """
    Fetch time-grouped aggregation for trend sparkline.
    Returns list of {period, value} dicts.
    """
    if not metric.time_column:
        return []

    try:
        time_col = column(metric.time_column)
        time_expr = apply_time_grain(time_col, metric.time_grain, warehouse_type)

        builder = AggQueryBuilder()
        builder.fetch_from(metric.table_name, metric.schema_name)
        builder.add_column(time_expr.label("period"))
        builder.add_aggregate_column(
            metric.column, metric.aggregation, alias="metric_value"
        )
        builder.group_cols_by(time_expr)
        builder.order_cols_by([("period", "asc")])
        builder.limit_rows(metric.trend_periods)

        query = builder.build()
        rows = warehouse_client.execute(query)

        trend = []
        for row in rows:
            period_val = row.get("period")
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

            val = row.get("metric_value")
            trend.append(
                {
                    "period": period_str,
                    "value": float(val) if val is not None else None,
                }
            )

        return trend

    except Exception as e:
        logger.error(f"Error fetching trend for metric {metric.id}: {e}")
        return []


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

    warehouse_client = WarehouseFactory.get_warehouse_client(org_warehouse)
    warehouse_type = org_warehouse.wtype

    results = {}

    def _process_metric(metric):
        current_value = fetch_current_value(warehouse_client, metric)
        trend = fetch_trend_data(warehouse_client, metric, warehouse_type)
        rag_status, achievement_pct = compute_rag_status(
            current_value,
            metric.target_value,
            metric.amber_threshold_pct,
            metric.green_threshold_pct,
        )
        return {
            "metric_id": metric.id,
            "current_value": current_value,
            "rag_status": rag_status,
            "achievement_pct": achievement_pct,
            "trend": trend,
        }

    # Parallelize warehouse queries (max 5 concurrent to be kind to the warehouse)
    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_metric = {
            executor.submit(_process_metric, m): m for m in metrics
        }
        for future in as_completed(future_to_metric):
            metric = future_to_metric[future]
            try:
                results[metric.id] = future.result()
            except Exception as e:
                logger.error(f"Error processing metric {metric.id}: {e}")
                results[metric.id] = {
                    "metric_id": metric.id,
                    "current_value": None,
                    "rag_status": "grey",
                    "achievement_pct": None,
                    "trend": [],
                }

    # Return in the order the metrics were requested
    return [results[m.id] for m in metrics if m.id in results]

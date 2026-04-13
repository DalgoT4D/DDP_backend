"""
Pivot table post-processing: classifies ROLLUP rows and rotates into pivoted JSON.

Supports multiple column dimensions. Column keys are tuples like
("Maharashtra", "Education") for a two-column-dimension pivot.
"""
from datetime import datetime
from typing import Any

NULL_DISPLAY_LABEL = "(No value)"
MAX_PIVOT_COLUMNS = 100


def classify_row(row: dict, row_dim_cols: list[str]) -> str:
    """
    Classify a ROLLUP result row using GROUPING() flags.
    Returns: 'data', 'subtotal', or 'grand_total'
    """
    grouping_flags = [row[f"_grp_{col}"] for col in row_dim_cols]

    if all(f == 0 for f in grouping_flags):
        return "data"
    if all(f == 1 for f in grouping_flags):
        return "grand_total"
    return "subtotal"


def is_column_total(row: dict, num_col_dims: int) -> bool:
    """Check if ALL column dimension GROUPING flags are 1 (overall column total)."""
    if num_col_dims == 0:
        return False
    return all(row.get(f"_grp_pivot_col_{i}", 0) == 1 for i in range(num_col_dims))


def get_row_labels(row: dict, row_dim_cols: list[str]) -> list[str]:
    """
    Build display labels for row dimensions.
    Real NULLs → "(No value)". ROLLUP NULLs → stop (subtotal boundary).
    """
    labels = []
    for col in row_dim_cols:
        if row[f"_grp_{col}"] == 1:
            break
        label = row[col] if row[col] is not None else NULL_DISPLAY_LABEL
        labels.append(str(label))
    return labels


def format_pivot_column_header(value: Any, time_grain: str | None) -> str:
    """Format a pivot column header based on time grain."""
    if time_grain is None or value is None:
        return str(value) if value is not None else NULL_DISPLAY_LABEL

    if isinstance(value, str):
        try:
            dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except (ValueError, TypeError):
            return str(value)
    elif isinstance(value, datetime):
        dt = value
    else:
        return str(value)

    formats = {
        "year": "%Y",
        "month": "%b %Y",
        "day": "%b %d, %Y",
        "hour": "%b %d, %Y %H:00",
        "minute": "%b %d, %Y %H:%M",
        "second": "%b %d, %Y %H:%M:%S",
    }
    fmt = formats.get(time_grain)
    return dt.strftime(fmt) if fmt else str(value)


def _get_column_key(
    row: dict,
    num_col_dims: int,
    col_dim_names: list[str],
    time_grains: dict[str, str] | None,
) -> tuple[str, ...]:
    """Extract and format the composite column key from a row."""
    key_parts = []
    for i in range(num_col_dims):
        raw_val = row.get(f"pivot_col_{i}")
        grain = (time_grains or {}).get(col_dim_names[i]) if col_dim_names else None
        formatted = format_pivot_column_header(raw_val, grain)
        key_parts.append(formatted)
    return tuple(key_parts)


def _is_leaf_column_row(row: dict, num_col_dims: int) -> bool:
    """Check if all column dimension GROUPING flags are 0 (a leaf-level cell)."""
    return all(row.get(f"_grp_pivot_col_{i}", 0) == 0 for i in range(num_col_dims))


def rotate_to_pivot(
    flat_rows: list[dict],
    row_dim_cols: list[str],
    num_col_dims: int,
    col_dim_names: list[str],
    metric_aliases: list[str],
    time_grains: dict[str, str] | None = None,
    page: int = 1,
    page_size: int = 50,
    metric_display_names: list[str] | None = None,
) -> dict:
    """
    Transform flat ROLLUP rows into pivoted JSON response.

    Supports multiple column dimensions via composite column keys (tuples).

    Returns:
        {
            "column_keys": [["Maharashtra", "Education"], ...],
            "column_dimension_names": ["state_name", "program"],
            "metric_headers": ["Count", "Spend"],
            "rows": [...],
            "grand_total": {...} | None,
            "total_row_groups": int,
            "page": int,
            "page_size": int,
        }
    """
    has_col_dims = num_col_dims > 0

    # Collect unique leaf-level column keys (sorted tuples)
    column_keys: list[tuple[str, ...]] = []
    if has_col_dims:
        raw_keys = set()
        for row in flat_rows:
            if _is_leaf_column_row(row, num_col_dims) and not is_column_total(row, num_col_dims):
                key = _get_column_key(row, num_col_dims, col_dim_names, time_grains)
                # Skip keys with None values (shouldn't happen for leaf rows but be safe)
                if NULL_DISPLAY_LABEL not in key or all(
                    row.get(f"_grp_pivot_col_{i}", 0) == 0 for i in range(num_col_dims)
                ):
                    raw_keys.add(key)
        column_keys = sorted(raw_keys)

    # Build pivoted rows keyed by (row_labels_tuple, row_type)
    pivoted: dict[tuple, dict] = {}
    row_order: list[tuple] = []

    for row in flat_rows:
        row_type = classify_row(row, row_dim_cols)
        row_labels = tuple(get_row_labels(row, row_dim_cols))
        col_total = is_column_total(row, num_col_dims) if has_col_dims else False

        key = (row_labels, row_type)
        if key not in pivoted:
            pivoted[key] = {
                "row_labels": list(row_labels),
                "is_subtotal": row_type == "subtotal",
                "values": [[None] * len(metric_aliases) for _ in column_keys],
                "row_total": [None] * len(metric_aliases),
            }
            row_order.append(key)

        metric_values = [row.get(m) for m in metric_aliases]

        if not has_col_dims or col_total:
            # No column dimensions or this is the overall total column
            pivoted[key]["row_total"] = metric_values
        elif _is_leaf_column_row(row, num_col_dims):
            # Leaf-level column cell
            col_key = _get_column_key(row, num_col_dims, col_dim_names, time_grains)
            if col_key in column_keys:
                col_idx = column_keys.index(col_key)
                pivoted[key]["values"][col_idx] = metric_values
        # else: column subtotal row — skip for now (not rendered)

    # Separate grand total from data/subtotal rows
    grand_total_entry = None
    data_rows = []
    for key in row_order:
        _labels, rtype = key
        entry = pivoted[key]
        if rtype == "grand_total":
            grand_total_entry = {
                "values": entry["values"],
                "row_total": entry["row_total"],
            }
        else:
            data_rows.append(entry)

    # Count top-level groups (unique first label among non-subtotal rows)
    top_level_groups = set()
    for entry in data_rows:
        if not entry["is_subtotal"] and entry["row_labels"]:
            top_level_groups.add(entry["row_labels"][0])

    return {
        "column_keys": [list(k) for k in column_keys],
        "column_dimension_names": col_dim_names,
        "metric_headers": metric_display_names if metric_display_names else metric_aliases,
        "rows": data_rows,
        "grand_total": grand_total_entry,
        "total_row_groups": len(top_level_groups),
        "page": page,
        "page_size": page_size,
    }

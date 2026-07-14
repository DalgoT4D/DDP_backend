"""
Pivot table post-processing: flattens ROLLUP output into a list of self-describing cells.

Each ROLLUP row becomes one cell tagged with (row_kind, col_kind). The tags name
which of the 3x3 grid regions the cell belongs to; the frontend derives the axes,
spans, and layout from the cells. Nothing here pre-shapes a grid.

Column keys are lists like ["Maharashtra", "Education"] for a two-column-dimension
pivot. Cell order mirrors the input (SQL ORDER BY); this module never re-sorts.
"""
NULL_DISPLAY_LABEL = "(No value)"


def _classify_row_kind(row: dict, row_dim_cols: list[str]) -> str:
    """Classify a row by its row-dimension GROUPING() flags.

    All flags 0 -> 'data'; all 1 -> 'grand_total'; mixed -> 'row_subtotal'.
    """
    flags = [row[f"_grp_{col}"] for col in row_dim_cols]
    if all(f == 0 for f in flags):
        return "data"
    if all(f == 1 for f in flags):
        return "grand_total"
    return "row_subtotal"


def _classify_col_kind(row: dict, num_col_dims: int) -> str:
    """Classify a row by its column-dimension GROUPING() flags.

    No column dims -> 'row_total' (the single value column). Otherwise:
    all flags 0 -> 'leaf'; all 1 -> 'row_total'; mixed -> 'col_subtotal'.
    """
    if num_col_dims == 0:
        return "row_total"
    flags = [row.get(f"_grp_pivot_col_{i}", 0) for i in range(num_col_dims)]
    if all(f == 1 for f in flags):
        return "row_total"
    if all(f == 0 for f in flags):
        return "leaf"
    return "col_subtotal"


def _row_key(row: dict, row_dim_cols: list[str]) -> list[str]:
    """Real (non-rolled-up) row-dimension values, stringified. Stops at the first
    rolled-up dim (the subtotal/grand-total boundary). Real NULLs -> "(No value)"."""
    parts = []
    for col in row_dim_cols:
        if row[f"_grp_{col}"] == 1:
            break
        val = row[col]
        parts.append(str(val) if val is not None else NULL_DISPLAY_LABEL)
    return parts


def _col_key(row: dict, num_col_dims: int) -> list[str]:
    """Real (non-rolled-up) column-dimension values, stringified. Stops at the first
    rolled-up dim. Real NULLs -> "(No value)". Empty for a row_total cell."""
    parts = []
    for i in range(num_col_dims):
        if row.get(f"_grp_pivot_col_{i}", 0) == 1:
            break
        val = row.get(f"pivot_col_{i}")
        parts.append(str(val) if val is not None else NULL_DISPLAY_LABEL)
    return parts


def rotate_to_pivot(
    flat_rows: list[dict],
    row_dim_cols: list[str],
    num_col_dims: int,
    col_dim_names: list[str],
    metric_aliases: list[str],
    metric_display_names: list[str] | None = None,
    show_column_subtotals: bool = False,
    show_row_subtotals: bool = True,
) -> dict:
    """
    Flatten ROLLUP rows into a cells[] response.

    Each input row -> one cell:
        {
            "row_key":  ["Mumbai", "Education"],   # real row-dim values ([] = grand total)
            "col_key":  ["2026-01", "Health"],     # real col-dim values ([] = row total)
            "row_kind": "data" | "row_subtotal" | "grand_total",
            "col_kind": "leaf" | "col_subtotal" | "row_total",
            "values":   [<metric>, ...],
        }

    Subtotal cells are dropped unless their toggle is on. Cell order is preserved
    from the input, so chronological ordering rides on the SQL ORDER BY.

    Returns:
        {
            "row_dimension_names": ["district", "program"],
            "column_dimension_names": ["month", "program"],
            "metric_headers": ["Count", "Spend"],
            "cells": [...],
        }
    """
    cells = []
    for row in flat_rows:
        row_kind = _classify_row_kind(row, row_dim_cols)
        col_kind = _classify_col_kind(row, num_col_dims)

        # ROLLUP always emits intermediate subtotal rows; drop them when the
        # payload didn't ask for that axis of subtotals.
        if row_kind == "row_subtotal" and not show_row_subtotals:
            continue
        if col_kind == "col_subtotal" and not show_column_subtotals:
            continue

        cells.append(
            {
                "row_key": _row_key(row, row_dim_cols),
                "col_key": _col_key(row, num_col_dims),
                "row_kind": row_kind,
                "col_kind": col_kind,
                "values": [row.get(m) for m in metric_aliases],
            }
        )

    return {
        "row_dimension_names": row_dim_cols,
        "column_dimension_names": col_dim_names,
        "metric_headers": metric_display_names if metric_display_names else metric_aliases,
        "cells": cells,
    }

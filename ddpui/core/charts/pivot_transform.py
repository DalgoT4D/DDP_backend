"""
Pivot table post-processing: flattens ROLLUP output into a list of self-describing cells.

Each ROLLUP row becomes one cell tagged with (row_kind, col_kind). The tags name
which of the 3x3 grid regions the cell belongs to; the frontend derives the row
axis, spans, and layout from the cells. Nothing here pre-shapes a grid.

The one thing the frontend cannot derive is column order: ROLLUP output is
row-major and sparse, so the response also carries a canonical, globally-sorted
column axis (column_keys / column_subtotal_keys) sorted by raw value here — where
the native types (datetime for time grains) are still available.

Column keys are lists like ["Maharashtra", "Education"] for a two-column-dimension
pivot.
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


def _raw_col_key(row: dict, num_col_dims: int) -> tuple:
    """Unformatted (native-typed) real column-dimension values, up to the first
    rolled-up dim. Used only for sorting the column axis so time-grained headers
    order chronologically (native datetime) rather than lexically."""
    parts = []
    for i in range(num_col_dims):
        if row.get(f"_grp_pivot_col_{i}", 0) == 1:
            break
        parts.append(row.get(f"pivot_col_{i}"))
    return tuple(parts)


def _raw_sort_key(raw_key: tuple) -> tuple:
    """Sort key ordering NULLs last and comparing raw values by their native type."""
    return tuple((v is None, v) for v in raw_key)


def _ordered_column_axis(
    flat_rows: list[dict], num_col_dims: int, col_kind: str
) -> list[list[str]]:
    """The canonical, globally-sorted list of unique column keys of one col_kind.

    ROLLUP output is row-major and may be sparse, so the column order cannot be
    read off cell arrival order — we collect every distinct key and sort by its
    raw value. `formatted_by_raw` maps raw tuple -> display key; we sort on raw,
    emit formatted.
    """
    if num_col_dims == 0:
        return []
    formatted_by_raw: dict[tuple, list[str]] = {}
    for row in flat_rows:
        if _classify_col_kind(row, num_col_dims) == col_kind:
            formatted_by_raw[_raw_col_key(row, num_col_dims)] = _col_key(row, num_col_dims)
    return [formatted_by_raw[r] for r in sorted(formatted_by_raw, key=_raw_sort_key)]


def rotate_to_pivot(
    flat_rows: list[dict],
    row_dim_cols: list[str],
    num_col_dims: int,
    col_dim_names: list[str],
    metric_aliases: list[str],
    metric_display_names: list[str] | None = None,
    show_column_subtotals: bool = False,
    show_row_subtotals: bool = True,
    show_row_grand_total: bool = True,
    show_column_grand_total: bool = True,
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

    Subtotal and grand-total cells are dropped unless their toggle is on:
    show_row_subtotals / show_column_subtotals gate the subtotal axes,
    show_column_grand_total gates the bottom "Total" row, and show_row_grand_total
    gates the rightmost "Total" column (ignored when there are no column dims, since
    row_total then holds the primary data). Row order is preserved from the input;
    the column axis (column_keys / column_subtotal_keys) is the canonical sorted
    order the frontend must render columns in.

    Returns:
        {
            "row_dimension_names": ["district", "program"],
            "column_dimension_names": ["month", "program"],
            "metric_headers": ["Count", "Spend"],
            "column_keys": [["2026-01", "Education"], ...],   # sorted leaf axis
            "column_subtotal_keys": [["2026-01"], ...],       # [] unless enabled
            "cells": [...],
        }
    """
    has_col_dims = num_col_dims > 0
    cells = []
    for row in flat_rows:
        row_kind = _classify_row_kind(row, row_dim_cols)
        col_kind = _classify_col_kind(row, num_col_dims)

        # ROLLUP always emits the intermediate subtotal / grand-total rows; drop the
        # ones the payload didn't ask for.
        if row_kind == "row_subtotal" and not show_row_subtotals:
            continue
        if col_kind == "col_subtotal" and not show_column_subtotals:
            continue
        # Bottom "Total" row (each column across rows).
        if row_kind == "grand_total" and not show_column_grand_total:
            continue
        # Rightmost "Total" column (each row across cols) — only a grand total when
        # column dims exist; without them row_total holds the primary data.
        if col_kind == "row_total" and has_col_dims and not show_row_grand_total:
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

    # The canonical column axes are sorted here (raw value) because ROLLUP output is
    # row-major and sparse — the frontend cannot recover column order from the cells.
    column_subtotal_keys = (
        _ordered_column_axis(flat_rows, num_col_dims, "col_subtotal")
        if show_column_subtotals
        else []
    )

    return {
        "row_dimension_names": row_dim_cols,
        "column_dimension_names": col_dim_names,
        "metric_headers": metric_display_names if metric_display_names else metric_aliases,
        "column_keys": _ordered_column_axis(flat_rows, num_col_dims, "leaf"),
        "column_subtotal_keys": column_subtotal_keys,
        "cells": cells,
    }

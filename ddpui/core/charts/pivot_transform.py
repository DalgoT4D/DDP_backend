"""
Pivot table post-processing: classifies ROLLUP rows and rotates into pivoted JSON.

Supports multiple column dimensions. Column keys are tuples like
("Maharashtra", "Education") for a two-column-dimension pivot.
"""
NULL_DISPLAY_LABEL = "(No value)"


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


def is_column_subtotal(row: dict, num_col_dims: int) -> bool:
    """Check if this is a column subtotal (partial column grouping).

    A column subtotal has at least one real column dim value (_grp=0)
    and at least one rolled-up value (_grp=1). Only meaningful with 2+ column dims.
    """
    if num_col_dims <= 1:
        return False
    flags = [row.get(f"_grp_pivot_col_{i}", 0) for i in range(num_col_dims)]
    return any(f == 0 for f in flags) and any(f == 1 for f in flags)


def _get_column_subtotal_key(
    row: dict,
    num_col_dims: int,
) -> tuple[str, ...]:
    """Extract the parent column key for a column subtotal row.

    Includes only the non-rolled-up (real) dimension values.
    """
    key_parts = []
    for i in range(num_col_dims):
        if row.get(f"_grp_pivot_col_{i}", 0) == 1:
            break
        raw_val = row.get(f"pivot_col_{i}")
        key_parts.append(str(raw_val) if raw_val is not None else NULL_DISPLAY_LABEL)
    return tuple(key_parts)


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


def _get_column_key(row: dict, num_col_dims: int) -> tuple[str, ...]:
    """Extract the composite column key from a row."""
    key_parts = []
    for i in range(num_col_dims):
        raw_val = row.get(f"pivot_col_{i}")
        key_parts.append(str(raw_val) if raw_val is not None else NULL_DISPLAY_LABEL)
    return tuple(key_parts)


def _get_raw_column_key(row: dict, num_col_dims: int) -> tuple:
    """Extract the unformatted composite column key for chronological ordering."""
    return tuple(row.get(f"pivot_col_{i}") for i in range(num_col_dims))


def _get_raw_column_subtotal_key(row: dict, num_col_dims: int) -> tuple:
    """Extract the unformatted parent column key for a column subtotal row."""
    parts = []
    for i in range(num_col_dims):
        if row.get(f"_grp_pivot_col_{i}", 0) == 1:
            break
        parts.append(row.get(f"pivot_col_{i}"))
    return tuple(parts)


def _raw_sort_key(raw_key: tuple) -> tuple:
    """Sort key that orders NULLs last and compares raw values directly.

    Raw values keep their native type (datetime for time grains, str/num
    otherwise), so time-grained headers sort chronologically, not lexically.
    """
    return tuple((v is None, v) for v in raw_key)


def _is_leaf_column_row(row: dict, num_col_dims: int) -> bool:
    """Check if all column dimension GROUPING flags are 0 (a leaf-level cell)."""
    return all(row.get(f"_grp_pivot_col_{i}", 0) == 0 for i in range(num_col_dims))


def _collect_column_keys(
    flat_rows: list[dict],
    num_col_dims: int,
    show_column_subtotals: bool,
) -> tuple[list[tuple[str, ...]], list[tuple[str, ...]]]:
    """Collect the unique leaf column keys (and subtotal keys) that form the pivot's headers.

    Keys are ordered by their RAW values so time-grained headers sort
    chronologically instead of lexicographically. `formatted_by_raw` maps
    raw tuple → display tuple; we sort on raw, emit formatted.

    Returns (column_keys, column_subtotal_keys).
    """
    if num_col_dims == 0:
        return [], []

    formatted_by_raw: dict[tuple, tuple[str, ...]] = {}
    formatted_by_raw_subtotal: dict[tuple, tuple[str, ...]] = {}
    for row in flat_rows:
        if _is_leaf_column_row(row, num_col_dims) and not is_column_total(row, num_col_dims):
            key = _get_column_key(row, num_col_dims)
            # Skip keys with None values (shouldn't happen for leaf rows but be safe)
            if NULL_DISPLAY_LABEL not in key or all(
                row.get(f"_grp_pivot_col_{i}", 0) == 0 for i in range(num_col_dims)
            ):
                formatted_by_raw[_get_raw_column_key(row, num_col_dims)] = key
        elif show_column_subtotals and is_column_subtotal(row, num_col_dims):
            sub_key = _get_column_subtotal_key(row, num_col_dims)
            formatted_by_raw_subtotal[_get_raw_column_subtotal_key(row, num_col_dims)] = sub_key

    column_keys = [formatted_by_raw[r] for r in sorted(formatted_by_raw, key=_raw_sort_key)]
    column_subtotal_keys = [
        formatted_by_raw_subtotal[r] for r in sorted(formatted_by_raw_subtotal, key=_raw_sort_key)
    ]
    return column_keys, column_subtotal_keys


def _compute_subtotal_insert_positions(
    column_keys: list[tuple[str, ...]],
    column_subtotal_keys: list[tuple[str, ...]],
) -> list[int]:
    """Map each column subtotal key to the leaf column index it should appear after.

    A subtotal key like ("CA",) should appear after the last leaf key starting with "CA".
    """
    insert_after: list[int] = []
    for sub_key in column_subtotal_keys:
        last_idx = -1
        for idx, leaf_key in enumerate(column_keys):
            if leaf_key[: len(sub_key)] == sub_key:
                last_idx = idx
        insert_after.append(last_idx)
    return insert_after


def _build_pivoted_rows(
    flat_rows: list[dict],
    row_dim_cols: list[str],
    num_col_dims: int,
    column_keys: list[tuple[str, ...]],
    column_subtotal_keys: list[tuple[str, ...]],
    metric_aliases: list[str],
    show_column_subtotals: bool,
    show_row_subtotals: bool,
) -> tuple[dict[tuple, dict], list[tuple]]:
    """Scatter each flat row's metric values into pivoted cells.

    Rows are keyed by (row_labels_tuple, row_type). Each flat row lands in one
    of three slots: the overall row_total column, a leaf column cell, or a
    column subtotal cell.

    Returns (pivoted, row_order) — the entry dict keyed by row key, and the
    insertion order of those keys.
    """
    has_col_dims = num_col_dims > 0
    pivoted: dict[tuple, dict] = {}
    row_order: list[tuple] = []

    for row in flat_rows:
        row_type = classify_row(row, row_dim_cols)
        # ROLLUP always emits intermediate subtotal rows; drop them when the
        # payload only asked for a grand total (show_row_subtotals=False).
        if row_type == "subtotal" and not show_row_subtotals:
            continue
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
            if show_column_subtotals and column_subtotal_keys:
                pivoted[key]["column_subtotal_values"] = [
                    [None] * len(metric_aliases) for _ in column_subtotal_keys
                ]
            row_order.append(key)

        metric_values = [row.get(m) for m in metric_aliases]

        if not has_col_dims or col_total:
            # No column dimensions or this is the overall total column
            pivoted[key]["row_total"] = metric_values
        elif _is_leaf_column_row(row, num_col_dims):
            # Leaf-level column cell
            col_key = _get_column_key(row, num_col_dims)
            if col_key in column_keys:
                col_idx = column_keys.index(col_key)
                pivoted[key]["values"][col_idx] = metric_values
        elif show_column_subtotals and is_column_subtotal(row, num_col_dims):
            # Column subtotal row
            sub_key = _get_column_subtotal_key(row, num_col_dims)
            if sub_key in column_subtotal_keys:
                sub_idx = column_subtotal_keys.index(sub_key)
                pivoted[key]["column_subtotal_values"][sub_idx] = metric_values

    return pivoted, row_order


def _split_grand_total(
    pivoted: dict[tuple, dict],
    row_order: list[tuple],
    show_column_subtotals: bool,
) -> tuple[list[dict], dict | None]:
    """Separate the grand-total entry from the data/subtotal rows.

    Returns (data_rows, grand_total_entry).
    """
    grand_total_entry = None
    data_rows = []
    for key in row_order:
        _labels, rtype = key
        entry = pivoted[key]
        if rtype == "grand_total":
            gt = {
                "values": entry["values"],
                "row_total": entry["row_total"],
            }
            if show_column_subtotals and "column_subtotal_values" in entry:
                gt["column_subtotal_values"] = entry["column_subtotal_values"]
            grand_total_entry = gt
        else:
            data_rows.append(entry)
    return data_rows, grand_total_entry


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
    Transform flat ROLLUP rows into pivoted JSON response.

    Supports multiple column dimensions via composite column keys (tuples).

    Pipeline:
      1. _collect_column_keys — derive the pivot's column headers
      2. _compute_subtotal_insert_positions — where column subtotals slot in
      3. _build_pivoted_rows — scatter metric values into cells
      4. _split_grand_total — peel the grand-total row off the data rows

    Returns:
        {
            "column_keys": [["Maharashtra", "Education"], ...],
            "column_dimension_names": ["state_name", "program"],
            "metric_headers": ["Count", "Spend"],
            "rows": [...],
            "grand_total": {...} | None,
        }
    """
    column_keys, column_subtotal_keys = _collect_column_keys(
        flat_rows, num_col_dims, show_column_subtotals
    )
    column_subtotal_insert_after = _compute_subtotal_insert_positions(
        column_keys, column_subtotal_keys
    )
    pivoted, row_order = _build_pivoted_rows(
        flat_rows,
        row_dim_cols,
        num_col_dims,
        column_keys,
        column_subtotal_keys,
        metric_aliases,
        show_column_subtotals,
        show_row_subtotals,
    )
    data_rows, grand_total_entry = _split_grand_total(pivoted, row_order, show_column_subtotals)

    result = {
        "column_keys": [list(k) for k in column_keys],
        "column_dimension_names": col_dim_names,
        "metric_headers": metric_display_names if metric_display_names else metric_aliases,
        "rows": data_rows,
        "grand_total": grand_total_entry,
    }

    if show_column_subtotals and column_subtotal_keys:
        result["column_subtotals"] = {
            "keys": [list(k) for k in column_subtotal_keys],
            "insert_after": column_subtotal_insert_after,
        }

    return result

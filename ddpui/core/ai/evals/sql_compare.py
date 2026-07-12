"""Execution-based result-set comparison — the eval_sql_correct hard metric.

Two queries are "the same answer" when their result sets match as multisets:
row order doesn't matter, column names don't matter (the agent may alias
differently than the gold SQL), and numbers compare with tolerance (SUM vs
SUM::numeric etc.). Column ORDER inside a row still matters — comparing
{state, count} to {count, state} positionally is the pragmatic middle ground
between strictness and alias-noise.
"""

from decimal import Decimal

NUMERIC_TOLERANCE = 1e-6


def _normalize_cell(value):
    """Numbers become floats (so 700, 700.0, Decimal('700') and '700' all
    agree); everything else compares as a stripped string."""
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(value, (int, float, Decimal)):
        return float(value)
    text = str(value).strip() if value is not None else ""
    try:
        return float(text)
    except ValueError:
        return text.lower()


def _cells_equal(a, b) -> bool:
    if isinstance(a, float) and isinstance(b, float):
        return abs(a - b) <= NUMERIC_TOLERANCE * max(1.0, abs(a), abs(b))
    if isinstance(a, str) and isinstance(b, str) and a != b:
        # labels are presentation: gold 'q3' matches agent 'q3 (oct-dec 2025)'
        shorter, longer = sorted((a, b), key=len)
        return len(shorter) >= 2 and shorter in longer
    return a == b


def _rows_equal(row_a: tuple, row_b: tuple) -> bool:
    return len(row_a) == len(row_b) and all(_cells_equal(a, b) for a, b in zip(row_a, row_b))


def result_sets_match(expected_rows: list, actual_rows: list) -> bool:
    """Order-insensitive, alias-agnostic, numerically tolerant comparison.

    Rows may be dicts (warehouse output) or lists (the UI result table shape);
    values are compared positionally within each row."""
    normalize = lambda rows: [  # noqa: E731
        tuple(_normalize_cell(v) for v in (row.values() if isinstance(row, dict) else row))
        for row in rows
    ]
    remaining = normalize(actual_rows)
    expected = normalize(expected_rows)
    if len(expected) != len(remaining):
        return False
    for exp_row in expected:
        match = next((i for i, act in enumerate(remaining) if _rows_equal(exp_row, act)), None)
        if match is None:
            return False
        remaining.pop(match)
    return True


def gold_satisfied(gold_rows: list, agent_rows: list, answer: str) -> bool:
    """Does the agent's result answer what the gold SQL answers?

    Agents legitimately ENRICH results (extra context columns, e.g. total +
    distinct + active in one row), so strict equality over-fails. Rules:

    - Scalar gold (1 row × 1 column): the value must be supported by the
      result (a matching cell, OR the row count — "7 states" answered with a
      7-row breakdown) AND appear in the narrated answer, which carries the
      assertion.
    - Single-row gold (e.g. LIMIT 1 top/bottom questions): the gold row must
      appear among the agent's rows (agents often show the full ranking and
      narrate the winner), and the answer must name the gold row's text cells.
    - Otherwise: some selection of the agent's columns, projected across all
      its rows, must equal the gold rows as a multiset (extra columns are
      fine; extra or missing ROWS are not).
    """
    from itertools import permutations

    normalize = lambda rows: [  # noqa: E731
        tuple(_normalize_cell(v) for v in (row.values() if isinstance(row, dict) else row))
        for row in rows
    ]
    gold = normalize(gold_rows)
    agent = normalize(agent_rows)
    if not gold or not agent:
        return False

    if len(gold) == 1 and len(gold[0]) == 1:
        value = gold[0][0]
        in_result = any(_cells_equal(value, cell) for row in agent for cell in row)
        as_row_count = isinstance(value, float) and value == float(len(agent))
        return (in_result or as_row_count) and answer_contains_value(answer, _render(value))

    gold_width = len(gold[0])
    agent_width = len(agent[0])
    if gold_width > agent_width:
        return False

    if len(gold) == 1:
        row_found = any(
            any(
                all(_cells_equal(g, agent_row[i]) for g, i in zip(gold[0], col_pick))
                for col_pick in permutations(range(agent_width), gold_width)
            )
            for agent_row in agent
        )
        entities = [_render(c) for c in gold[0] if isinstance(c, str)]
        return row_found and all(answer_contains_value(answer, e) for e in entities)

    if len(gold) != len(agent):
        return False
    for col_pick in permutations(range(agent_width), gold_width):
        projected = [tuple(row[i] for i in col_pick) for row in agent]
        if result_sets_match(gold, projected):
            return True
    return False


def _render(value) -> str:
    """A normalized cell back to text for the answer-contains check
    (171.0 → "171" so it matches '**171** beneficiaries')."""
    if isinstance(value, float) and value == int(value):
        return str(int(value))
    return str(value)


def answer_contains_value(answer: str, expected_value: str) -> bool:
    """Fallback metric when gold SQL is overkill: the expected value (e.g.
    "1,284" or "Pune") must appear in the answer text, ignoring thousands
    separators and case."""
    strip = lambda s: s.replace(",", "").lower()  # noqa: E731
    return strip(str(expected_value)) in strip(answer)

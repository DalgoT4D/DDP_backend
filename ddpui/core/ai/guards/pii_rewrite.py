"""Deterministic PII rewriting for columns the user ticked on the approval card.

The model has already written the SQL by the time the user chooses, so the
rewrite has to be mechanical — no second model call, which could quietly change
the query's meaning along with its column list.

resolve_projection answers "which physical columns does this query select?" (the
card's checkbox list). hash_projection wraps the ticked ones in the warehouse's
own hash function.

Both work on the tree AFTER sqlglot's qualify(), which resolves table aliases,
resolves unqualified column names, and expands SELECT * — given a schema map.
Columns are rewritten at EVERY projection in the tree, not only the outer SELECT
list: with a CTE the outer query selects `x.phone`, where `x` is not a physical
table, so an outer-only rewrite would miss it. Hashing where the column leaves
its real table propagates outward through any number of CTE or subquery layers.

Nothing outside a projection is touched. A value used only in WHERE, JOIN,
GROUP BY or HAVING never appears in a returned row, so there is nothing to hash.
"""

from dataclasses import dataclass

import sqlglot
from sqlglot import expressions as exp
from sqlglot.errors import OptimizeError, ParseError
from sqlglot.optimizer.qualify import qualify
from sqlglot.optimizer.scope import traverse_scope

# Predicates whose literal operand puts a real value into the SQL text itself.
# That text is persisted in the tool artifact, the checkpoint and the trace, so
# the card warns about it even though we never rewrite it.
_PREDICATES = (
    exp.EQ,
    exp.NEQ,
    exp.In,
    exp.Like,
    exp.ILike,
    exp.GT,
    exp.LT,
    exp.GTE,
    exp.LTE,
    exp.Between,
)


class UnresolvableProjection(Exception):
    """The SELECT list cannot be reduced to concrete schema.table.column entries.
    The message is written for the LLM to read and self-correct."""


@dataclass(frozen=True)
class ProjectedColumn:
    """One physical column this query selects, with whether the SQL compares it
    against a literal somewhere outside the projection."""

    schema: str
    table: str
    column: str
    has_literal: bool

    @property
    def key(self) -> str:
        return f"{self.schema}.{self.table}.{self.column}"


def resolve_projection(sql: str, dialect: str, schema_map: dict) -> list[ProjectedColumn]:
    """Every physical column selected anywhere in the tree, sorted by key.

    May over-list: a column a CTE selects but the outer query drops still appears.
    Over-listing only costs the user a tick that does nothing; under-listing would
    hide a column they needed to mask."""
    tree = _qualified_tree(sql, dialect, schema_map)
    literals = _columns_compared_to_literals(tree)

    found: dict[str, ProjectedColumn] = {}
    for _column, key in _projected_columns(tree):
        schema, table, column_name = key.split(".", 2)
        found[key] = ProjectedColumn(
            schema=schema,
            table=table,
            column=column_name,
            has_literal=key in literals,
        )
    return [found[key] for key in sorted(found)]


def _qualified_tree(sql: str, dialect: str, schema_map: dict) -> exp.Expression:
    try:
        tree = sqlglot.parse_one(sql, dialect=dialect)
    except ParseError as err:
        raise UnresolvableProjection(f"Could not parse the SQL: {err}") from err
    try:
        return qualify(tree, dialect=dialect, schema=schema_map)
    except OptimizeError as err:
        raise UnresolvableProjection(
            f"Could not work out which table each selected column belongs to ({err}). "
            "Re-check the column names with get_table_details, then list the selected "
            "columns explicitly and qualify each one with its table."
        ) from err


def _projected_columns(tree: exp.Expression) -> list[tuple[exp.Column, str]]:
    """(column node, schema.table.column) for every physical column projected in
    any scope. Resolution is per-scope: two subqueries may use the same alias for
    different tables, and a tree-wide alias map would silently merge them."""
    pairs = []
    for scope in traverse_scope(tree):
        select = scope.expression
        if not isinstance(select, exp.Select):
            continue
        for projection in select.expressions:
            for column in projection.find_all(exp.Column):
                key = _physical_key(scope, column)
                if key:
                    pairs.append((column, key))
    return pairs


def _physical_key(scope, column: exp.Column) -> str | None:
    """schema.table.column when this column reads from a real table in this scope,
    else None — a column sourced from a CTE or subquery is hashed inside that body
    instead, where it leaves its physical table."""
    source = scope.sources.get(column.table)
    if isinstance(source, exp.Table) and source.db:
        return f"{source.db}.{source.name}.{column.name}"
    return None


def _columns_compared_to_literals(tree: exp.Expression) -> set[str]:
    """schema.table.column keys the SQL compares against a literal OUTSIDE any
    projection — i.e. a real value sitting in the query text."""
    projection_nodes = {
        id(node)
        for scope in traverse_scope(tree)
        if isinstance(scope.expression, exp.Select)
        for projection in scope.expression.expressions
        for node in projection.walk()
    }
    keys: set[str] = set()
    for scope in traverse_scope(tree):
        select = scope.expression
        if not isinstance(select, exp.Select):
            continue
        for predicate in select.find_all(*_PREDICATES):
            if id(predicate) in projection_nodes:
                continue
            if not any(predicate.find_all(exp.Literal)):
                continue
            for column in predicate.find_all(exp.Column):
                key = _physical_key(scope, column)
                if key:
                    keys.add(key)
    return keys

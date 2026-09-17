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
    aliases = _alias_map(tree)
    literals = _columns_compared_to_literals(tree, aliases)

    found: dict[str, ProjectedColumn] = {}
    for column in _projected_columns(tree, aliases):
        schema, table = aliases[column.table]
        key = f"{schema}.{table}.{column.name}"
        found[key] = ProjectedColumn(
            schema=schema,
            table=table,
            column=column.name,
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


def _alias_map(tree: exp.Expression) -> dict[str, tuple[str, str]]:
    """{alias or table name: (schema, table)} for physical tables only.

    qualify() rewrites every column to carry its table's ALIAS, not its schema, so
    this is what turns `b.phone` into prod.beneficiaries.phone. CTE and subquery
    names have no schema and are excluded — a column pointing at one is not a
    physical column, and the hash is applied inside that CTE instead."""
    return {
        table.alias_or_name: (table.db, table.name)
        for table in tree.find_all(exp.Table)
        if table.db
    }


def _projected_columns(tree: exp.Expression, aliases: dict) -> list[exp.Column]:
    """Every column reference inside any SELECT list in the tree that resolves to
    a physical table."""
    columns = []
    for select in tree.find_all(exp.Select):
        for projection in select.expressions:
            columns.extend(
                column for column in projection.find_all(exp.Column) if column.table in aliases
            )
    return columns


def _columns_compared_to_literals(tree: exp.Expression, aliases: dict) -> set[str]:
    """schema.table.column keys the SQL compares against a literal OUTSIDE any
    projection — i.e. a real value sitting in the query text."""
    in_projection = {
        id(node)
        for select in tree.find_all(exp.Select)
        for projection in select.expressions
        for node in projection.walk()
    }
    keys: set[str] = set()
    for predicate in tree.find_all(*_PREDICATES):
        if id(predicate) in in_projection:
            continue
        if not any(predicate.find_all(exp.Literal)):
            continue
        for column in predicate.find_all(exp.Column):
            if column.table in aliases:
                schema, table = aliases[column.table]
                keys.add(f"{schema}.{table}.{column.name}")
    return keys

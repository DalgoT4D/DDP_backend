"""AST-based SQL guard for Chat with Data.

Validates LLM-generated SQL before it may touch an org's warehouse. All checks are
structural (sqlglot parse tree), never regex/keyword matching — comment tricks and
keyword-lookalike identifiers cannot bypass a node-type check.
"""

from dataclasses import dataclass, field

import sqlglot
from sqlglot import expressions as exp


class GuardError(Exception):
    """Raised when SQL fails validation. The message is written for the LLM to read
    and self-correct, so it names the violated rule."""


# Node types that make a statement non-read-only, wherever they appear in the tree
# (a DELETE inside a CTE is still a DELETE). Built with hasattr so a sqlglot upgrade
# that renames a class fails open-loudly in tests, not silently at runtime.
_FORBIDDEN_NODE_NAMES = (
    "Insert",
    "Update",
    "Delete",
    "Merge",
    "Create",
    "Drop",
    "Alter",
    "TruncateTable",
    "Command",  # COPY, VACUUM, CALL and other verb-first statements
    "Copy",
    "Set",
    "Show",
    "Use",
    "Grant",
    "Into",  # SELECT ... INTO writes a table
    "Lock",  # SELECT ... FOR UPDATE takes row locks
    "Transaction",
    "Commit",
    "Rollback",
)
_FORBIDDEN_NODES = tuple(getattr(exp, name) for name in _FORBIDDEN_NODE_NAMES if hasattr(exp, name))

# SELECT plus set operations over SELECTs (UNION / INTERSECT / EXCEPT) — all read-only.
# exp.SetOperation is their common base in current sqlglot; exp.Union in older versions.
_ALLOWED_ROOTS = (
    (exp.Select, exp.SetOperation) if hasattr(exp, "SetOperation") else (exp.Select, exp.Union)
)


@dataclass
class GuardedSQL:
    """The validated, rewritten statement that is safe to execute."""

    sql: str
    tables: list[str] = field(default_factory=list)


def validate(
    sql: str,
    dialect: str,
    allowed_schemas: list[str],
    max_rows: int,
    allowed_tables: list[str] | None = None,
) -> GuardedSQL:
    """Parse `sql` in the warehouse dialect and enforce the guard rules.

    `allowed_tables` (list of 'schema.table') restricts queries to those exact
    tables — used by dashboard-scoped chat. None means schema-level rules only;
    an empty list blocks every table (fail-closed). Comparison is
    case-insensitive: Postgres folds unquoted identifiers to lowercase, and our
    refs come from Chart rows written by the same case conventions. BigQuery
    dataset/table names are case-sensitive server-side, so a case-mismatched
    ref could pass the guard and then fail at the warehouse — an acceptable
    failure direction (never grants access it shouldn't).

    Returns a GuardedSQL with LIMIT injected/clamped, or raises GuardError.
    """
    try:
        statements = sqlglot.parse(sql, dialect=dialect)
    except sqlglot.errors.ParseError as err:
        raise GuardError(f"Could not parse SQL: {err}") from err

    if len(statements) != 1 or statements[0] is None:
        raise GuardError("Exactly one SQL statement is allowed")

    tree = statements[0]

    if not isinstance(tree, _ALLOWED_ROOTS):
        raise GuardError("Only SELECT statements are allowed")

    for node in tree.walk():
        if isinstance(node, _FORBIDDEN_NODES):
            raise GuardError(f"Query must be read-only — found a {node.key.upper()} operation")

    tables = _referenced_tables(tree)
    _check_schemas(tables, allowed_schemas)
    if allowed_tables is not None:
        _check_tables(tables, allowed_tables)

    _apply_limit(tree, max_rows)

    return GuardedSQL(sql=tree.sql(dialect=dialect), tables=sorted(tables))


def _referenced_tables(tree: exp.Expression) -> set[str]:
    """All physical tables referenced anywhere in the statement, as 'schema.table'.

    CTE aliases look like tables in the FROM clause but are not physical tables —
    an unqualified reference to a CTE alias is skipped, not schema-checked.
    """
    cte_aliases = {cte.alias_or_name for cte in tree.find_all(exp.CTE)}
    tables = set()
    for table in tree.find_all(exp.Table):
        name = table.name
        schema = table.db
        if not schema and name in cte_aliases:
            continue
        tables.add(f"{schema}.{name}" if schema else name)
    return tables


def _check_schemas(tables: set[str], allowed_schemas: list[str]) -> None:
    allowed = set(allowed_schemas)
    for ref in tables:
        if "." not in ref:
            raise GuardError(
                f"Table '{ref}' must be schema-qualified (e.g. schema.table). "
                f"Allowed schemas: {sorted(allowed)}"
            )
        schema = ref.split(".", 1)[0]
        if schema not in allowed:
            raise GuardError(
                f"Schema '{schema}' is not allowed. Allowed schemas: {sorted(allowed)}"
            )


def _check_tables(tables: set[str], allowed_tables: list[str]) -> None:
    """Every physical table must be in the scope's allowlist. The message is
    written for the LLM to relay to the user, so it explains the scope."""
    allowed = {ref.lower() for ref in allowed_tables}
    for ref in tables:
        if ref.lower() not in allowed:
            raise GuardError(
                f"Table '{ref}' is not available in this chat — this conversation "
                f"is scoped to one dashboard. Available tables: {sorted(allowed_tables)}. "
                "If the user's question needs other data, tell them to open the full "
                "Chat with Data page instead of retrying."
            )


def _apply_limit(tree: exp.Expression, max_rows: int) -> None:
    """Inject LIMIT max_rows, or clamp an existing larger LIMIT down to max_rows.
    A user-written smaller LIMIT is kept — it expresses intent ("top 5")."""
    existing = tree.args.get("limit")
    if existing is not None:
        try:
            current = int(existing.expression.this)
        except (TypeError, ValueError):
            current = None  # non-literal limit (e.g. LIMIT (SELECT ...)) — overwrite
        if current is not None and current <= max_rows:
            return
    tree.limit(max_rows, copy=False)

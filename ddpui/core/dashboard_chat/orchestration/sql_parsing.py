"""SQL parsing helpers for dashboard chat validation."""

from collections.abc import Sequence
import re
from typing import Any

from ddpui.core.dashboard_chat.context.allowlist import normalize_dashboard_chat_table_name
from ddpui.core.dashboard_chat.contracts import DashboardChatSchemaSnippet
from ddpui.core.dashboard_chat.warehouse.sql_guard import DashboardChatSqlGuard


def _primary_table_name(sql: str) -> str | None:
    """Return the primary FROM table for single-query correction logic."""
    table_match = re.search(r"\bFROM\s+([`\"]?)([\w\.]+)\1", sql, re.IGNORECASE)
    if not table_match:
        return None
    return normalize_dashboard_chat_table_name(table_match.group(2))


def _table_references(cls, sql: str) -> list[dict[str, str | None]]:
    """Return normalized FROM/JOIN table references and aliases from one SQL statement."""
    references: list[dict[str, str | None]] = []
    for match in re.finditer(
        r"\b(?:FROM|JOIN)\s+([`\"]?)([\w\.]+)\1(?:\s+(?:AS\s+)?([A-Za-z_][A-Za-z0-9_]*))?",
        sql,
        flags=re.IGNORECASE,
    ):
        table_name = normalize_dashboard_chat_table_name(match.group(2))
        if not table_name:
            continue
        alias = str(match.group(3) or "").lower() or None
        references.append(
            {
                "table_name": table_name,
                "alias": alias,
                "short_name": table_name.split(".")[-1],
            }
        )
    return references


def _resolve_table_qualifier(
    cls,
    qualifier: str,
    table_references: Sequence[dict[str, str | None]],
) -> str | None:
    """Resolve a qualifier like `f` or `analytics_table` to one query table."""
    normalized_qualifier = qualifier.lower().strip().strip('`"')
    matches = [
        str(reference["table_name"])
        for reference in table_references
        if normalized_qualifier
        in {
            str(reference.get("alias") or ""),
            str(reference.get("short_name") or ""),
            str(reference.get("table_name") or ""),
        }
    ]
    deduped_matches = list(dict.fromkeys(match for match in matches if match))
    if len(deduped_matches) == 1:
        return deduped_matches[0]
    return None


def _table_columns(snippet: DashboardChatSchemaSnippet | Any) -> set[str]:
    """Return the normalized column names available on one schema snippet."""
    return {
        str(column.get("name") or "").lower()
        for column in getattr(snippet, "columns", []) or []
    }


def _tables_with_column(
    cls,
    column_name: str,
    table_names: Sequence[str],
    schema_cache: dict[str, Any],
) -> list[str]:
    """Return the query tables that contain one column."""
    normalized_column_name = column_name.lower()
    return [
        table_name
        for table_name in table_names
        if normalized_column_name in cls._table_columns(schema_cache.get(table_name))
    ]


def _resolve_identifier_table(
    cls,
    *,
    qualifier: str | None,
    column_name: str,
    table_references: Sequence[dict[str, str | None]],
    schema_cache: dict[str, Any],
) -> str | None:
    """Resolve one referenced column to a concrete query table when it is unambiguous."""
    if qualifier is not None:
        resolved_table = cls._resolve_table_qualifier(qualifier, table_references)
        if not resolved_table:
            return None
        if column_name.lower() in cls._table_columns(schema_cache.get(resolved_table)):
            return resolved_table
        return None

    query_tables = [
        str(reference["table_name"])
        for reference in table_references
        if reference.get("table_name")
    ]
    matching_tables = cls._tables_with_column(column_name, query_tables, schema_cache)
    if len(matching_tables) == 1:
        return matching_tables[0]
    return None


def _referenced_sql_identifier_refs(cls, sql: str) -> list[tuple[str | None, str]]:
    """Extract likely physical identifier references from the outer SQL."""
    table_aliases = {
        alias.lower()
        for alias in re.findall(
            r"\b(?:FROM|JOIN)\s+[`\"]?[\w\.]+[`\"]?(?:\s+(?:AS\s+)?([A-Za-z_][A-Za-z0-9_]*))?",
            sql,
            flags=re.IGNORECASE,
        )
        if alias
    }
    select_aliases = cls._select_aliases(sql)
    referenced_identifiers: list[tuple[str | None, str]] = []

    select_clause = DashboardChatSqlGuard._extract_outer_select_clause(sql)
    if select_clause:
        for expression in DashboardChatSqlGuard._split_select_expressions(select_clause):
            referenced_identifiers.extend(
                cls._extract_identifier_refs_from_sql_segment(expression, table_aliases)
            )

    for pattern in [
        r"\bWHERE\s+(.+?)(?:\bGROUP\b|\bORDER\b|\bLIMIT\b|$)",
        r"\bGROUP\s+BY\s+(.+?)(?:\bORDER\b|\bLIMIT\b|$)",
        r"\bORDER\s+BY\s+(.+?)(?:\bLIMIT\b|$)",
    ]:
        match = re.search(pattern, sql, flags=re.IGNORECASE | re.DOTALL)
        if match:
            referenced_identifiers.extend(
                cls._extract_identifier_refs_from_sql_segment(
                    match.group(1),
                    table_aliases,
                    ignored_identifiers=select_aliases,
                )
            )

    return list(dict.fromkeys(referenced_identifiers))


def _select_aliases(sql: str) -> set[str]:
    """Return aliases introduced by the outer SELECT clause."""
    select_clause = DashboardChatSqlGuard._extract_outer_select_clause(sql)
    if not select_clause:
        return set()

    aliases: set[str] = set()
    for expression in DashboardChatSqlGuard._split_select_expressions(select_clause):
        alias_match = re.search(
            r"\bAS\s+([A-Za-z_][A-Za-z0-9_]*)\s*$",
            expression,
            flags=re.IGNORECASE,
        )
        if alias_match:
            aliases.add(alias_match.group(1).lower())
    return aliases


def _extract_identifier_refs_from_sql_segment(
    segment: str,
    table_aliases: set[str],
    ignored_identifiers: set[str] | None = None,
) -> list[tuple[str | None, str]]:
    """Pull qualified and unqualified column-like identifiers out of one SQL segment."""
    normalized_segment = re.sub(r"'[^']*'", " ", segment)
    normalized_segment = re.sub(
        r"\bAS\s+[A-Za-z_][A-Za-z0-9_]*",
        " ",
        normalized_segment,
        flags=re.IGNORECASE,
    )
    ignored_tokens = {
        "SELECT",
        "FROM",
        "WHERE",
        "GROUP",
        "BY",
        "ORDER",
        "LIMIT",
        "COUNT",
        "SUM",
        "AVG",
        "MIN",
        "MAX",
        "DISTINCT",
        "AND",
        "OR",
        "AS",
        "IN",
        "CASE",
        "WHEN",
        "THEN",
        "ELSE",
        "END",
        "TRUE",
        "FALSE",
        "NULL",
        "NOT",
        "ASC",
        "DESC",
        "ON",
        "JOIN",
    }
    ignored_identifiers = {identifier.lower() for identifier in (ignored_identifiers or set())}
    identifiers: list[tuple[str | None, str]] = []
    for match in re.finditer(
        r"(?:(?P<qualifier>[A-Za-z_][A-Za-z0-9_]*)\.)?(?P<identifier>[A-Za-z_][A-Za-z0-9_]*)",
        normalized_segment,
    ):
        qualifier = match.group("qualifier")
        identifier = match.group("identifier")
        if not identifier:
            continue
        if identifier.upper() in ignored_tokens:
            continue
        if identifier.lower() in table_aliases or identifier.lower() in ignored_identifiers:
            continue
        trailing_segment = normalized_segment[match.end() :].lstrip()
        if qualifier is None and trailing_segment.startswith("("):
            continue
        identifiers.append((qualifier.lower() if qualifier else None, identifier.lower()))
    return identifiers


def _best_table_for_missing_columns(
    missing_columns: Sequence[str],
    schema_cache: dict[str, Any],
) -> str | None:
    """Return the first allowlisted table that covers all missing columns."""
    wanted_columns = {column_name.lower() for column_name in missing_columns}
    for table_name, snippet in schema_cache.items():
        available_columns = {
            str(column.get("name") or "").lower() for column in snippet.columns
        }
        if wanted_columns.issubset(available_columns):
            return table_name
    return None


def _extract_text_filter_values(where_clause: str) -> list[tuple[str | None, str, str]]:
    """Extract quoted text filter values from one WHERE clause."""
    extracted_values: list[tuple[str | None, str, str]] = []
    for qualifier, column_name, value in re.findall(
        r"(?:([a-zA-Z_][a-zA-Z0-9_]*)\.)?([a-zA-Z_][a-zA-Z0-9_]*)\s*=\s*'([^']+)'",
        where_clause,
        flags=re.IGNORECASE,
    ):
        extracted_values.append((qualifier.lower() if qualifier else None, column_name, value))

    for match in re.finditer(
        r"(?:([a-zA-Z_][a-zA-Z0-9_]*)\.)?([a-zA-Z_][a-zA-Z0-9_]*)\s+IN\s*\(([^)]*)\)",
        where_clause,
        flags=re.IGNORECASE,
    ):
        qualifier = match.group(1)
        column_name = match.group(2)
        for value in re.findall(r"'([^']+)'", match.group(3)):
            extracted_values.append(
                (qualifier.lower() if qualifier else None, column_name, value)
            )
    return extracted_values


def _find_tables_with_column(
    column_name: str,
    schema_cache: dict[str, Any],
    limit: int = 10,
) -> list[str]:
    """Find allowlisted tables that contain one column."""
    matches: list[str] = []
    normalized_column_name = column_name.lower()
    for table_name, snippet in schema_cache.items():
        if any(
            normalized_column_name == str(column.get("name") or "").lower()
            for column in snippet.columns
        ):
            matches.append(table_name)
        if len(matches) >= limit:
            break
    return matches


def _structural_dimensions_from_sql(cls, sql: str) -> set[str]:
    """Return normalized non-aggregate dimensions used by one SQL statement."""
    if not sql:
        return set()

    dimensions: set[str] = set()
    for dimension in cls._extract_dimensions_from_sql(sql):
        identifier_refs = cls._extract_identifier_refs_from_sql_segment(
            dimension,
            table_aliases=set(),
        )
        if identifier_refs:
            dimensions.update(
                cls._normalize_dimension_name(column_name)
                for _, column_name in identifier_refs
            )
            continue
        dimensions.add(cls._normalize_dimension_name(dimension))
    select_clause = DashboardChatSqlGuard._extract_outer_select_clause(sql)
    if not select_clause:
        return {dimension for dimension in dimensions if dimension}

    for expression in DashboardChatSqlGuard._split_select_expressions(select_clause):
        normalized_expression = expression.strip()
        if not normalized_expression or DashboardChatSqlGuard._contains_aggregate(
            normalized_expression
        ):
            continue
        for _, column_name in cls._extract_identifier_refs_from_sql_segment(
            normalized_expression,
            table_aliases=set(),
            ignored_identifiers=cls._select_aliases(sql),
        ):
            dimensions.add(cls._normalize_dimension_name(column_name))
    return {dimension for dimension in dimensions if dimension}


def _normalize_dimension_name(value: str) -> str:
    """Normalize dimension names from SQL expressions and natural-language follow-ups."""
    normalized_value = value.strip().strip('`"').lower()
    normalized_value = normalized_value.split(".")[-1]
    normalized_value = re.sub(r"[^a-z0-9_]+", "_", normalized_value)
    normalized_value = re.sub(r"_+", "_", normalized_value).strip("_")
    return normalized_value

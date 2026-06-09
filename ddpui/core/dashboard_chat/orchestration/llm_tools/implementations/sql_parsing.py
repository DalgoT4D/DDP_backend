"""SQL parsing helpers for dashboard chat validation."""

from collections.abc import Sequence
import re
from typing import Any

from ddpui.core.dashboard_chat.context.dashboard_table_allowlist import (
    find_matching_dashboard_chat_table_name,
    normalize_dashboard_chat_table_name,
)
from ddpui.core.dashboard_chat.contracts.retrieval_contracts import DashboardChatSchemaSnippet
from ddpui.core.dashboard_chat.orchestration.conversation_context import extract_dimensions_from_sql
from ddpui.core.dashboard_chat.warehouse.sql_guard import DashboardChatSqlGuard


def strip_nested_query_segments(sql: str) -> str:
    """Replace nested SELECT/WITH subqueries with empty parens for outer-query parsing."""
    result: list[str] = []
    stack: list[list[str]] = []

    for char in sql:
        if char == "(":
            stack.append(["("])
            continue
        if char == ")" and stack:
            segment = stack.pop()
            segment.append(")")
            segment_text = "".join(segment)
            replacement = "()" if re.search(r"\b(?:select|with)\b", segment_text, re.IGNORECASE) else segment_text
            if stack:
                stack[-1].append(replacement)
            else:
                result.append(replacement)
            continue
        if stack:
            stack[-1].append(char)
        else:
            result.append(char)

    while stack:
        orphaned_segment = "".join(stack.pop())
        if stack:
            stack[-1].append(orphaned_segment)
        else:
            result.append(orphaned_segment)
    return "".join(result)


def _read_balanced_parentheses(sql: str, start_index: int) -> tuple[str, int]:
    """Return one balanced parenthesized segment and the index after it."""
    depth = 0
    collected: list[str] = []
    for index in range(start_index, len(sql)):
        char = sql[index]
        collected.append(char)
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
            if depth == 0:
                return "".join(collected), index + 1
    return "".join(collected), len(sql)


def extract_cte_output_columns(sql: str) -> dict[str, set[str]]:
    """Infer output columns for leading CTEs so alias-based SQL isn't falsely rejected."""
    stripped_sql = sql.lstrip()
    if not stripped_sql[:4].lower() == "with":
        return {}

    cursor = 4
    cte_columns: dict[str, set[str]] = {}
    if stripped_sql[cursor:].lstrip().lower().startswith("recursive"):
        cursor = stripped_sql.lower().index("recursive", cursor) + len("recursive")

    while cursor < len(stripped_sql):
        while cursor < len(stripped_sql) and stripped_sql[cursor].isspace():
            cursor += 1
        name_match = re.match(r"([A-Za-z_][A-Za-z0-9_]*)", stripped_sql[cursor:])
        if not name_match:
            break
        cte_name = name_match.group(1).lower()
        cursor += len(name_match.group(1))

        while cursor < len(stripped_sql) and stripped_sql[cursor].isspace():
            cursor += 1

        explicit_columns: set[str] = set()
        if cursor < len(stripped_sql) and stripped_sql[cursor] == "(":
            column_list_segment, cursor = _read_balanced_parentheses(stripped_sql, cursor)
            explicit_columns = {
                token.strip().strip('`"').lower()
                for token in column_list_segment.strip()[1:-1].split(",")
                if token.strip()
            }
            while cursor < len(stripped_sql) and stripped_sql[cursor].isspace():
                cursor += 1

        as_match = re.match(r"AS\s*\(", stripped_sql[cursor:], flags=re.IGNORECASE)
        if not as_match:
            break
        cursor += as_match.end() - 1
        cte_body_segment, cursor = _read_balanced_parentheses(stripped_sql, cursor)
        cte_body = cte_body_segment[1:-1]

        columns = set(explicit_columns)
        if not columns:
            select_clause = DashboardChatSqlGuard._extract_outer_select_clause(cte_body)
            if select_clause:
                for expression in DashboardChatSqlGuard._split_select_expressions(select_clause):
                    alias_match = re.search(
                        r"\bAS\s+([A-Za-z_][A-Za-z0-9_]*)\s*$",
                        expression,
                        flags=re.IGNORECASE,
                    )
                    if alias_match:
                        columns.add(alias_match.group(1).lower())
                        continue
                    simple_column_match = re.fullmatch(
                        r"(?:[A-Za-z_][A-Za-z0-9_]*\.)?([A-Za-z_][A-Za-z0-9_]*)",
                        expression.strip(),
                    )
                    if simple_column_match:
                        columns.add(simple_column_match.group(1).lower())
        if columns:
            cte_columns[cte_name] = columns

        while cursor < len(stripped_sql) and stripped_sql[cursor].isspace():
            cursor += 1
        if cursor < len(stripped_sql) and stripped_sql[cursor] == ",":
            cursor += 1
            continue
        break
    return cte_columns


def extract_cte_names(sql: str) -> set[str]:
    """Return leading CTE names, even when their output columns cannot be inferred."""
    stripped_sql = sql.lstrip()
    if not stripped_sql[:4].lower() == "with":
        return set()

    cursor = 4
    cte_names: set[str] = set()
    if stripped_sql[cursor:].lstrip().lower().startswith("recursive"):
        cursor = stripped_sql.lower().index("recursive", cursor) + len("recursive")

    while cursor < len(stripped_sql):
        while cursor < len(stripped_sql) and stripped_sql[cursor].isspace():
            cursor += 1
        name_match = re.match(r"([A-Za-z_][A-Za-z0-9_]*)", stripped_sql[cursor:])
        if not name_match:
            break
        cte_names.add(name_match.group(1).lower())
        cursor += len(name_match.group(1))

        while cursor < len(stripped_sql) and stripped_sql[cursor].isspace():
            cursor += 1
        if cursor < len(stripped_sql) and stripped_sql[cursor] == "(":
            _, cursor = _read_balanced_parentheses(stripped_sql, cursor)
            while cursor < len(stripped_sql) and stripped_sql[cursor].isspace():
                cursor += 1

        as_match = re.match(r"AS\s*\(", stripped_sql[cursor:], flags=re.IGNORECASE)
        if not as_match:
            break
        cursor += as_match.end() - 1
        _, cursor = _read_balanced_parentheses(stripped_sql, cursor)

        while cursor < len(stripped_sql) and stripped_sql[cursor].isspace():
            cursor += 1
        if cursor < len(stripped_sql) and stripped_sql[cursor] == ",":
            cursor += 1
            continue
        break
    return cte_names


def cte_schema_snippets(sql: str) -> dict[str, DashboardChatSchemaSnippet]:
    """Build synthetic schema snippets for leading CTEs based on their output columns."""
    snippets: dict[str, DashboardChatSchemaSnippet] = {}
    for cte_name, columns in extract_cte_output_columns(sql).items():
        snippets[cte_name] = DashboardChatSchemaSnippet(
            table_name=cte_name,
            columns=[
                {"name": column_name, "data_type": "derived", "nullable": True}
                for column_name in sorted(columns)
            ],
        )
    return snippets


def primary_table_name(sql: str) -> str | None:
    """Return the primary FROM table for single-query correction logic."""
    table_match = re.search(
        r"\bFROM\s+([`\"]?)([\w\.]+)\1",
        strip_nested_query_segments(sql),
        re.IGNORECASE,
    )
    if not table_match:
        return None
    return normalize_dashboard_chat_table_name(table_match.group(2))


def table_references(sql: str) -> list[dict[str, str | None]]:
    """Return normalized FROM/JOIN table references and aliases from one SQL statement."""
    stripped_sql = strip_nested_query_segments(sql)
    alias_stopwords = {
        "where",
        "group",
        "order",
        "limit",
        "join",
        "on",
        "left",
        "right",
        "inner",
        "outer",
        "full",
        "cross",
        "union",
        "having",
    }
    references: list[dict[str, str | None]] = []
    for match in re.finditer(
        r"\b(?:FROM|JOIN)\s+([`\"]?)([\w\.]+)\1(?:\s+(?:AS\s+)?([A-Za-z_][A-Za-z0-9_]*))?",
        stripped_sql,
        flags=re.IGNORECASE,
    ):
        table_name = normalize_dashboard_chat_table_name(match.group(2))
        if not table_name:
            continue
        alias = str(match.group(3) or "").lower() or None
        if alias in alias_stopwords:
            alias = None
        references.append(
            {
                "table_name": table_name,
                "alias": alias,
                "short_name": table_name.split(".")[-1],
            }
        )
    return references


def resolve_table_qualifier(
    qualifier: str,
    table_refs: Sequence[dict[str, str | None]],
) -> str | None:
    """Resolve a qualifier like `f` or `analytics_table` to one query table."""
    normalized_qualifier = qualifier.lower().strip().strip('`"')
    matches = [
        str(reference["table_name"])
        for reference in table_refs
        if normalized_qualifier
        in {
            str(reference.get("alias") or ""),
            str(reference.get("short_name") or "").lower(),
            str(reference.get("table_name") or "").lower(),
        }
    ]
    deduped_matches = list(dict.fromkeys(match for match in matches if match))
    if len(deduped_matches) == 1:
        return deduped_matches[0]
    return None


def table_columns(snippet: DashboardChatSchemaSnippet | Any) -> set[str]:
    """Return the normalized column names available on one schema snippet."""
    return {
        str(column.get("name") or "").lower() for column in getattr(snippet, "columns", []) or []
    }


def schema_snippet_for_table(
    table_name: str,
    schema_snippets_by_table: dict[str, Any],
) -> DashboardChatSchemaSnippet | Any | None:
    """Return a schema snippet using case-insensitive table matching when needed."""
    if table_name in schema_snippets_by_table:
        return schema_snippets_by_table[table_name]
    matched_table_name = find_matching_dashboard_chat_table_name(table_name, schema_snippets_by_table)
    if matched_table_name is None:
        return None
    return schema_snippets_by_table.get(matched_table_name)


def tables_with_column(
    column_name: str,
    table_names: Sequence[str],
    schema_snippets_by_table: dict[str, Any],
) -> list[str]:
    """Return the query tables that contain one column."""
    normalized_column_name = column_name.lower()
    return [
        table_name
        for table_name in table_names
        if normalized_column_name
        in table_columns(schema_snippet_for_table(table_name, schema_snippets_by_table))
    ]


def resolve_identifier_table(
    *,
    qualifier: str | None,
    column_name: str,
    table_refs: Sequence[dict[str, str | None]],
    schema_snippets_by_table: dict[str, Any],
) -> str | None:
    """Resolve one referenced column to a concrete query table when it is unambiguous."""
    if qualifier is not None:
        resolved_table = resolve_table_qualifier(qualifier, table_refs)
        if not resolved_table:
            return None
        if column_name.lower() in table_columns(
            schema_snippet_for_table(resolved_table, schema_snippets_by_table)
        ):
            return resolved_table
        return None

    query_tables = [
        str(reference["table_name"]) for reference in table_refs if reference.get("table_name")
    ]
    matching_tables = tables_with_column(column_name, query_tables, schema_snippets_by_table)
    if len(matching_tables) == 1:
        return matching_tables[0]
    return None


def select_aliases(sql: str) -> set[str]:
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


def extract_identifier_refs_from_sql_segment(
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
        "IS",
        "NOT",
        "DATE",
        "CAST",
        "INTERVAL",
        "CURRENT_DATE",
        "CURRENT_TIMESTAMP",
        "EXTRACT",
        "GROUPING",
        "SETS",
        "YEAR",
        "MONTH",
        "DAY",
        "NUMERIC",
        "DECIMAL",
        "INTEGER",
        "BIGINT",
        "SMALLINT",
        "TEXT",
        "VARCHAR",
        "CHAR",
        "BOOLEAN",
        "TIMESTAMP",
        "DOUBLE",
        "PRECISION",
        "FLOAT",
        "REAL",
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
        if normalized_segment[max(0, match.start() - 2) : match.start()] == "::":
            continue
        preceding_text = normalized_segment[: match.start()]
        preceding_token_match = re.search(r"([A-Za-z_][A-Za-z0-9_]*)\s*$", preceding_text)
        preceding_token = (
            preceding_token_match.group(1).upper() if preceding_token_match is not None else None
        )
        if preceding_token in {"FROM", "JOIN"}:
            continue
        trailing_segment = normalized_segment[match.end() :].lstrip()
        if qualifier is None and trailing_segment.startswith("("):
            continue
        identifiers.append((qualifier.lower() if qualifier else None, identifier.lower()))
    return identifiers


def referenced_sql_identifier_refs(sql: str) -> list[tuple[str | None, str]]:
    """Extract likely physical identifier references from the outer SQL."""
    stripped_sql = strip_nested_query_segments(sql)
    table_aliases = {
        alias.lower()
        for alias in re.findall(
            r"\b(?:FROM|JOIN)\s+[`\"]?[\w\.]+[`\"]?(?:\s+(?:AS\s+)?([A-Za-z_][A-Za-z0-9_]*))?",
            stripped_sql,
            flags=re.IGNORECASE,
        )
        if alias
    }
    sql_select_aliases = select_aliases(sql)
    referenced_identifiers: list[tuple[str | None, str]] = []

    select_clause = DashboardChatSqlGuard._extract_outer_select_clause(sql)
    if select_clause:
        for expression in DashboardChatSqlGuard._split_select_expressions(select_clause):
            referenced_identifiers.extend(
                extract_identifier_refs_from_sql_segment(expression, table_aliases)
            )

    for pattern in [
        r"\bWHERE\s+(.+?)(?:\bGROUP\b|\bORDER\b|\bLIMIT\b|$)",
        r"\bGROUP\s+BY\s+(.+?)(?:\bORDER\b|\bLIMIT\b|$)",
        r"\bORDER\s+BY\s+(.+?)(?:\bLIMIT\b|$)",
    ]:
        match = re.search(pattern, stripped_sql, flags=re.IGNORECASE | re.DOTALL)
        if match:
            referenced_identifiers.extend(
                extract_identifier_refs_from_sql_segment(
                    match.group(1),
                    table_aliases,
                    ignored_identifiers=sql_select_aliases,
                )
            )

    return list(dict.fromkeys(referenced_identifiers))


def best_table_for_missing_columns(
    missing_columns: Sequence[str],
    schema_snippets_by_table: dict[str, Any],
) -> str | None:
    """Return the first allowlisted table that covers all missing columns."""
    wanted_columns = {column_name.lower() for column_name in missing_columns}
    for table_name, snippet in schema_snippets_by_table.items():
        available_columns = {str(column.get("name") or "").lower() for column in snippet.columns}
        if wanted_columns.issubset(available_columns):
            return table_name
    return None


def extract_text_filter_values(where_clause: str) -> list[tuple[str | None, str, str]]:
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
            extracted_values.append((qualifier.lower() if qualifier else None, column_name, value))
    return extracted_values


def find_tables_with_column(
    column_name: str,
    schema_snippets_by_table: dict[str, Any],
    limit: int = 10,
) -> list[str]:
    """Find allowlisted tables that contain one column."""
    matches: list[str] = []
    normalized_column_name = column_name.lower()
    for table_name, snippet in schema_snippets_by_table.items():
        if any(
            normalized_column_name == str(column.get("name") or "").lower()
            for column in snippet.columns
        ):
            matches.append(table_name)
        if len(matches) >= limit:
            break
    return matches


def structural_dimensions_from_sql(sql: str) -> set[str]:
    """Return normalized non-aggregate dimensions used by one SQL statement."""
    if not sql:
        return set()

    dimensions: set[str] = set()
    for dimension in extract_dimensions_from_sql(sql):
        identifier_refs = extract_identifier_refs_from_sql_segment(dimension, table_aliases=set())
        if identifier_refs:
            dimensions.update(normalize_dimension_name(col) for _, col in identifier_refs)
            continue
        dimensions.add(normalize_dimension_name(dimension))

    select_clause = DashboardChatSqlGuard._extract_outer_select_clause(sql)
    if not select_clause:
        return {d for d in dimensions if d}

    for expression in DashboardChatSqlGuard._split_select_expressions(select_clause):
        normalized_expression = expression.strip()
        if not normalized_expression or DashboardChatSqlGuard._contains_aggregate(
            normalized_expression
        ):
            continue
        for _, column_name in extract_identifier_refs_from_sql_segment(
            normalized_expression,
            table_aliases=set(),
            ignored_identifiers=select_aliases(sql),
        ):
            dimensions.add(normalize_dimension_name(column_name))

    return {d for d in dimensions if d}


def normalize_dimension_name(value: str) -> str:
    """Normalize dimension names from SQL expressions and natural-language follow-ups."""
    normalized_value = value.strip().strip('`"').lower()
    normalized_value = normalized_value.split(".")[-1]
    normalized_value = re.sub(r"[^a-z0-9_]+", "_", normalized_value)
    normalized_value = re.sub(r"_+", "_", normalized_value).strip("_")
    return normalized_value

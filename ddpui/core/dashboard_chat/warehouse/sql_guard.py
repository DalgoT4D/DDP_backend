"""SQL safety guardrails for dashboard chat."""

import re

import sqlparse

from ddpui.core.dashboard_chat.context.dashboard_table_allowlist import DashboardChatAllowlist
from ddpui.core.dashboard_chat.contracts.sql_contracts import DashboardChatSqlValidationResult

FORBIDDEN_SQL_KEYWORDS = {
    "INTO",
    "INSERT",
    "UPDATE",
    "DELETE",
    "DROP",
    "ALTER",
    "CREATE",
    "TRUNCATE",
    "GRANT",
    "REVOKE",
    "MERGE",
    "CALL",
    "EXECUTE",
    "VACUUM",
}

AGGREGATE_FUNCTION_PATTERNS = (
    r"\bCOUNT\s*\(",
    r"\bSUM\s*\(",
    r"\bAVG\s*\(",
    r"\bMIN\s*\(",
    r"\bMAX\s*\(",
)

class DashboardChatSqlGuard:
    """Validate SQL before it reaches the warehouse."""

    def __init__(
        self,
        allowlist: DashboardChatAllowlist,
        max_rows: int = 200,
    ):
        self.allowlist = allowlist
        self.max_rows = max_rows

    def validate(self, sql: str) -> DashboardChatSqlValidationResult:
        """Validate a generated SQL statement."""
        errors: list[str] = []
        warnings: list[str] = []

        sql_without_comments = self._strip_sql_comments(sql)
        statements = [
            statement.strip()
            for statement in sqlparse.split(sql_without_comments)
            if statement.strip()
        ]
        if len(statements) != 1:
            return DashboardChatSqlValidationResult(
                is_valid=False,
                sanitized_sql=None,
                errors=["Multiple statements are not allowed"],
            )

        sanitized_sql = statements[0].rstrip(";").strip()
        sql_upper = sanitized_sql.upper()

        if not (sql_upper.startswith("SELECT") or sql_upper.startswith("WITH")):
            errors.append("Query must start with SELECT or WITH")

        select_into_detected = self._contains_select_into_clause(sanitized_sql)
        if select_into_detected:
            errors.append("SELECT INTO is not allowed")

        for keyword in FORBIDDEN_SQL_KEYWORDS:
            if keyword == "INTO" and select_into_detected:
                continue
            if re.search(rf"\b{keyword}\b", sql_upper):
                errors.append(f"Forbidden keyword detected: {keyword}")

        limit_match = re.search(r"\bLIMIT\s+(\d+)\b", sql_upper)
        if limit_match:
            limit_value = int(limit_match.group(1))
            if limit_value > self.max_rows:
                errors.append(f"LIMIT {limit_value} exceeds the maximum allowed {self.max_rows}")
        else:
            sanitized_sql = f"{sanitized_sql}\nLIMIT {self.max_rows}"
            warnings.append(f"No LIMIT clause found. Added LIMIT {self.max_rows}.")

        if re.search(r"\bSELECT\s+\*", sql_upper):
            warnings.append("SELECT * detected. Prefer explicit column lists.")

        tables = self._extract_table_names(sanitized_sql)
        for table_name in tables:
            if not self.allowlist.is_allowed(table_name):
                errors.append(
                    f"Table '{table_name}' is not accessible in the current dashboard context"
                )

        return DashboardChatSqlValidationResult(
            is_valid=not errors,
            sanitized_sql=sanitized_sql if not errors else None,
            tables=tables,
            warnings=warnings,
            errors=errors,
        )

    @staticmethod
    def _strip_sql_comments(sql: str) -> str:
        """Remove line and block comments before validation."""
        return sqlparse.format(sql, strip_comments=True)

    @classmethod
    def _extract_table_names(cls, sql: str) -> list[str]:
        """Extract physical table names from FROM/JOIN clauses."""
        sql_without_quotes = sql.replace('"', "").replace("`", "")
        cte_names = set(
            cte_name.lower()
            for cte_name in re.findall(
                r"(?:\bWITH\b|,)\s*([a-zA-Z_][a-zA-Z0-9_]*)\s+AS\s*\(",
                sql_without_quotes,
                flags=re.IGNORECASE,
            )
        )

        tables: list[str] = []
        depth = 0
        index = 0
        sql_length = len(sql_without_quotes)
        while index < sql_length:
            character = sql_without_quotes[index]
            if character == "(":
                depth += 1
                index += 1
                continue
            if character == ")":
                depth = max(depth - 1, 0)
                index += 1
                continue

            keyword = None
            if depth == 0:
                if cls._matches_keyword(sql_without_quotes.upper(), index, "FROM"):
                    keyword = "FROM"
                elif cls._matches_keyword(sql_without_quotes.upper(), index, "JOIN"):
                    keyword = "JOIN"

            if keyword is None:
                index += 1
                continue

            index += len(keyword)
            while index < sql_length and sql_without_quotes[index].isspace():
                index += 1

            start = index
            while index < sql_length and (
                sql_without_quotes[index].isalnum()
                or sql_without_quotes[index] in {"_", "."}
            ):
                index += 1

            table_name = sql_without_quotes[start:index].lower()
            if (
                table_name
                and table_name.upper() not in FORBIDDEN_SQL_KEYWORDS
                and table_name not in cte_names
            ):
                tables.append(table_name)

        return list(dict.fromkeys(tables))

    @classmethod
    def _contains_select_into_clause(cls, sql: str) -> bool:
        """Detect SELECT ... INTO before the outer FROM clause."""
        select_clause = cls._extract_outer_select_clause(sql)
        if not select_clause:
            return False
        return bool(re.search(r"\bINTO\b", select_clause, re.IGNORECASE))

    @staticmethod
    def _extract_outer_select_clause(sql: str) -> str | None:
        """Return the outer-most SELECT projection segment."""
        sql_upper = sql.upper()
        depth = 0
        select_start: int | None = None

        for index, character in enumerate(sql_upper):
            if character == "(":
                depth += 1
                continue
            if character == ")":
                depth = max(depth - 1, 0)
                continue

            if depth == 0 and DashboardChatSqlGuard._matches_keyword(sql_upper, index, "SELECT"):
                select_start = index + len("SELECT")
                break

        if select_start is None:
            return None

        depth = 0
        for index in range(select_start, len(sql_upper)):
            character = sql_upper[index]
            if character == "(":
                depth += 1
                continue
            if character == ")":
                depth = max(depth - 1, 0)
                continue
            if depth == 0 and DashboardChatSqlGuard._matches_keyword(sql_upper, index, "FROM"):
                return sql[select_start:index].strip()

        return None

    @staticmethod
    def _matches_keyword(sql_upper: str, index: int, keyword: str) -> bool:
        """Check whether a keyword occurs at a top-level position."""
        keyword_end = index + len(keyword)
        if sql_upper[index:keyword_end] != keyword:
            return False

        previous_character = sql_upper[index - 1] if index > 0 else " "
        next_character = sql_upper[keyword_end] if keyword_end < len(sql_upper) else " "
        return not (previous_character.isalnum() or previous_character == "_") and not (
            next_character.isalnum() or next_character == "_"
        )

    @staticmethod
    def _split_select_expressions(select_clause: str) -> list[str]:
        """Split a SELECT clause by top-level commas only."""
        expressions: list[str] = []
        current_expression: list[str] = []
        depth = 0

        for character in select_clause:
            if character == "(":
                depth += 1
            elif character == ")":
                depth = max(depth - 1, 0)
            elif character == "," and depth == 0:
                expressions.append("".join(current_expression).strip())
                current_expression = []
                continue
            current_expression.append(character)

        if current_expression:
            expressions.append("".join(current_expression).strip())
        return expressions

    @staticmethod
    def _contains_aggregate(expression: str) -> bool:
        """Return whether one SELECT expression uses an aggregate function."""
        return any(
            re.search(pattern, expression, re.IGNORECASE) for pattern in AGGREGATE_FUNCTION_PATTERNS
        )

"""SQL safety guardrails for dashboard chat."""

import re

import sqlparse

from ddpui.core.dashboard_chat.allowlist import DashboardChatAllowlist
from ddpui.core.dashboard_chat.runtime_types import DashboardChatSqlValidationResult

FORBIDDEN_SQL_KEYWORDS = {
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

PII_PATTERNS = [
    r"\b(name|phone|email|address|national_id|id_number)\b",
    r"\b(contact|mobile|telephone|personal|identification)\b",
    r"\b(firstname|lastname|full_name|participant_name|survivor_name)\b",
]


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
            statement.strip() for statement in sqlparse.split(sql_without_comments) if statement.strip()
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

        for keyword in FORBIDDEN_SQL_KEYWORDS:
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

        for pii_pattern in PII_PATTERNS:
            if re.search(pii_pattern, sanitized_sql, re.IGNORECASE):
                warnings.append(f"Query may touch PII-like columns matching {pii_pattern}.")

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
        sql_without_block_comments = re.sub(r"/\*.*?\*/", "", sql, flags=re.DOTALL)
        return re.sub(r"--.*", "", sql_without_block_comments)

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
        for table_name in re.findall(
            r"\b(?:FROM|JOIN)\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?)",
            sql_without_quotes,
            flags=re.IGNORECASE,
        ):
            if table_name.upper() in FORBIDDEN_SQL_KEYWORDS or table_name.lower() in cte_names:
                continue
            tables.append(table_name.lower())

        return list(dict.fromkeys(tables))

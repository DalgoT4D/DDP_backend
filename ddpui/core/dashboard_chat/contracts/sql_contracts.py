"""SQL-validation dashboard chat contracts."""

from dataclasses import dataclass, field


@dataclass(frozen=True)
class DashboardChatSqlValidationResult:
    """Outcome of SQL guard validation."""

    is_valid: bool
    sanitized_sql: str | None
    tables: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

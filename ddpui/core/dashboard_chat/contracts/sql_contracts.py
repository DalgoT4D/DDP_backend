"""SQL-validation dashboard chat contracts."""

from pydantic import BaseModel, ConfigDict, Field


class DashboardChatSqlValidationResult(BaseModel):
    """Outcome of SQL guard validation."""

    model_config = ConfigDict(frozen=True)

    is_valid: bool
    sanitized_sql: str | None
    tables: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)


class DashboardChatSqlVerificationResult(BaseModel):
    """Outcome of semantic SQL verification against the user question."""

    model_config = ConfigDict(frozen=True)

    is_valid: bool
    severity: str = "hard_block"
    reason_code: str = ""
    reasoning: str = ""
    issues: list[str] = Field(default_factory=list)
    repair_instructions: list[str] = Field(default_factory=list)
    risk_flags: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)

"""Pydantic schemas for dashboard chat metadata artifacts."""

from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


DASHBOARD_CHAT_METADATA_SCHEMA_VERSION = 5


def _normalize_string_list(value: Any) -> list[str]:
    """Accept either a single string or an iterable of strings."""
    if value is None:
        return []
    if isinstance(value, str):
        stripped = value.strip()
        return [stripped] if stripped else []
    if isinstance(value, (list, tuple, set)):
        normalized: list[str] = []
        for item in value:
            if item is None:
                continue
            text = str(item).strip()
            if text:
                normalized.append(text)
        return normalized
    text = str(value).strip()
    return [text] if text else []


class DashboardChatColumnRangeProfile(BaseModel):
    """Observed numeric or time range metadata for one column."""

    model_config = ConfigDict(extra="ignore")

    numeric_min: float | None = None
    numeric_max: float | None = None
    time_min: str | None = None
    time_max: str | None = None


class DashboardChatColumnStatistics(BaseModel):
    """Observed low-level column statistics."""

    model_config = ConfigDict(extra="ignore")

    nullable: bool | None = None
    null_percentage: float | None = None
    sample_values: list[str] = Field(default_factory=list)
    range_profile: DashboardChatColumnRangeProfile | None = None

    @field_validator("sample_values", mode="before")
    @classmethod
    def _normalize_sample_values(cls, value: Any) -> list[str]:
        return _normalize_string_list(value)


class DashboardChatMetadataColumn(BaseModel):
    """Structured metadata for one table column."""

    model_config = ConfigDict(extra="ignore")

    column_name: str = ""
    data_type: str = ""
    description: str = ""
    semantic_role: str = ""
    value_semantics: str = ""
    pii: bool = False
    statistics: DashboardChatColumnStatistics = Field(default_factory=DashboardChatColumnStatistics)

    @model_validator(mode="before")
    @classmethod
    def _flatten_legacy_shape(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "observed" not in value and "inferred" not in value:
            return value

        observed = value.get("observed") or {}
        inferred = value.get("inferred") or {}
        range_profile = DashboardChatColumnRangeProfile(
            numeric_min=observed.get("numeric_min"),
            numeric_max=observed.get("numeric_max"),
            time_min=observed.get("time_min"),
            time_max=observed.get("time_max"),
        )
        return {
            "column_name": value.get("column_name") or observed.get("name") or value.get("name") or "",
            "data_type": value.get("data_type") or observed.get("data_type") or observed.get("type") or "",
            "description": value.get("description") or observed.get("description") or "",
            "semantic_role": value.get("semantic_role") or inferred.get("semantic_role") or "",
            "value_semantics": value.get("value_semantics") or "",
            "pii": bool(value.get("pii", inferred.get("pii", False))),
            "statistics": {
                "nullable": observed.get("nullable"),
                "null_percentage": observed.get("null_percentage"),
                "sample_values": observed.get("sample_values") or [],
                "range_profile": range_profile.model_dump(mode="json")
                if any(
                    candidate is not None
                    for candidate in [
                        range_profile.numeric_min,
                        range_profile.numeric_max,
                        range_profile.time_min,
                        range_profile.time_max,
                    ]
                )
                else None,
            },
        }

    @property
    def name(self) -> str:
        return self.column_name


class DashboardChatMetadataJoinPath(BaseModel):
    """One inferred candidate join path between two allowlisted tables."""

    model_config = ConfigDict(extra="ignore")

    source_table: str
    target_table: str
    via_columns: list[str] = Field(default_factory=list)
    cardinality: str = ""
    preferred: bool = False
    dashboard_relevant: bool = True
    required_for_entity_names: bool = False
    required_for_metrics: bool = False

    @field_validator("via_columns", mode="before")
    @classmethod
    def _normalize_via_columns(cls, value: Any) -> list[str]:
        return _normalize_string_list(value)


class DashboardChatChartMetricSpec(BaseModel):
    """Metric semantics used by one chart."""

    model_config = ConfigDict(extra="ignore")

    column: str = ""
    aggregation: str = ""
    alias: str = ""


class DashboardChatChartFilterSpec(BaseModel):
    """Filter semantics used by one chart."""

    model_config = ConfigDict(extra="ignore")

    column: str = ""
    operator: str = ""
    value: str = ""


class DashboardChatMetadataChartUsage(BaseModel):
    """Chart linkage and semantics for one table."""

    model_config = ConfigDict(extra="ignore")

    chart_id: int | None = None
    chart_title: str = ""
    relation: str = "direct"
    chart_type: str = ""
    metrics: list[DashboardChatChartMetricSpec] = Field(default_factory=list)
    dimensions: list[str] = Field(default_factory=list)
    filters: list[DashboardChatChartFilterSpec] = Field(default_factory=list)
    time_column: str | None = None

    @field_validator("dimensions", mode="before")
    @classmethod
    def _normalize_dimensions(cls, value: Any) -> list[str]:
        return _normalize_string_list(value)


class DashboardChatTableStatistics(BaseModel):
    """Observed table-level statistics."""

    model_config = ConfigDict(extra="ignore")

    row_count: int | None = None
    column_count: int = 0
    distinct_counts: dict[str, int | None] = Field(default_factory=dict)


class DashboardChatTableGrain(BaseModel):
    """Structured grain metadata for one table."""

    model_config = ConfigDict(extra="ignore")

    row_definition: str = ""
    natural_keys: list[str] = Field(default_factory=list)
    candidate_unique_id_columns: list[str] = Field(default_factory=list)
    evidence: list[str] = Field(default_factory=list)

    @field_validator("natural_keys", "candidate_unique_id_columns", "evidence", mode="before")
    @classmethod
    def _normalize_lists(cls, value: Any) -> list[str]:
        return _normalize_string_list(value)


class DashboardChatTableTemporal(BaseModel):
    """Structured temporal guidance for one table."""

    model_config = ConfigDict(extra="ignore")

    primary_filter_time_column: str = ""
    time_column_meanings: dict[str, str] = Field(default_factory=dict)


class DashboardChatTableCounting(BaseModel):
    """Structured counting guidance for one table."""

    model_config = ConfigDict(extra="ignore")

    default_row_count_entity: str = ""
    entity_counting_guidance: dict[str, str] = Field(default_factory=dict)


class DashboardChatActionFilterRule(BaseModel):
    """Rule for counting entities who actually performed an action."""

    model_config = ConfigDict(extra="ignore")

    action: str = ""
    entity: str = ""
    required_conditions: list[str] = Field(default_factory=list)

    @field_validator("required_conditions", mode="before")
    @classmethod
    def _normalize_required_conditions(cls, value: Any) -> list[str]:
        return _normalize_string_list(value)


class DashboardChatAnswerabilityLimitation(BaseModel):
    """One structured answerability limitation."""

    model_config = ConfigDict(extra="ignore")

    question_need: str = ""
    resolution: str = ""
    details: str = ""


class DashboardChatTableAnswerability(BaseModel):
    """Structured answerability metadata for one table."""

    model_config = ConfigDict(extra="ignore")

    retained_dimensions: list[str] = Field(default_factory=list)
    rolled_up_over: list[str] = Field(default_factory=list)
    comparison_axes_available: list[str] = Field(default_factory=list)
    direct_answer_capabilities: list[str] = Field(default_factory=list)
    answerability_limitations: list[DashboardChatAnswerabilityLimitation] = Field(
        default_factory=list
    )
    action_filter_rules: list[DashboardChatActionFilterRule] = Field(default_factory=list)

    @field_validator(
        "retained_dimensions",
        "rolled_up_over",
        "comparison_axes_available",
        "direct_answer_capabilities",
        mode="before",
    )
    @classmethod
    def _normalize_string_lists(cls, value: Any) -> list[str]:
        return _normalize_string_list(value)


class DashboardChatMetadataTable(BaseModel):
    """Structured metadata for one allowlisted table."""

    model_config = ConfigDict(extra="ignore")

    table_name: str
    dbt_unique_id: str = ""
    schema_name: str = ""
    model_name: str = ""
    layer: str = ""
    table_type: str = ""
    description: str = ""
    upstream_models: list[str] = Field(default_factory=list)
    chart_usage: list[DashboardChatMetadataChartUsage] = Field(default_factory=list)
    statistics: DashboardChatTableStatistics = Field(default_factory=DashboardChatTableStatistics)
    primary_entities: list[str] = Field(default_factory=list)
    grain: DashboardChatTableGrain = Field(default_factory=DashboardChatTableGrain)
    temporal: DashboardChatTableTemporal = Field(default_factory=DashboardChatTableTemporal)
    counting: DashboardChatTableCounting = Field(default_factory=DashboardChatTableCounting)
    answerability: DashboardChatTableAnswerability = Field(
        default_factory=DashboardChatTableAnswerability
    )
    columns: list[DashboardChatMetadataColumn] = Field(default_factory=list)

    @model_validator(mode="before")
    @classmethod
    def _flatten_legacy_shape(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "observed" not in value and "inferred" not in value:
            return value

        observed = value.get("observed") or {}
        inferred = value.get("inferred") or {}
        unique_ids = _normalize_string_list(observed.get("unique_ids"))
        raw_statistics = dict(observed.get("statistics") or {})
        distinct_counts = dict(raw_statistics.get("distinct_counts") or {})
        return {
            "table_name": value.get("table_name") or "",
            "dbt_unique_id": value.get("dbt_unique_id") or (unique_ids[0] if unique_ids else ""),
            "schema_name": value.get("schema_name") or observed.get("schema_name") or "",
            "model_name": value.get("model_name") or observed.get("model_name") or "",
            "layer": value.get("layer") or observed.get("layer") or "",
            "table_type": value.get("table_type") or inferred.get("table_type") or "",
            "description": value.get("description")
            or inferred.get("table_purpose")
            or observed.get("table_description")
            or "",
            "upstream_models": value.get("upstream_models") or [],
            "chart_usage": value.get("chart_usage") or observed.get("chart_usage") or [],
            "statistics": {
                "row_count": value.get("statistics", {}).get("row_count")
                if isinstance(value.get("statistics"), dict)
                else None,
                "column_count": value.get("statistics", {}).get("column_count")
                if isinstance(value.get("statistics"), dict)
                else 0,
                "distinct_counts": value.get("statistics", {}).get("distinct_counts")
                if isinstance(value.get("statistics"), dict)
                else {},
            }
            if "statistics" in value
            else {
                "row_count": observed.get("total_row_count") or raw_statistics.get("total_row_count"),
                "column_count": observed.get("column_count") or 0,
                "distinct_counts": distinct_counts,
            },
            "primary_entities": value.get("primary_entities") or inferred.get("primary_entities") or [],
            "grain": value.get("grain")
            or {
                "row_definition": inferred.get("row_grain") or "",
                "natural_keys": inferred.get("natural_keys") or [],
                "candidate_unique_id_columns": inferred.get("candidate_unique_id_columns") or [],
                "evidence": inferred.get("evidence") or [],
            },
            "temporal": value.get("temporal")
            or {
                "primary_filter_time_column": (
                    (inferred.get("primary_time_columns") or [""])[0]
                    if inferred.get("primary_time_columns")
                    else ""
                ),
                "time_column_meanings": {},
            },
            "counting": value.get("counting")
            or {
                "default_row_count_entity": "",
                "entity_counting_guidance": inferred.get("entity_counting_guidance") or {},
            },
            "answerability": value.get("answerability") or {},
            "columns": value.get("columns") or [],
        }

    @field_validator("upstream_models", "primary_entities", mode="before")
    @classmethod
    def _normalize_lists(cls, value: Any) -> list[str]:
        return _normalize_string_list(value)

    @property
    def row_grain(self) -> str:
        return self.grain.row_definition

    @property
    def natural_keys(self) -> list[str]:
        return self.grain.natural_keys

    @property
    def candidate_unique_id_columns(self) -> list[str]:
        return self.grain.candidate_unique_id_columns

    @property
    def primary_time_columns(self) -> list[str]:
        return (
            [self.temporal.primary_filter_time_column]
            if self.temporal.primary_filter_time_column
            else []
        )

    @property
    def total_row_count(self) -> int | None:
        return self.statistics.row_count


class DashboardChatChartRegistryEntry(BaseModel):
    """Compact always-on chart registry entry."""

    model_config = ConfigDict(extra="ignore")

    chart_id: int
    title: str
    section: str = ""
    chart_type: str = ""
    description: str = ""
    preferred_table: str = ""
    metrics: list[DashboardChatChartMetricSpec] = Field(default_factory=list)
    metric_columns: list[str] = Field(default_factory=list)
    dimension_columns: list[str] = Field(default_factory=list)
    filters: list[DashboardChatChartFilterSpec] = Field(default_factory=list)
    time_column: str | None = None

    @field_validator("metric_columns", "dimension_columns", mode="before")
    @classmethod
    def _normalize_chart_lists(cls, value: Any) -> list[str]:
        return _normalize_string_list(value)


class DashboardChatMetadataArtifactPayload(BaseModel):
    """Persisted dashboard-scoped metadata artifact payload."""

    model_config = ConfigDict(extra="ignore")

    schema_version: int = DASHBOARD_CHAT_METADATA_SCHEMA_VERSION
    dashboard_id: int
    org_id: int
    dashboard_title: str = ""
    dashboard_description: str = ""
    built_at: str | None = None
    source_fingerprint: str = ""
    allowlisted_tables: list[str] = Field(default_factory=list)
    chart_table_map: dict[str, list[str]] = Field(default_factory=dict)
    tables: list[DashboardChatMetadataTable] = Field(default_factory=list)
    join_paths: list[DashboardChatMetadataJoinPath] = Field(default_factory=list)

    @field_validator("allowlisted_tables", mode="before")
    @classmethod
    def _normalize_allowlisted_tables(cls, value: Any) -> list[str]:
        return _normalize_string_list(value)

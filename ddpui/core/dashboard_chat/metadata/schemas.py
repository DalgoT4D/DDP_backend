"""Pydantic schemas for dashboard chat metadata artifacts."""

from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator


DASHBOARD_CHAT_METADATA_SCHEMA_VERSION = 2


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


class DashboardChatObservedColumn(BaseModel):
    """Observed physical facts for one table column."""

    model_config = ConfigDict(extra="ignore")

    name: str
    data_type: str = ""
    description: str = ""
    nullable: bool | None = None
    null_percentage: float | None = None
    distinct_count: int | None = None
    sample_values: list[str] = Field(default_factory=list)
    numeric_min: float | None = None
    numeric_max: float | None = None
    time_min: str | None = None
    time_max: str | None = None

    @field_validator("sample_values", mode="before")
    @classmethod
    def _normalize_list_fields(cls, value: Any) -> list[str]:
        return _normalize_string_list(value)


class DashboardChatInferredColumn(BaseModel):
    """LLM-inferred semantic interpretation for one table column."""

    model_config = ConfigDict(extra="ignore")

    semantic_role: str = ""
    entity_tags: list[str] = Field(default_factory=list)
    measure_tags: list[str] = Field(default_factory=list)
    pii: bool = False
    pii_type: str = ""
    aggregation_hints: list[str] = Field(default_factory=list)
    filter_usefulness: str = ""
    join_usefulness: str = ""
    ambiguity_notes: list[str] = Field(default_factory=list)
    confidence: float | None = None
    evidence: list[str] = Field(default_factory=list)

    @field_validator(
        "entity_tags",
        "measure_tags",
        "aggregation_hints",
        "ambiguity_notes",
        "evidence",
        mode="before",
    )
    @classmethod
    def _normalize_list_fields(cls, value: Any) -> list[str]:
        return _normalize_string_list(value)


class DashboardChatMetadataColumn(BaseModel):
    """Structured metadata for one table column."""

    model_config = ConfigDict(extra="ignore")

    observed: DashboardChatObservedColumn
    inferred: DashboardChatInferredColumn = Field(default_factory=DashboardChatInferredColumn)

    @property
    def name(self) -> str:
        return self.observed.name

    @property
    def data_type(self) -> str:
        return self.observed.data_type

    @property
    def description(self) -> str:
        return self.observed.description

    @property
    def nullable(self) -> bool | None:
        return self.observed.nullable

    @property
    def null_percentage(self) -> float | None:
        return self.observed.null_percentage

    @property
    def distinct_count(self) -> int | None:
        return self.observed.distinct_count

    @property
    def sample_values(self) -> list[str]:
        return self.observed.sample_values

    @property
    def numeric_min(self) -> float | None:
        return self.observed.numeric_min

    @property
    def numeric_max(self) -> float | None:
        return self.observed.numeric_max

    @property
    def time_min(self) -> str | None:
        return self.observed.time_min

    @property
    def time_max(self) -> str | None:
        return self.observed.time_max

    @property
    def semantic_role(self) -> str:
        return self.inferred.semantic_role

    @property
    def entity_tags(self) -> list[str]:
        return self.inferred.entity_tags

    @property
    def measure_tags(self) -> list[str]:
        return self.inferred.measure_tags

    @property
    def pii(self) -> bool:
        return self.inferred.pii

    @property
    def pii_type(self) -> str:
        return self.inferred.pii_type

    @property
    def aggregation_hints(self) -> list[str]:
        return self.inferred.aggregation_hints

    @property
    def filter_usefulness(self) -> str:
        return self.inferred.filter_usefulness

    @property
    def join_usefulness(self) -> str:
        return self.inferred.join_usefulness

    @property
    def ambiguity_notes(self) -> list[str]:
        return self.inferred.ambiguity_notes


class DashboardChatMetadataJoinPath(BaseModel):
    """One inferred candidate join path between two allowlisted tables."""

    model_config = ConfigDict(extra="ignore")

    source_table: str
    target_table: str
    via_columns: list[str] = Field(default_factory=list)
    cardinality: str = ""
    confidence: float | None = None
    preferred: bool = False
    dashboard_relevant: bool = True
    required_for_entity_names: bool = False
    required_for_metrics: bool = False

    @field_validator("via_columns", mode="before")
    @classmethod
    def _normalize_via_columns(cls, value: Any) -> list[str]:
        return _normalize_string_list(value)


class DashboardChatMetadataChartUsage(BaseModel):
    """Chart linkage for one table."""

    model_config = ConfigDict(extra="ignore")

    chart_id: int | None = None
    chart_title: str = ""
    relation: str = "direct"


class DashboardChatObservedTable(BaseModel):
    """Observed physical facts for one allowlisted table."""

    model_config = ConfigDict(extra="ignore")

    unique_ids: list[str] = Field(default_factory=list)
    layer: str = ""
    schema_name: str = ""
    model_name: str = ""
    human_label: str = ""
    table_description: str = ""
    time_coverage: dict[str, Any] = Field(default_factory=dict)
    total_row_count: int | None = None
    approximate_size_hint: str = ""
    column_count: int = 0
    chart_usage: list[DashboardChatMetadataChartUsage] = Field(default_factory=list)
    statistics: dict[str, Any] = Field(default_factory=dict)

    @field_validator("unique_ids", mode="before")
    @classmethod
    def _normalize_unique_ids(cls, value: Any) -> list[str]:
        return _normalize_string_list(value)


class DashboardChatInferredTable(BaseModel):
    """LLM-inferred semantic interpretation for one allowlisted table."""

    model_config = ConfigDict(extra="ignore")

    table_purpose: str = ""
    table_type: str = ""
    row_grain: str = ""
    row_grain_confidence: float | None = None
    primary_entities: list[str] = Field(default_factory=list)
    natural_keys: list[str] = Field(default_factory=list)
    natural_key_confidence: float | None = None
    candidate_unique_id_columns: list[str] = Field(default_factory=list)
    primary_time_columns: list[str] = Field(default_factory=list)
    primary_time_confidence: float | None = None
    preferred_use_cases: list[str] = Field(default_factory=list)
    anti_pattern_use_cases: list[str] = Field(default_factory=list)
    example_questions: list[str] = Field(default_factory=list)
    entity_counting_guidance: dict[str, str] = Field(default_factory=dict)
    grain_warnings: list[str] = Field(default_factory=list)
    required_join_patterns: list[str] = Field(default_factory=list)
    ambiguity_notes: list[str] = Field(default_factory=list)
    evidence: list[str] = Field(default_factory=list)

    @field_validator(
        "primary_entities",
        "natural_keys",
        "candidate_unique_id_columns",
        "primary_time_columns",
        "preferred_use_cases",
        "anti_pattern_use_cases",
        "example_questions",
        "grain_warnings",
        "required_join_patterns",
        "ambiguity_notes",
        "evidence",
        mode="before",
    )
    @classmethod
    def _normalize_lists(cls, value: Any) -> list[str]:
        return _normalize_string_list(value)


class DashboardChatMetadataTable(BaseModel):
    """Structured metadata for one allowlisted table."""

    model_config = ConfigDict(extra="ignore")

    table_name: str
    observed: DashboardChatObservedTable
    inferred: DashboardChatInferredTable = Field(default_factory=DashboardChatInferredTable)
    columns: list[DashboardChatMetadataColumn] = Field(default_factory=list)

    @property
    def unique_ids(self) -> list[str]:
        return self.observed.unique_ids

    @property
    def layer(self) -> str:
        return self.observed.layer

    @property
    def schema_name(self) -> str:
        return self.observed.schema_name

    @property
    def model_name(self) -> str:
        return self.observed.model_name

    @property
    def human_label(self) -> str:
        return self.observed.human_label

    @property
    def table_description(self) -> str:
        return self.observed.table_description

    @property
    def time_coverage(self) -> dict[str, Any]:
        return self.observed.time_coverage

    @property
    def total_row_count(self) -> int | None:
        return self.observed.total_row_count

    @property
    def approximate_size_hint(self) -> str:
        return self.observed.approximate_size_hint

    @property
    def column_count(self) -> int:
        return self.observed.column_count

    @property
    def chart_usage(self) -> list[DashboardChatMetadataChartUsage]:
        return self.observed.chart_usage

    @property
    def statistics(self) -> dict[str, Any]:
        return self.observed.statistics

    @property
    def table_purpose(self) -> str:
        return self.inferred.table_purpose

    @property
    def table_type(self) -> str:
        return self.inferred.table_type

    @property
    def row_grain(self) -> str:
        return self.inferred.row_grain

    @property
    def primary_entities(self) -> list[str]:
        return self.inferred.primary_entities

    @property
    def natural_keys(self) -> list[str]:
        return self.inferred.natural_keys

    @property
    def candidate_unique_id_columns(self) -> list[str]:
        return self.inferred.candidate_unique_id_columns

    @property
    def primary_time_columns(self) -> list[str]:
        return self.inferred.primary_time_columns

    @property
    def preferred_use_cases(self) -> list[str]:
        return self.inferred.preferred_use_cases

    @property
    def anti_pattern_use_cases(self) -> list[str]:
        return self.inferred.anti_pattern_use_cases

    @property
    def example_questions(self) -> list[str]:
        return self.inferred.example_questions

    @property
    def entity_counting_guidance(self) -> dict[str, str]:
        return self.inferred.entity_counting_guidance

    @property
    def grain_warnings(self) -> list[str]:
        return self.inferred.grain_warnings

    @property
    def required_join_patterns(self) -> list[str]:
        return self.inferred.required_join_patterns

    @property
    def ambiguity_notes(self) -> list[str]:
        return self.inferred.ambiguity_notes


class DashboardChatChartRegistryEntry(BaseModel):
    """Compact always-on chart registry entry."""

    model_config = ConfigDict(extra="ignore")

    chart_id: int
    title: str
    section: str = ""
    chart_type: str = ""
    description: str = ""
    preferred_table: str = ""
    metric_columns: list[str] = Field(default_factory=list)
    dimension_columns: list[str] = Field(default_factory=list)
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
    entity_index: dict[str, list[str]] = Field(default_factory=dict)
    measure_index: dict[str, list[str]] = Field(default_factory=dict)
    column_index: dict[str, list[str]] = Field(default_factory=dict)

    @field_validator("allowlisted_tables", mode="before")
    @classmethod
    def _normalize_allowlisted_tables(cls, value: Any) -> list[str]:
        return _normalize_string_list(value)

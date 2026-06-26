"""Deterministic metadata exploration helpers for dashboard chat."""

from __future__ import annotations

import re
from typing import Any

from ddpui.core.dashboard_chat.metadata.schemas import (
    DashboardChatChartRegistryEntry,
    DashboardChatMetadataArtifactPayload,
    DashboardChatMetadataJoinPath,
    DashboardChatMetadataTable,
)

SEARCH_TOKEN_PATTERN = re.compile(r"[A-Za-z0-9_]+")


def tokenize(text: str | None) -> set[str]:
    """Tokenize free text into normalized search tokens."""
    if not text:
        return set()
    tokens: set[str] = set()
    for raw_token in SEARCH_TOKEN_PATTERN.findall(text.lower()):
        for token in raw_token.split("_"):
            cleaned = token.strip()
            if not cleaned:
                continue
            tokens.add(cleaned)
            if len(cleaned) > 3 and cleaned.endswith("s"):
                tokens.add(cleaned[:-1])
    return tokens


def table_lookup(
    artifact: DashboardChatMetadataArtifactPayload,
) -> dict[str, DashboardChatMetadataTable]:
    """Return a table lookup keyed by physical table name."""
    return {table.table_name: table for table in artifact.tables}


def chart_registry_lookup(chart_registry: list[dict[str, Any]]) -> dict[int, DashboardChatChartRegistryEntry]:
    """Return chart entries keyed by chart id."""
    parsed = [DashboardChatChartRegistryEntry.model_validate(entry) for entry in chart_registry]
    return {entry.chart_id: entry for entry in parsed}


def search_metadata_tables(
    artifact: DashboardChatMetadataArtifactPayload,
    *,
    question_terms: list[str] | None = None,
    entity_terms: list[str] | None = None,
    measure_terms: list[str] | None = None,
    grain_terms: list[str] | None = None,
    time_terms: list[str] | None = None,
    question_type: str | None = None,
    required_output_shape: str | None = None,
    limit: int = 8,
) -> list[dict[str, Any]]:
    """Rank tables against a structured query brief."""
    question_tokens = set(tokenize(" ".join(question_terms or [])))
    entity_tokens = set(tokenize(" ".join(entity_terms or [])))
    measure_tokens = set(tokenize(" ".join(measure_terms or [])))
    grain_tokens = set(tokenize(" ".join(grain_terms or [])))
    time_tokens = set(tokenize(" ".join(time_terms or [])))
    shape_tokens = tokenize(required_output_shape or "")
    type_tokens = tokenize(question_type or "")
    ranked: list[tuple[int, dict[str, Any]]] = []
    for table in artifact.tables:
        haystack = " ".join(
            [
                table.table_name,
                table.model_name,
                table.description,
                table.row_grain,
                " ".join(table.primary_entities),
                " ".join(table.upstream_models),
                " ".join(table.temporal.time_column_meanings.values()),
                " ".join(table.counting.entity_counting_guidance.values()),
                " ".join(table.answerability.retained_dimensions),
                " ".join(table.answerability.rolled_up_over),
                " ".join(table.answerability.comparison_axes_available),
                " ".join(table.answerability.direct_answer_capabilities),
                " ".join(
                    limitation.question_need
                    + " "
                    + limitation.resolution
                    + " "
                    + limitation.details
                    for limitation in table.answerability.answerability_limitations
                ),
                " ".join(col.name for col in table.columns),
                " ".join(col.description for col in table.columns),
                " ".join(col.semantic_role for col in table.columns),
                " ".join(col.value_semantics for col in table.columns),
            ]
        )
        haystack_tokens = tokenize(haystack)
        score = 0
        reasons: list[str] = []

        if entity_tokens:
            overlap = entity_tokens & haystack_tokens
            score += 5 * len(overlap)
            if overlap:
                reasons.append(f"entity match: {', '.join(sorted(overlap))}")
        if question_tokens:
            overlap = question_tokens & haystack_tokens
            score += 2 * len(overlap)
            if overlap:
                reasons.append(f"question match: {', '.join(sorted(overlap))}")
        if measure_tokens:
            overlap = measure_tokens & haystack_tokens
            score += 6 * len(overlap)
            if overlap:
                reasons.append(f"measure match: {', '.join(sorted(overlap))}")
        if grain_tokens:
            overlap = grain_tokens & haystack_tokens
            score += 3 * len(overlap)
            if overlap:
                reasons.append(f"grain match: {', '.join(sorted(overlap))}")
        if time_tokens:
            overlap = time_tokens & haystack_tokens
            score += 2 * len(overlap)
            if overlap:
                reasons.append(f"time match: {', '.join(sorted(overlap))}")
        if shape_tokens:
            score += 2 * len(shape_tokens & haystack_tokens)
        if type_tokens:
            score += 2 * len(type_tokens & haystack_tokens)
        if table.table_type in {"fact", "row_grain"}:
            score += 2
        if any(
            limitation.resolution == "requires_join"
            for limitation in table.answerability.answerability_limitations
        ):
            score += 1
        if score <= 0:
            continue
        ranked.append(
            (
                score,
                {
                    "table_name": table.table_name,
                    "model_name": table.model_name,
                    "layer": table.layer,
                    "row_grain": table.row_grain,
                    "table_type": table.table_type,
                    "description": table.description,
                    "reasons": reasons,
                },
            )
        )

    ranked.sort(key=lambda item: (-item[0], item[1]["table_name"]))
    return [payload for _, payload in ranked[:limit]]


def search_columns_by_name(
    artifact: DashboardChatMetadataArtifactPayload,
    *,
    column_name: str,
    limit: int = 20,
) -> list[dict[str, Any]]:
    """Find every allowlisted table that contains a matching column."""
    if not column_name.strip():
        return []
    wanted_tokens = tokenize(column_name)
    matches: list[tuple[int, dict[str, Any]]] = []
    for table in artifact.tables:
        for column in table.columns:
            name_tokens = tokenize(column.name)
            overlap = wanted_tokens & name_tokens
            exact_name_match = column.name.lower() == column_name.lower()
            token_subset_match = bool(wanted_tokens) and wanted_tokens.issubset(name_tokens)
            if not exact_name_match and not token_subset_match:
                continue
            score = len(overlap) + (5 if exact_name_match else 0)
            matches.append(
                (
                    score,
                    {
                        "table_name": table.table_name,
                        "column_name": column.name,
                        "data_type": column.data_type,
                        "description": column.description,
                        "semantic_role": column.semantic_role,
                        "value_semantics": column.value_semantics,
                        "pii": column.pii,
                        "table_row_grain": table.row_grain,
                        "table_type": table.table_type,
                    },
                )
            )
    matches.sort(key=lambda item: (-item[0], item[1]["table_name"], item[1]["column_name"]))
    return [payload for _, payload in matches[:limit]]


def get_related_tables(
    artifact: DashboardChatMetadataArtifactPayload,
    *,
    table_names: list[str],
    entity_terms: list[str] | None = None,
    measure_terms: list[str] | None = None,
) -> list[dict[str, Any]]:
    """Return related tables connected through the join graph."""
    requested = set(table_names)
    entity_tokens = set(tokenize(" ".join(entity_terms or [])))
    measure_tokens = set(tokenize(" ".join(measure_terms or [])))
    tables_by_name = table_lookup(artifact)
    related: dict[str, dict[str, Any]] = {}

    for join_path in artifact.join_paths:
        if join_path.source_table not in requested and join_path.target_table not in requested:
            continue
        other_table = (
            join_path.target_table
            if join_path.source_table in requested
            else join_path.source_table
        )
        table = tables_by_name.get(other_table)
        if table is None:
            continue
        haystack = tokenize(
            " ".join(
                [
                    table.table_name,
                    table.description,
                    " ".join(table.primary_entities),
                    " ".join(col.name for col in table.columns),
                    " ".join(col.description for col in table.columns),
                    " ".join(col.value_semantics for col in table.columns),
                ]
            )
        )
        score = len(entity_tokens & haystack) * 4 + len(measure_tokens & haystack) * 5
        existing = related.get(other_table)
        payload = {
            "table_name": other_table,
            "row_grain": table.row_grain,
            "table_type": table.table_type,
            "join_columns": join_path.via_columns,
            "join_cardinality": join_path.cardinality,
            "preferred": join_path.preferred,
        }
        if existing is None or score > existing.get("_score", -1):
            payload["_score"] = score
            related[other_table] = payload

    ranked = sorted(related.values(), key=lambda item: (-item["_score"], item["table_name"]))
    for item in ranked:
        item.pop("_score", None)
    return ranked


def join_paths_for_tables(
    artifact: DashboardChatMetadataArtifactPayload,
    *,
    table_names: list[str],
) -> list[DashboardChatMetadataJoinPath]:
    """Return join paths touching any of the provided tables."""
    requested = set(table_names)
    existing_paths = [
        join_path
        for join_path in artifact.join_paths
        if join_path.source_table in requested or join_path.target_table in requested
    ]
    return _with_inferred_natural_key_join_paths(
        artifact=artifact,
        requested=requested,
        existing_paths=existing_paths,
    )


def _with_inferred_natural_key_join_paths(
    *,
    artifact: DashboardChatMetadataArtifactPayload,
    requested: set[str],
    existing_paths: list[DashboardChatMetadataJoinPath],
) -> list[DashboardChatMetadataJoinPath]:
    """Add obvious natural-key joins for older artifacts missing derived join paths."""
    if not requested:
        return existing_paths

    paths = list(existing_paths)
    existing_keys = {
        (
            path.source_table,
            path.target_table,
            tuple(path.via_columns),
        )
        for path in paths
    }
    tables = artifact.tables
    for source in tables:
        if source.table_name not in requested:
            continue
        source_keys = _join_key_candidates(source)
        if not source_keys:
            continue
        for target in tables:
            if source.table_name == target.table_name:
                continue
            target_keys = _join_key_candidates(target)
            shared_keys = sorted(source_keys & target_keys)
            if not shared_keys:
                continue
            path_key = (source.table_name, target.table_name, tuple(shared_keys[:3]))
            if path_key in existing_keys:
                continue
            existing_keys.add(path_key)
            paths.append(
                DashboardChatMetadataJoinPath(
                    source_table=source.table_name,
                    target_table=target.table_name,
                    via_columns=shared_keys[:3],
                    cardinality=_infer_join_cardinality_from_statistics(source, target, shared_keys[0]),
                    preferred=True,
                    dashboard_relevant=True,
                    required_for_entity_names=any("name" in column.column_name.lower() for column in target.columns),
                    required_for_metrics=any(column.semantic_role == "metric" for column in target.columns),
                )
            )
    return paths


def _join_key_candidates(table: DashboardChatMetadataTable) -> set[str]:
    return set(table.candidate_unique_id_columns) | set(table.natural_keys)


def _infer_join_cardinality_from_statistics(
    source: DashboardChatMetadataTable,
    target: DashboardChatMetadataTable,
    shared_column: str,
) -> str:
    source_rows = source.total_row_count
    target_rows = target.total_row_count
    source_distinct = source.statistics.distinct_counts.get(shared_column)
    target_distinct = target.statistics.distinct_counts.get(shared_column)
    if None in {source_rows, target_rows, source_distinct, target_distinct}:
        return "unknown"
    if source_distinct == source_rows and target_distinct == target_rows:
        return "one_to_one"
    if source_distinct < source_rows and target_distinct == target_rows:
        return "many_to_one"
    if source_distinct == source_rows and target_distinct < target_rows:
        return "one_to_many"
    if source_distinct < source_rows and target_distinct < target_rows:
        return "many_to_many"
    return "unknown"

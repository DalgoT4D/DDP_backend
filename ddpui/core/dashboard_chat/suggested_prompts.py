from __future__ import annotations

import re
from collections.abc import Iterable


TIME_TOKENS = (
    "quarter",
    "quarterly",
    "month",
    "monthly",
    "year",
    "yearly",
    "week",
    "weekly",
    "day",
    "daily",
    "date",
    "time",
    "period",
)

ENTITY_LABELS = {
    "district": "districts",
    "facilitator": "facilitators",
    "school": "schools",
    "state": "states",
    "program": "programs",
    "block": "blocks",
    "ward": "wards",
    "village": "villages",
    "student": "students",
    "learner": "learners",
    "teacher": "teachers",
    "partner": "partners",
    "organization": "organizations",
    "org": "organizations",
}

METRIC_PREFIX_PATTERNS = (
    ("count_distinct_", "unique"),
    ("count_", "number of"),
    ("avg_", "average"),
    ("average_", "average"),
    ("sum_", ""),
    ("total_", "total"),
    ("max_", "highest"),
    ("min_", "lowest"),
)


def _normalize_text(parts: Iterable[str | None]) -> str:
    return " ".join(part.strip() for part in parts if part and part.strip())


def _humanize_identifier(value: str | None) -> str:
    if not value:
        return ""

    normalized_value = re.sub(r"[_\.]+", " ", str(value).strip().lower())
    normalized_value = re.sub(r"\b(label|name|id)\b", "", normalized_value)
    normalized_value = re.sub(r"\s+", " ", normalized_value).strip()
    return normalized_value


def _pluralize_label(label: str) -> str:
    normalized_label = label.strip().lower()
    if not normalized_label:
        return "categories"
    if normalized_label.endswith("ies") or normalized_label.endswith("s"):
        return normalized_label
    if normalized_label.endswith("y") and normalized_label[-2:-1] not in {"a", "e", "i", "o", "u"}:
        return f"{normalized_label[:-1]}ies"
    return f"{normalized_label}s"


def _looks_time_like(value: str | None) -> bool:
    normalized_value = _humanize_identifier(value)
    return any(token in normalized_value for token in TIME_TOKENS)


def _search_entity_label(text: str) -> str | None:
    normalized_text = text.lower()
    for token, label in ENTITY_LABELS.items():
        if token in normalized_text:
            return label
    return None


def _metric_label_from_string(metric_name: str | None) -> str:
    normalized_metric_name = _humanize_identifier(metric_name)
    if not normalized_metric_name:
        return ""

    for prefix, prefix_label in METRIC_PREFIX_PATTERNS:
        if normalized_metric_name.startswith(prefix.replace("_", " ")):
            suffix = normalized_metric_name.removeprefix(prefix.replace("_", " ")).strip()
            if prefix_label == "number of":
                return f"{prefix_label} {_pluralize_label(suffix)}"
            if prefix_label:
                return f"{prefix_label} {suffix}".strip()
            return suffix
    return normalized_metric_name


def _metric_label_from_chart(chart: dict) -> str:
    extra_config = chart.get("extra_config") or {}
    metrics = extra_config.get("metrics") or []
    for metric in metrics:
        if isinstance(metric, dict):
            alias = _humanize_identifier(metric.get("alias"))
            if alias:
                return alias
            column = metric.get("column")
            aggregation = _humanize_identifier(metric.get("aggregation"))
            if column:
                column_label = _humanize_identifier(column)
                if aggregation in {"avg", "average"}:
                    return f"average {column_label}"
                if aggregation == "count_distinct":
                    return f"unique {_pluralize_label(column_label)}"
                if aggregation == "count":
                    return f"number of {_pluralize_label(column_label)}"
                if aggregation:
                    return f"{aggregation} {column_label}".strip()
                return column_label
        elif isinstance(metric, str):
            metric_label = _metric_label_from_string(metric)
            if metric_label:
                return metric_label

    aggregate_column = extra_config.get("aggregate_column") or extra_config.get("value_column")
    aggregate_function = extra_config.get("aggregate_function")
    if aggregate_column:
        column_label = _humanize_identifier(aggregate_column)
        if aggregate_function == "avg":
            return f"average {column_label}"
        if aggregate_function == "count_distinct":
            return f"unique {_pluralize_label(column_label)}"
        if aggregate_function == "count":
            return f"number of {_pluralize_label(column_label)}"
        if aggregate_function in {"min", "max"}:
            return f"{aggregate_function} {column_label}"
        return column_label

    chart_title = str(chart.get("title") or "").strip()
    if chart_title:
        title_prefix = re.split(r"\b(by|over|across|vs)\b", chart_title, maxsplit=1, flags=re.IGNORECASE)[
            0
        ].strip()
        humanized_prefix = _humanize_identifier(title_prefix)
        if humanized_prefix:
            return humanized_prefix

    return _humanize_identifier(chart.get("table_name")) or "this metric"


def _dimension_label_from_chart(chart: dict, fallback_context: str) -> str | None:
    extra_config = chart.get("extra_config") or {}
    dimension_candidates = []

    for key in ("dimension_column", "extra_dimension_column", "geographic_column"):
        value = extra_config.get(key)
        if isinstance(value, str) and value.strip():
            dimension_candidates.append(value)

    dimensions = extra_config.get("dimensions") or []
    if isinstance(dimensions, list):
        dimension_candidates.extend(value for value in dimensions if isinstance(value, str) and value.strip())

    for candidate in dimension_candidates:
        if _looks_time_like(candidate):
            continue
        label = _humanize_identifier(candidate)
        if label:
            return _pluralize_label(label)

    return _search_entity_label(fallback_context)


def _time_label_from_chart(chart: dict, fallback_context: str) -> str | None:
    extra_config = chart.get("extra_config") or {}
    dimension_candidates = []

    for key in ("dimension_column", "extra_dimension_column"):
        value = extra_config.get(key)
        if isinstance(value, str) and value.strip():
            dimension_candidates.append(value)

    dimensions = extra_config.get("dimensions") or []
    if isinstance(dimensions, list):
        dimension_candidates.extend(value for value in dimensions if isinstance(value, str) and value.strip())

    for candidate in dimension_candidates:
        if _looks_time_like(candidate):
            return _humanize_identifier(candidate)

    normalized_context = fallback_context.lower()
    for token in TIME_TOKENS:
        if token in normalized_context:
            return token
    return None


def _uses_plural_verb(metric_label: str) -> bool:
    normalized_metric_label = metric_label.strip().lower()
    if normalized_metric_label.startswith(("number of ", "average ", "highest ", "lowest ", "total ")):
        return False
    if normalized_metric_label.endswith("ies"):
        return True
    return normalized_metric_label.endswith("s") and not normalized_metric_label.endswith("ss")


def _build_trend_prompt(chart_prompt_context: dict) -> str:
    metric_label = chart_prompt_context["metric_label"]
    time_label = chart_prompt_context["time_label"] or "time"
    verb = "have" if _uses_plural_verb(metric_label) else "has"
    if time_label == "time":
        return f"How {verb} {metric_label} changed over time?"
    return f"How {verb} {metric_label} changed by {time_label}?"


def _build_comparison_prompt(chart_prompt_context: dict) -> str:
    return (
        f'Which {chart_prompt_context["dimension_label"]} have the highest '
        f'{chart_prompt_context["metric_label"]}?'
    )


def _build_explanation_prompt(chart_prompt_context: dict) -> str:
    chart_title = chart_prompt_context["chart_title"]
    if chart_prompt_context["chart_type"] == "number":
        return f'What does the "{chart_title}" metric represent?'
    if chart_prompt_context["time_label"]:
        return f'What does the "{chart_title}" chart measure by {chart_prompt_context["time_label"]}?'
    if chart_prompt_context["dimension_label"]:
        return (
            f'What does the "{chart_title}" chart compare across '
            f'{chart_prompt_context["dimension_label"]}?'
        )
    return f'What does the "{chart_title}" chart measure?'


def _can_build_trend_prompt(chart_prompt_context: dict) -> bool:
    return bool(chart_prompt_context["time_label"] or chart_prompt_context["chart_type"] == "line")


def _can_build_comparison_prompt(chart_prompt_context: dict) -> bool:
    return bool(chart_prompt_context["dimension_label"] and chart_prompt_context["chart_type"] != "number")


def _build_chart_prompt_contexts(
    dashboard_export: dict,
    org_context_markdown: str,
    dashboard_context_markdown: str,
) -> list[dict]:
    dashboard = dashboard_export.get("dashboard") or {}
    charts = dashboard_export.get("charts") or []
    dashboard_title = str(dashboard.get("title") or "this dashboard").strip()
    dashboard_description = str(dashboard.get("description") or "").strip()
    shared_context = _normalize_text(
        [dashboard_title, dashboard_description, org_context_markdown, dashboard_context_markdown]
    )

    chart_prompt_contexts = []
    for chart in charts:
        chart_title = str(chart.get("title") or "").strip() or dashboard_title
        chart_description = str(chart.get("description") or "").strip()
        chart_context = _normalize_text(
            [
                chart_title,
                chart_description,
                str(chart.get("schema_name") or "").strip(),
                str(chart.get("table_name") or "").replace("_", " ").strip(),
                shared_context,
            ]
        )
        chart_prompt_contexts.append(
            {
                "chart_id": chart.get("id"),
                "chart_title": chart_title,
                "chart_type": str(chart.get("chart_type") or "").strip().lower(),
                "metric_label": _metric_label_from_chart(chart),
                "dimension_label": _dimension_label_from_chart(chart, chart_context),
                "time_label": _time_label_from_chart(chart, chart_context),
            }
        )

    return chart_prompt_contexts


def _select_prompt(
    chart_prompt_contexts: list[dict],
    used_chart_ids: set[int | None],
    *,
    prompt_builder,
    predicate,
) -> str | None:
    for prefer_unused_chart in (True, False):
        for chart_prompt_context in chart_prompt_contexts:
            chart_id = chart_prompt_context["chart_id"]
            if prefer_unused_chart and chart_id in used_chart_ids:
                continue
            if not predicate(chart_prompt_context):
                continue
            prompt = prompt_builder(chart_prompt_context)
            if not prompt:
                continue
            used_chart_ids.add(chart_id)
            return prompt
    return None


def build_dashboard_suggested_prompts(
    dashboard_export: dict,
    org_context_markdown: str,
    dashboard_context_markdown: str,
) -> list[str]:
    chart_prompt_contexts = _build_chart_prompt_contexts(
        dashboard_export=dashboard_export,
        org_context_markdown=org_context_markdown,
        dashboard_context_markdown=dashboard_context_markdown,
    )

    suggested_prompts: list[str] = []
    used_chart_ids: set[int | None] = set()

    trend_prompt = _select_prompt(
        chart_prompt_contexts,
        used_chart_ids,
        prompt_builder=_build_trend_prompt,
        predicate=_can_build_trend_prompt,
    )
    if trend_prompt:
        suggested_prompts.append(trend_prompt)

    comparison_prompt = _select_prompt(
        chart_prompt_contexts,
        used_chart_ids,
        prompt_builder=_build_comparison_prompt,
        predicate=_can_build_comparison_prompt,
    )
    if comparison_prompt and comparison_prompt not in suggested_prompts:
        suggested_prompts.append(comparison_prompt)

    explanation_prompt = _select_prompt(
        chart_prompt_contexts,
        used_chart_ids,
        prompt_builder=_build_explanation_prompt,
        predicate=lambda chart_prompt_context: True,
    )
    if explanation_prompt and explanation_prompt not in suggested_prompts:
        suggested_prompts.append(explanation_prompt)

    for chart_prompt_context in chart_prompt_contexts:
        if len(suggested_prompts) == 3:
            break
        if chart_prompt_context["chart_id"] in used_chart_ids:
            continue
        explanation_prompt = _build_explanation_prompt(chart_prompt_context)
        if explanation_prompt in suggested_prompts:
            continue
        used_chart_ids.add(chart_prompt_context["chart_id"])
        suggested_prompts.append(explanation_prompt)

    if suggested_prompts:
        return suggested_prompts[:3]

    dashboard_title = str((dashboard_export.get("dashboard") or {}).get("title") or "this dashboard").strip()
    return [
        f'What does the "{dashboard_title}" dashboard measure?',
        f'Which metrics stand out on the "{dashboard_title}" dashboard?',
        f'How are results grouped on the "{dashboard_title}" dashboard?',
    ]

from __future__ import annotations

import re


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

METRIC_PREFIX_PATTERNS = (
    ("count_distinct_", "unique"),
    ("count_", "number of"),
    ("avg_", "average"),
    ("average_", "average"),
    ("sum_", "total"),
    ("total_", "total"),
    ("max_", "highest"),
    ("min_", "lowest"),
)


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


def _metric_label_from_column(
    column_name: str | None,
    aggregation_name: str | None,
) -> str:
    column_label = _humanize_identifier(column_name)
    aggregation_label = _humanize_identifier(aggregation_name)

    if not column_label:
        return ""
    if not aggregation_label:
        return column_label
    if aggregation_label in {"avg", "average"}:
        return f"average {column_label}"
    if aggregation_label == "sum":
        return f"total {column_label}"
    if aggregation_label == "count_distinct":
        return f"unique {_pluralize_label(column_label)}"
    if aggregation_label == "count":
        return f"number of {_pluralize_label(column_label)}"
    if aggregation_label == "min":
        return f"lowest {column_label}"
    if aggregation_label == "max":
        return f"highest {column_label}"
    return f"{aggregation_label} of {column_label}"


def _metric_label_from_chart(chart: dict) -> str:
    extra_config = chart.get("extra_config") or {}
    metrics = extra_config.get("metrics") or []
    for metric in metrics:
        if isinstance(metric, dict):
            alias = _humanize_identifier(metric.get("alias"))
            if alias:
                return alias
            column = metric.get("column")
            if column:
                return _metric_label_from_column(column, metric.get("aggregation"))
        elif isinstance(metric, str):
            metric_label = _metric_label_from_string(metric)
            if metric_label:
                return metric_label

    aggregate_column = extra_config.get("aggregate_column") or extra_config.get("value_column")
    aggregate_function = extra_config.get("aggregate_function")
    if aggregate_column:
        metric_label = _metric_label_from_column(aggregate_column, aggregate_function)
        if metric_label:
            return metric_label

    chart_title = str(chart.get("title") or "").strip()
    if chart_title:
        title_prefix = re.split(r"\b(by|over|across|vs)\b", chart_title, maxsplit=1, flags=re.IGNORECASE)[
            0
        ].strip()
        humanized_prefix = _humanize_identifier(title_prefix)
        if humanized_prefix:
            return humanized_prefix

    return _humanize_identifier(chart.get("table_name")) or "this metric"


def _dimension_candidates_from_chart(chart: dict) -> list[str]:
    extra_config = chart.get("extra_config") or {}
    dimension_candidates: list[str] = []

    for key in ("dimension_column", "extra_dimension_column", "geographic_column"):
        value = extra_config.get(key)
        if isinstance(value, str) and value.strip():
            dimension_candidates.append(value)

    dimensions = extra_config.get("dimensions") or []
    if isinstance(dimensions, list):
        dimension_candidates.extend(
            value for value in dimensions if isinstance(value, str) and value.strip()
        )

    return dimension_candidates


def _dimension_label_from_chart(chart: dict) -> str | None:
    for candidate in _dimension_candidates_from_chart(chart):
        if _looks_time_like(candidate):
            continue
        label = _humanize_identifier(candidate)
        if label:
            return _pluralize_label(label)

    return None


def _time_label_from_chart(chart: dict) -> str | None:
    for candidate in _dimension_candidates_from_chart(chart):
        if _looks_time_like(candidate):
            return _humanize_identifier(candidate)
    return None


def _build_trend_prompt(chart_prompt_context: dict) -> str:
    metric_label = chart_prompt_context["metric_label"]
    time_label = chart_prompt_context["time_label"] or "time"
    if time_label == "time":
        return f"How did {metric_label} change over time?"
    return f"How did {metric_label} change by {time_label}?"


def _build_comparison_prompt(chart_prompt_context: dict) -> str:
    return (
        f'How does {chart_prompt_context["metric_label"]} compare across '
        f'{chart_prompt_context["dimension_label"]}?'
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
    return bool(chart_prompt_context["time_label"])


def _can_build_comparison_prompt(chart_prompt_context: dict) -> bool:
    return bool(chart_prompt_context["dimension_label"] and chart_prompt_context["chart_type"] != "number")


def _build_chart_prompt_contexts(
    dashboard_export: dict,
) -> list[dict]:
    dashboard = dashboard_export.get("dashboard") or {}
    charts = dashboard_export.get("charts") or []
    dashboard_title = str(dashboard.get("title") or "this dashboard").strip()

    chart_prompt_contexts = []
    for chart in reversed(charts):
        chart_title = str(chart.get("title") or "").strip() or dashboard_title
        chart_prompt_contexts.append(
            {
                "chart_id": chart.get("id"),
                "chart_title": chart_title,
                "chart_type": str(chart.get("chart_type") or "").strip().lower(),
                "metric_label": _metric_label_from_chart(chart),
                "dimension_label": _dimension_label_from_chart(chart),
                "time_label": _time_label_from_chart(chart),
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
    for chart_prompt_context in chart_prompt_contexts:
        chart_id = chart_prompt_context["chart_id"]
        if chart_id in used_chart_ids:
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
) -> list[str]:
    chart_prompt_contexts = _build_chart_prompt_contexts(
        dashboard_export=dashboard_export,
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
        predicate=lambda chart_prompt_context: bool(chart_prompt_context["chart_title"]),
    )
    if explanation_prompt and explanation_prompt not in suggested_prompts:
        suggested_prompts.append(explanation_prompt)

    for chart_prompt_context in chart_prompt_contexts:
        if len(suggested_prompts) == 3:
            break
        chart_id = chart_prompt_context["chart_id"]
        if chart_id in used_chart_ids:
            continue
        explanation_prompt = _build_explanation_prompt(chart_prompt_context)
        if explanation_prompt in suggested_prompts:
            continue
        used_chart_ids.add(chart_id)
        suggested_prompts.append(explanation_prompt)

    return suggested_prompts[:3]

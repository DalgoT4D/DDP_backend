"""Helpers for rendering alert messages from evaluated result rows."""

import json
import re
from typing import Any

from django.core.serializers.json import DjangoJSONEncoder

TOKEN_PATTERN = re.compile(r"{{\s*([A-Za-z_][A-Za-z0-9_]*)\s*}}")


def normalize_result_rows(rows: list[Any]) -> list[dict[str, Any]]:
    """Convert warehouse result rows into plain dicts."""
    normalized: list[dict[str, Any]] = []
    for row in rows:
        normalized.append(json.loads(json.dumps(dict(row), cls=DjangoJSONEncoder)))
    return normalized


def render_alert_message(
    *,
    alert_name: str,
    message: str,
    group_message: str,
    rows: list[dict[str, Any]],
    table_name: str,
    metric_name: str = "",
    group_by_column: str | None = None,
) -> str:
    """Render one outbound email body for the evaluated alert rows."""
    if not rows:
        return _render_template(
            message,
            _build_global_context(
                alert_name=alert_name,
                metric_name=metric_name,
                table_name=table_name,
                group_by_column=group_by_column,
                failing_group_count=0,
            ),
        )

    if not group_by_column:
        return _render_template(
            message,
            _build_row_context(
                alert_name=alert_name,
                metric_name=metric_name,
                table_name=table_name,
                group_by_column=group_by_column,
                failing_group_count=len(rows),
                row=rows[0],
            ),
        )

    rendered_intro = _render_template(
        message,
        _build_global_context(
            alert_name=alert_name,
            metric_name=metric_name,
            table_name=table_name,
            group_by_column=group_by_column,
            failing_group_count=len(rows),
        ),
    ).strip()

    template = group_message.strip() or _default_group_template()
    group_sections = [
        _render_template(
            template,
            _build_row_context(
                alert_name=alert_name,
                metric_name=metric_name,
                table_name=table_name,
                group_by_column=group_by_column,
                failing_group_count=len(rows),
                row=row,
            ),
        ).strip()
        for row in rows
    ]

    non_empty_sections = [section for section in group_sections if section]
    if rendered_intro and non_empty_sections:
        return f"{rendered_intro}\n\n" + "\n\n".join(non_empty_sections)
    if rendered_intro:
        return rendered_intro
    return "\n\n".join(non_empty_sections)


def _build_global_context(
    *,
    alert_name: str,
    metric_name: str,
    table_name: str,
    group_by_column: str | None,
    failing_group_count: int,
) -> dict[str, Any]:
    return {
        "alert_name": alert_name,
        "metric_name": metric_name,
        "table_name": table_name,
        "group_by_column": group_by_column or "",
        "failing_group_count": failing_group_count,
    }


def _build_row_context(
    *,
    alert_name: str,
    metric_name: str,
    table_name: str,
    group_by_column: str | None,
    failing_group_count: int,
    row: dict[str, Any],
) -> dict[str, Any]:
    context = _build_global_context(
        alert_name=alert_name,
        metric_name=metric_name,
        table_name=table_name,
        group_by_column=group_by_column,
        failing_group_count=failing_group_count,
    )
    context["alert_value"] = row.get("alert_value", "")
    context["group_by_value"] = row.get(group_by_column, "") if group_by_column else ""
    return context


def _render_template(template: str, context: dict[str, Any]) -> str:
    def replace_token(match: re.Match[str]) -> str:
        key = match.group(1)
        value = context.get(key, "")
        return "" if value is None else str(value)

    return TOKEN_PATTERN.sub(replace_token, template or "")


def _default_group_template() -> str:
    lines = [
        "{{group_by_column}}: {{group_by_value}}",
        "Alert value: {{alert_value}}",
    ]
    return "\n".join(lines)

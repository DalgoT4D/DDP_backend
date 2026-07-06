"""Alert message template rendering.

Spec calls for Mustache templates with a closed, per-alert-type token set. v1
ships with simple `{{token}}` substitution (no sections, no loops) — equivalent
to pystache for this constrained surface, and keeps the dependency footprint
small. Missing tokens are preserved as `{{token}}` so authors see which ones
still need data (matches the frontend live preview).

Token sets are intentionally hardcoded here AND mirrored in webapp_v2's
`types/alerts.ts` so the template editor can offer autocomplete chips.
"""

from __future__ import annotations

import re
from typing import Any, Mapping, Optional

from ddpui.core.charts.number_formatting import format_number_v2
from ddpui.models.alert import Alert, AlertType
from ddpui.schemas.alert_schema import StandaloneConfig


TOKENS_BY_TYPE = {
    AlertType.METRIC_THRESHOLD: ["alert_name", "metric_name", "target_value", "current_value"],
    AlertType.KPI_RAG: [
        "alert_name",
        "kpi_name",
        "target_value",
        "current_value",
        "rag_status",
    ],
    AlertType.STANDALONE: ["alert_name", "dataset_name", "target_value", "current_value"],
}


_TOKEN_RE = re.compile(r"\{\{\s*(\w+)\s*\}\}")


def render(template: str, values: Mapping[str, Any]) -> str:
    """Substitute `{{token}}` occurrences in template with values[token].

    Missing tokens (or values that are None / empty string) are left in place
    so the author can spot un-resolved variables in previews.
    """

    def replace(match: re.Match) -> str:
        token = match.group(1)
        if token not in values:
            return match.group(0)
        v = values[token]
        if v is None or v == "":
            return match.group(0)
        return str(v)

    return _TOKEN_RE.sub(replace, template or "")


def resolve_tokens(
    alert_type: str,
    *,
    alert_name: str,
    metric_name: Optional[str] = None,
    kpi_name: Optional[str] = None,
    dataset_name: Optional[str] = None,
    target_value: Any = None,
    current_value: Any = None,
    rag_status: Optional[str] = None,
) -> dict[str, Any]:
    """Build the {token: value} dict that `render()` consumes for an alert.

    Caller supplies the raw inputs; this function picks the relevant subset
    for the alert_type and returns a dict the renderer can substitute into a
    template.
    """
    base: dict[str, Any] = {"alert_name": alert_name}
    if alert_type == AlertType.METRIC_THRESHOLD:
        base.update(
            {
                "metric_name": metric_name,
                "target_value": target_value,
                "current_value": current_value,
            }
        )
    elif alert_type == AlertType.KPI_RAG:
        base.update(
            {
                "kpi_name": kpi_name,
                "target_value": target_value,
                "current_value": current_value,
                "rag_status": rag_status,
            }
        )
    elif alert_type == AlertType.STANDALONE:
        base.update(
            {
                "dataset_name": dataset_name,
                "target_value": target_value,
                "current_value": current_value,
            }
        )
    return base


def _format_via_kpi_customizations(value: Any, customizations: Mapping[str, Any]) -> Any:
    """Apply KPI display customizations to a numeric value.

    Returns ``value`` unchanged if it's None (so the renderer's None-handling
    leaves the token unresolved) or if it's not numeric. Otherwise mirrors
    ``webapp_v2/lib/formatters.ts:formatKPIValue`` so email bodies stay
    byte-identical with the KPI card.
    """
    if value is None:
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return value
    return format_number_v2(
        numeric,
        format_type=customizations.get("numberFormat", "default") or "default",
        decimal_places=customizations.get("decimalPlaces") or 0,
        prefix=customizations.get("numberPrefix") or "",
        suffix=customizations.get("numberSuffix") or "",
    )


def tokens_for_alert(
    alert: Alert, *, current_value: Any, rag_status: Optional[str] = None
) -> dict[str, Any]:
    """Convenience helper — builds tokens from an Alert instance + a value.

    For ``kpi_rag`` alerts, ``current_value`` and ``target_value`` are formatted
    using the KPI's display customizations (``extra_config.customizations``) so
    email bodies match the KPI card. If ``customizations`` is empty/absent, the
    raw value passes through unchanged. When ``customizations`` exists but has
    no ``numberFormat``, formatting still runs with format ``default`` so
    decimal places + prefix/suffix take effect (matches frontend ``formatKPIValue``).
    """
    metric_name = alert.metric.name if alert.metric_id and alert.metric else None
    kpi_name = alert.kpi.name if alert.kpi_id and alert.kpi else None

    dataset_name: Optional[str] = None
    if alert.alert_type == AlertType.STANDALONE and alert.standalone_config:
        cfg = StandaloneConfig(**alert.standalone_config)
        dataset_name = f"{cfg.schema_name}.{cfg.table_name}".strip(".") or None

    target_value: Any = None
    if alert.alert_type == AlertType.KPI_RAG:
        target_value = alert.kpi.target_value if alert.kpi_id and alert.kpi else None
    else:
        cond = alert.condition or {}
        target_value = cond.get("value")

    # Format kpi_rag current/target via the KPI's display customizations so the
    # email body matches the KPI card byte-for-byte.
    if alert.alert_type == AlertType.KPI_RAG and alert.kpi_id and alert.kpi:
        cust = (alert.kpi.extra_config or {}).get("customizations") or {}
        # Apply formatting whenever any display field is set — decimalPlaces /
        # numberPrefix / numberSuffix work even with no explicit numberFormat
        # (treated as "default"), matching frontend formatKPIValue.
        if isinstance(cust, dict) and any(
            cust.get(k) is not None
            for k in ("numberFormat", "decimalPlaces", "numberPrefix", "numberSuffix")
        ):
            current_value = _format_via_kpi_customizations(current_value, cust)
            target_value = _format_via_kpi_customizations(target_value, cust)

    return resolve_tokens(
        alert.alert_type,
        alert_name=alert.name,
        metric_name=metric_name,
        kpi_name=kpi_name,
        dataset_name=dataset_name,
        target_value=target_value,
        current_value=current_value,
        rag_status=rag_status,
    )

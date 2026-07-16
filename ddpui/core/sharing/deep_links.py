"""Frontend deep-link + resource-presentation helpers, shared by
``access_requests`` and ``sharing_actions``. Lives in its own module so
both can import it without a cycle.
"""

from django.conf import settings

# Deep-link shape per rtype — each query param matches what the webapp page
# actually reads.
DEEP_LINK_PATH = {
    "chart": "/charts/{id}",
    "dashboard": "/dashboards/{id}",
    "report": "/reports/{id}",
    "alert": "/alerts?alertId={id}",
    "metric": "/metrics?highlight={id}",
    "kpi": "/kpis?open={id}",
}

NOUN_BY_RTYPE = {
    "chart": "chart",
    "dashboard": "dashboard",
    "report": "report",
    "alert": "alert",
    "metric": "metric",
    "kpi": "KPI",
}


def frontend_url() -> str:
    return (
        getattr(settings, "FRONTEND_URL_V2", None)
        or getattr(settings, "FRONTEND_URL", None)
        or "http://localhost:3001"
    )


def build_resource_url(rtype: str, resource_id) -> str:
    """Deep link back to the resource in the frontend; falls back to the bare
    frontend URL for an unknown rtype."""
    path_template = DEEP_LINK_PATH.get(rtype)
    if path_template is None:
        return frontend_url()
    return f"{frontend_url()}{path_template.format(id=resource_id)}"


def resource_label(rtype: str, resource) -> str:
    """Best-effort human label: `title` (dashboard/report) or `name`
    (alert/metric/kpi); falls back to a generic `noun #id`."""
    label = getattr(resource, "title", None) or getattr(resource, "name", None)
    if label:
        return label
    noun = NOUN_BY_RTYPE.get(rtype, rtype)
    return f"{noun} #{resource.pk}"

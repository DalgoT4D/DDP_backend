"""Frontend deep-link + resource-presentation helpers for Resource Sharing,
shared by the read/notify paths (``access_requests.py``) and the write path
(``sharing_actions.py``).

Lives in its own module so both can import it without a cycle:
``access_requests`` imports ``sharing_actions`` (for the owner-resolution
helpers), so ``sharing_actions`` cannot import back from ``access_requests``.
The deep-link map + URL/label builders that Task 15b put in
``access_requests`` are extracted here verbatim (same query-param shapes the
webapp pages read) so the grant-notification email reuses them instead of
duplicating the URL shapes.
"""

from django.conf import settings

# Deep-link shape per rtype -- each query param matches what the webapp page
# actually reads (Task 15b): /alerts reads `?alertId=`, /metrics reads
# `?highlight=`, /kpis reads `?open=`; dashboards/reports route by path.
DEEP_LINK_PATH = {
    "dashboard": "/dashboards/{id}",
    "report": "/reports/{id}",
    "alert": "/alerts?alertId={id}",
    "metric": "/metrics?highlight={id}",
    "kpi": "/kpis?open={id}",
}

NOUN_BY_RTYPE = {
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
    """Deep link back to `resource` in the frontend (best-effort -- falls
    back to the bare frontend URL for an rtype this map doesn't know, which
    never happens for a registered rtype today)."""
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

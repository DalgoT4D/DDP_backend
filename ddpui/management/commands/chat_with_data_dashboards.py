"""Create/refresh the Langfuse dashboards for Dalgo Copilot.

Idempotent: widgets and dashboards are matched by name and reused, so re-run
freely — e.g. when a new org gets the feature:

    python manage.py chat_with_data_dashboards --org admin-dev --org atecf

Builds on the unstable dashboards API (v4 server). Widget queries use the v2
metrics data model (observations view; the traces view is not supported).
Span/generation names referenced here are the stable names from
ddpui/core/ai/tracing.py — treat both sides as one API.
"""

import json
import os
import urllib.request

from django.core.management.base import BaseCommand, CommandError

STAGE_SPAN_NAMES = ["route-question", "retrieve-context", "run-sql-agent", "validate-answer"]

# One widget definition per chart; matched by name on re-run
WIDGETS = [
    {
        "name": "Questions per day",
        "description": "Root observations = one per question asked",
        "view": "observations",
        "chartType": "LINE_TIME_SERIES",
        "metrics": [{"measure": "count", "agg": "count"}],
        "dimensions": [],
        "filters": [
            {"column": "isRootObservation", "operator": "=", "value": True, "type": "boolean"}
        ],
    },
    {
        "name": "Model cost per day",
        "description": "Total LLM spend across all orgs and models",
        "view": "observations",
        "chartType": "LINE_TIME_SERIES",
        "metrics": [{"measure": "totalCost", "agg": "sum"}],
        "dimensions": [],
        "filters": [],
    },
    {
        "name": "Cost by model",
        "description": "Where the spend goes: router/validator vs the main agent model",
        "view": "observations",
        "chartType": "VERTICAL_BAR",
        "metrics": [{"measure": "totalCost", "agg": "sum"}],
        "dimensions": [{"field": "providedModelName"}],
        "filters": [],
    },
    {
        "name": "Cost by org (tags)",
        "description": "Spend grouped by trace tags (org slug is the first tag)",
        "view": "observations",
        "chartType": "PIE",
        "metrics": [{"measure": "totalCost", "agg": "sum"}],
        "dimensions": [{"field": "tags"}],
        "filters": [],
    },
    {
        "name": "Errors per day",
        "description": "level=ERROR only — HITL pauses are excluded by design",
        "view": "observations",
        "chartType": "LINE_TIME_SERIES",
        "metrics": [{"measure": "count", "agg": "count"}],
        "dimensions": [],
        "filters": [{"column": "level", "operator": "=", "value": "ERROR", "type": "string"}],
    },
    {
        "name": "Stage latency p95",
        "description": "p95 per TurnGraph stage span",
        "view": "observations",
        "chartType": "HORIZONTAL_BAR",
        "metrics": [{"measure": "latency", "agg": "p95"}],
        "dimensions": [{"field": "name"}],
        "filters": [
            {
                "column": "name",
                "operator": "any of",
                "value": STAGE_SPAN_NAMES,
                "type": "stringOptions",
            }
        ],
    },
    {
        "name": "Answer validation (avg)",
        "description": "result_validation score: 1=ok, 0=warn — quality drift watch",
        "view": "scores-numeric",
        "chartType": "LINE_TIME_SERIES",
        "metrics": [{"measure": "value", "agg": "avg"}],
        "dimensions": [],
        "filters": [
            {"column": "name", "operator": "=", "value": "result_validation", "type": "string"}
        ],
    },
]

OVERVIEW_DASHBOARD = "Copilot — Overview"
# widgets shown on each per-org dashboard (org filter applied dashboard-level)
ORG_WIDGET_NAMES = [
    "Questions per day",
    "Model cost per day",
    "Errors per day",
    "Stage latency p95",
    "Answer validation (avg)",
]


class LangfuseApi:
    """Minimal REST client for the endpoints this command needs."""

    def __init__(self):
        public = os.getenv("LANGFUSE_PUBLIC_KEY")
        secret = os.getenv("LANGFUSE_SECRET_KEY")
        if not (public and secret):
            raise CommandError("LANGFUSE_PUBLIC_KEY / LANGFUSE_SECRET_KEY are not set")
        self.host = os.getenv("LANGFUSE_HOST", "http://localhost:3000").rstrip("/")
        import base64

        token = base64.b64encode(f"{public}:{secret}".encode()).decode()
        self._auth = f"Basic {token}"

    def request(self, method: str, path: str, body: dict | None = None) -> dict:
        req = urllib.request.Request(
            f"{self.host}{path}",
            method=method,
            data=json.dumps(body).encode() if body is not None else None,
            headers={"Authorization": self._auth, "Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(req) as response:
                return json.loads(response.read() or "{}")
        except urllib.error.HTTPError as err:
            detail = err.read().decode(errors="replace")[:500]
            raise CommandError(f"{method} {path} -> {err.code}: {detail}") from err

    # widgets
    def list_widgets(self) -> list[dict]:
        return self.request("GET", "/api/public/unstable/dashboard-widgets?limit=100").get(
            "data", []
        )

    def create_widget(self, spec: dict) -> dict:
        return self.request("POST", "/api/public/unstable/dashboard-widgets", spec)

    # dashboards
    def list_dashboards(self) -> list[dict]:
        return self.request("GET", "/api/public/unstable/dashboards?limit=100").get("data", [])

    def get_dashboard(self, dashboard_id: str) -> dict:
        return self.request("GET", f"/api/public/unstable/dashboards/{dashboard_id}")

    def create_dashboard(self, name: str, description: str, filters: list[dict]) -> dict:
        return self.request(
            "POST",
            "/api/public/unstable/dashboards",
            {"name": name, "description": description, "filters": filters},
        )

    def add_placement(self, dashboard_id: str, widget_id: str, x: int, y: int) -> dict:
        return self.request(
            "POST",
            f"/api/public/unstable/dashboards/{dashboard_id}/placements",
            {"type": "widget", "widgetId": widget_id, "x": x, "y": y, "width": 6, "height": 6},
        )


def _placed_widget_ids(dashboard: dict) -> set[str]:
    """Widget ids already placed — walk the response defensively (unstable API)."""
    found: set[str] = set()

    def walk(node):
        if isinstance(node, dict):
            if "widgetId" in node:
                found.add(node["widgetId"])
            for value in node.values():
                walk(value)
        elif isinstance(node, list):
            for value in node:
                walk(value)

    walk(dashboard)
    return found


class Command(BaseCommand):
    help = "Create/refresh the Copilot Langfuse dashboards (idempotent)"

    def add_arguments(self, parser):
        parser.add_argument(
            "--org",
            action="append",
            default=[],
            help="Org slug to build a filtered dashboard for (repeatable)",
        )

    def handle(self, *args, **options):
        api = LangfuseApi()

        # 1. widgets, matched by name
        existing = {w["name"]: w for w in api.list_widgets()}
        widget_ids: dict[str, str] = {}
        for spec in WIDGETS:
            if spec["name"] in existing:
                widget_ids[spec["name"]] = existing[spec["name"]]["id"]
                self.stdout.write(f"widget exists: {spec['name']}")
            else:
                created = api.create_widget(spec)
                widget_ids[spec["name"]] = created["id"]
                self.stdout.write(self.style.SUCCESS(f"widget created: {spec['name']}"))

        # 2. the all-orgs overview
        self._ensure_dashboard(
            api,
            name=OVERVIEW_DASHBOARD,
            description="Dalgo Copilot usage, cost, latency, and quality across all orgs",
            filters=[],
            widget_names=[w["name"] for w in WIDGETS],
            widget_ids=widget_ids,
        )

        # 3. one filtered dashboard per org
        for slug in options["org"]:
            self._ensure_dashboard(
                api,
                name=f"Copilot — {slug}",
                description=f"Dalgo Copilot activity for org {slug}",
                filters=[
                    {
                        "column": "tags",
                        "operator": "any of",
                        "value": [slug],
                        "type": "arrayOptions",
                    }
                ],
                widget_names=ORG_WIDGET_NAMES,
                widget_ids=widget_ids,
            )

    def _ensure_dashboard(self, api, *, name, description, filters, widget_names, widget_ids):
        dashboards = {d["name"]: d for d in api.list_dashboards()}
        if name in dashboards:
            dashboard_id = dashboards[name]["id"]
            self.stdout.write(f"dashboard exists: {name}")
        else:
            dashboard_id = api.create_dashboard(name, description, filters)["id"]
            self.stdout.write(self.style.SUCCESS(f"dashboard created: {name}"))

        placed = _placed_widget_ids(api.get_dashboard(dashboard_id))
        row, col = 0, 0
        for widget_name in widget_names:
            widget_id = widget_ids[widget_name]
            if widget_id in placed:
                col, row = (6, row) if col == 0 else (0, row + 6)
                continue
            api.add_placement(dashboard_id, widget_id, x=col, y=row)
            self.stdout.write(f"  placed: {widget_name}")
            col, row = (6, row) if col == 0 else (0, row + 6)

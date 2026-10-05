"""Which tools each agent gets, which of them pause for the user, and how
each one is labelled in the chat UI.

The tools themselves live in tools/*_tools.py and register by name with
tools/registry.py; this module only groups those names. Every name here is
checked against the registry by test_toolsets.py, so a typo fails CI instead
of silently dropping a tool or an approval gate.
"""

# ---------------------------------------------------------------------------
# Per-agent toolboxes
# ---------------------------------------------------------------------------

# The SQL agent's toolbox: data Q&A only. Creation (charts, dashboards, KPIs,
# metrics, reports) belongs to the platform guide agent — the router sends
# those requests there (intent platform_help).
SQL_AGENT_TOOLS = (
    "list_schemas",
    "list_tables",
    "get_table_details",
    "lookup_column_values",
    "execute_sql",
    "ask_user",
    "handoff_to_platform_guide",
)

# The guide agent's toolbox: docs + inventory + discovery (for real column
# names during chart/metric creation) + the creation tools + ask_user.
GUIDE_AGENT_TOOLS = (
    "get_dalgo_help",
    "list_metrics",
    "list_kpis",
    "list_charts",
    "list_reports",
    "list_schemas",
    "list_tables",
    "get_table_details",
    "list_dashboards",
    "create_chart",
    "create_dashboard",
    "add_charts_to_dashboard",
    "create_metric",
    "create_kpi",
    "create_report",
    "ask_user",
)

# ---------------------------------------------------------------------------
# Human-in-the-loop (agent/hitl.py)
# ---------------------------------------------------------------------------

# Tool calls that pause for user approval before executing. The two warehouse
# tools that return real VALUES are gated so the user can mark PII columns first;
# pure metadata lookups (list_schemas, list_tables, get_table_details) are not —
# gating them would cost several clicks before any question could be answered.
# Only warehouse reads pause for approval on this agent
SQL_APPROVAL_TOOLS = ("execute_sql", "lookup_column_values")

# Creation tools pause for user approval (same cards as the SQL agent's)
GUIDE_APPROVAL_TOOLS = (
    "create_chart",
    "create_dashboard",
    "add_charts_to_dashboard",
    "create_metric",
    "create_kpi",
    "create_report",
)

# The clarification tool: respond-only, the human's answer IS the tool result
QUESTION_TOOL = "ask_user"

# Tools whose card carries a PII checkbox list: the only two that return real
# warehouse values to the model.
PII_REVIEW_TOOLS = ("execute_sql", "lookup_column_values")

# ---------------------------------------------------------------------------
# Handoff (chat/turn_graph.py)
# ---------------------------------------------------------------------------

# The SQL agent calls this to yield the turn to the platform guide
HANDOFF_TOOL = "handoff_to_platform_guide"

# ---------------------------------------------------------------------------
# UI labels (chat/turn_runner.py)
# ---------------------------------------------------------------------------

# Plain-language activity labels shown to non-technical users while tools run
TOOL_LABELS = {
    "list_schemas": "Looking at your data…",
    "list_tables": "Looking at your tables…",
    "get_table_details": "Reading table structure…",
    "lookup_column_values": "Looking up how values are stored…",
    "execute_sql": "Running query…",
    "create_chart": "Creating chart…",
    "list_dashboards": "Checking your dashboards…",
    "create_dashboard": "Creating dashboard…",
    "add_charts_to_dashboard": "Adding to dashboard…",
    "ask_user": "Asking you a question…",
    "get_dalgo_help": "Reading the Dalgo guide…",
    "list_metrics": "Checking your metrics…",
    "list_kpis": "Checking your KPIs…",
    "list_charts": "Checking your charts…",
    "list_reports": "Checking your reports…",
    "create_metric": "Creating metric…",
    "create_kpi": "Creating KPI…",
    "create_report": "Creating report…",
    "handoff_to_platform_guide": "Bringing in the platform guide…",
}
GENERIC_TOOL_LABEL = "Working…"

# Chat with Data — developer guide

Chat with Data is a LangGraph agent (`ddpui/core/chat_with_data/`) that answers
natural-language questions by running guarded, read-only SQL against an org's
warehouse.

## Architecture in one paragraph

`agent.py` compiles LangChain's prebuilt agent loop (`create_agent`) with five
tools from `tools/registry.py` (`list_schemas`, `list_tables`,
`get_table_details`, `profile_column`, `execute_sql`). Org specifics (dialect,
schema allowlist, limits, warehouse client) travel in a `RunContext`
(`state.py`) injected into tools via `ToolRuntime` — the model never sees org
identifiers. `guards/sql_guard.py` AST-validates every query (sqlglot):
SELECT-only by node type, schema allowlist, LIMIT clamp. Middleware
(`middleware.py`) supplies the per-org system prompt, trims history, and
hard-stops after 3 failed SQL attempts. Conversation memory is a LangGraph
Postgres checkpointer (`checkpointer.py`) in the app database.

## Environment variables

Add to your `.env` (and keep `.env.template` in sync):

```bash
# Anthropic API key used by the agent (deployment-level, required)
ANTHROPIC_API_KEY=sk-ant-...

# Agent model override; defaults to claude-sonnet-5
CHAT_WITH_DATA_MODEL=

# Langfuse tracing (optional — tracing is OFF when the keys are unset).
# Self-hostable, so traces never leave the deployment.
LANGFUSE_PUBLIC_KEY=
LANGFUSE_SECRET_KEY=
LANGFUSE_HOST=http://localhost:3000
```

## One-time setup

```bash
uv sync
uv run python manage.py chat_with_data_setup   # creates checkpointer tables
```

## Try the agent from a terminal (no frontend needed)

```bash
uv run python manage.py chat_with_data_repl --org <org-slug>
```

```
you> how many surveys did we run in Pune last month?
⚙ list_tables {'schema_name': 'prod'}
  ↳ Tables in prod:
⚙ get_table_details {'schema_name': 'prod', 'table_name': 'surveys'}
  ↳ Table prod.surveys
⚙ execute_sql {'sql': "SELECT COUNT(*) ..."}
  ↳ Query returned 1 rows.
You ran 1,284 surveys in Pune in June. ...
```

## Watch what the agent is doing (Langfuse)

1. Run Langfuse locally (`docker compose` from the
   [langfuse repo](https://github.com/langfuse/langfuse)) — it serves on port 3000.
2. In the Langfuse UI: create an organization + project, then copy the API keys
   from **Project settings → API keys** into `.env`:
   `LANGFUSE_PUBLIC_KEY=pk-lf-...`, `LANGFUSE_SECRET_KEY=sk-lf-...`,
   `LANGFUSE_HOST=http://localhost:3000`.
3. Restart the backend, ask a question, and open **Traces**. Each turn is one
   trace (grouped by chat session): every model call with token usage, every
   tool call with its input/output (including the generated SQL), and latency.

Implementation notes (`ddpui/core/chat_with_data/observability.py`): tracing is
disabled unless both keys are set, and every hook is fail-safe — a Langfuse
outage can never break a chat turn. Traces are tagged with `org_slug` and
`dialect`, keyed by opaque session/orguser ids (never emails), and carry the
turn's `request_uuid` so a trace can be joined to its `ChatWithDataTurnAudit`
row and log lines. The dbt stack pins `protobuf<5`, which rules out the
Langfuse v3 SDK — we use the v2 client behind a small `langchain_core`
callback handler instead.

Traces contain prompts **and query results**. For production orgs this is only
acceptable against a **self-hosted** Langfuse inside the deployment — never a
cloud instance the org hasn't consented to.

## Tests

```bash
uv run pytest ddpui/tests/core/chat_with_data -v
```

The agent loop is tested with a scripted fake model (no API key needed); the
SQL guard has the red-team bypass catalog; tools run against a fake warehouse.

## Known limitations (v1)

- Read-only is enforced at the SQL-AST layer; warehouse credentials are
  read-write (read-only role per org is a fast-follow).
- BigQuery has no per-query timeout yet (Postgres uses `statement_timeout`);
  the LIMIT clamp bounds result size.
- Python 3.10: no `custom` stream mode from async tools (needs 3.11), so tool
  progress events come from `updates` mode only.

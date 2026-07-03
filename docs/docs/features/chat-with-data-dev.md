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

# LangSmith tracing — dev debugging only, leave off in production
LANGSMITH_TRACING=false
LANGSMITH_API_KEY=
LANGSMITH_PROJECT=dalgo-chat-with-data-dev
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

## Watch what the agent is doing (LangSmith)

1. Sign up at [smith.langchain.com](https://smith.langchain.com) — the free
   Developer tier (5K traces/month) is enough for development.
2. Create an API key, then set in `.env`:
   `LANGSMITH_TRACING=true`, `LANGSMITH_API_KEY=<key>`,
   `LANGSMITH_PROJECT=dalgo-chat-with-data-dev`.
3. Run the REPL and open your project in LangSmith. Every turn shows the full
   run tree: each model call, each tool call with its arguments (including the
   generated SQL), token counts, and latency.

Tracing sends prompts and query results to LangSmith's cloud — **never enable it
against production orgs / real beneficiary data.** (Self-hosted LangSmith is
enterprise-only; if fully-local tracing ever matters, Langfuse is the
open-source alternative.)

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

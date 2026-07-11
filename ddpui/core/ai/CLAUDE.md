# `ddpui/core/ai` — Dalgo's AI features

This package holds every AI feature in Dalgo and the infrastructure they share.
Today that is two features:

1. **Chat with Data** — an NGO staff member asks "how many surveys did we run in
   Maharashtra?" and an agent queries their warehouse and answers in plain language.
2. **Report summaries** — one click drafts an executive summary of a report from
   its frozen chart data.

Both are built on the same pieces: a model factory, an agent loop, SQL guards,
and tracing. New AI features should extend this package, not start a new one.
Keep this file updated when you add an agent, tool, scope, or llm_call — it is
the map new engineers and agents read first.

## Folder map

| Folder | What it does | Start reading at |
|---|---|---|
| `agent/` | One module per agent, plus the loop infrastructure they share | `chat_data_agent.py` |
| `chat/` | The Chat with Data turn pipeline: graph, streaming, sessions, history | `turn_runner.py` |
| `scopes/` | What a session may see (org-wide, or one dashboard's tables) | `resolver.py` |
| `llm_calls/` | One-shot model calls: route, reflect, audit, title | `router.py` |
| `tools/` | The tools the chat agent can call (discover, query, create charts) | `registry.py` |
| `guards/` | Deterministic SQL safety — no LLM involved | `sql_guard.py` |
| `messages/` | Reading LangChain messages: text, artifacts, conversation views | `artifacts.py` |
| `tracing.py` | Langfuse tracing — one trace per turn, off unless keys are set | — |

## The journey of one question

Priya types "how many surveys in Maharashtra?" into the chat. The WebSocket
consumer (`ddpui/websockets/chat_with_data_consumer.py`) authenticates her,
builds a `RunContext` (her org's warehouse, allowed schemas, permissions), and
hands the question to the turn runner.

```
question ──► route_node          llm_calls/router.py: data question, small talk,
                │                or needs clarification? (fail-open: data question)
                ▼
        retrieve_context_node    placeholder for table-card retrieval (M5)
                │
                ▼
            sql_agent            agent/chat_data_agent.py loop:
                │                  list_tables → get_table_details → profile_column
                │                  → execute_sql (guards/sql_guard.py validates first;
                │                    llm_calls/sql_reflection.py reviews complex SQL)
                ▼
           validate_node         llm_calls/turn_audit.py: does the answer match
                │                the SQL and the result? (never blocks the answer)
                ▼
     events stream to the UI     chat/turn_runner.py translates every step into
                                 WS events: token, tool_start/end, message_complete,
                                 validation — then writes a ChatWithDataTurnAudit row
```

The graph lives in `chat/turn_graph.py`; the event translation and audit row in
`chat/turn_runner.py`. Conversation memory is a LangGraph Postgres checkpointer
(`agent/checkpointer.py`), replayed for the UI by `chat/history.py`.

## The three checks (they are not the same thing)

| Check | File | When | Who | Can it block? |
|---|---|---|---|---|
| Guard | `guards/sql_guard.py` | before every query | code (AST) | yes — rejects unsafe SQL |
| Reflection | `llm_calls/sql_reflection.py` | before complex queries | small model | yes — sends SQL back for revision |
| Audit | `llm_calls/turn_audit.py` | after the answer | small model | no — adds a caveat, never blocks |

**Example:** the guard rejects `DELETE FROM surveys` outright. Reflection catches
"this JOIN double-counts surveys" before the query runs. The audit notices "the
question said Maharashtra but the SQL has no state filter" after the answer, and
the UI shows that as a caveat.

## Design rules

- **Fail-open for helpers, fail-loud for deliverables.** If the router, reflection,
  audit, or title call fails, the turn continues as if the check found nothing.
  If the report summary fails, the user clicked a button and gets a real error
  (`agent/report_summary_agent.py`).
- **The model never sees org identifiers or credentials.** Everything org-specific
  travels in `RunContext` (`agent/run_context.py`), resolved server-side by
  `agent/context_builder.py` — the only module here that reads the ORM for context.
- **The warehouse is read-only.** Tools that "create" things (charts, dashboards)
  write Dalgo metadata only. `execute_sql` is the single path to the warehouse,
  and the guard clamps every query to a single `SELECT` with a row limit.
- **One artifact contract.** Tools attach structured results to their messages;
  `messages/artifacts.py` is the only interpreter. The live stream, the audit,
  and history replay all read through it so they can never disagree.

## How to extend

**Add a tool** (e.g. `export_csv`): one new module in `tools/`, decorated with
`@register_tool`, plus an import line in `tools/registry.py`. The agent graph
does not change. Follow `tools/chart_tools.py` as the template.

**Add an agent** (e.g. a data-quality summarizer): one new module in `agent/`,
built from `agent/base.py`'s model factory and, if it needs a loop,
`chat_data_agent.py`'s middleware stack. `agent/report_summary_agent.py` shows
the minimal one-call shape.

**Add a scope** (e.g. report-scoped chat): one new module in `scopes/` that
returns a `ResolvedScope` (`scopes/base.py`), plus a dispatch line in
`scopes/resolver.py`. `scopes/dashboard_scope.py` is the reference.

## Models and configuration

Every job picks its model with an env var and a default, through
`agent/base.py::build_model`:

| Env var | Used by | Default |
|---|---|---|
| `CHAT_WITH_DATA_MODEL` | chat agent | `claude-sonnet-5` |
| `CHAT_WITH_DATA_ROUTER_MODEL` | router | `claude-haiku-4-5` |
| `CHAT_WITH_DATA_VALIDATOR_MODEL` | turn audit | `claude-haiku-4-5` |
| `CHAT_WITH_DATA_REFLECTION_MODEL` | SQL reflection | `claude-haiku-4-5` |
| `CHAT_WITH_DATA_TITLE_MODEL` | session titles | `claude-haiku-4-5` |
| `REPORT_SUMMARY_MODEL` | report summaries | `claude-sonnet-5` |

Tracing needs `LANGFUSE_PUBLIC_KEY` + `LANGFUSE_SECRET_KEY`; without them it is
silently off and a tracing failure can never break a turn (`tracing.py`).

## Running and testing

```bash
uv run pytest ddpui/tests/core/ai -v          # unit tests for this package
uv run python manage.py chat_with_data_repl --org <slug>   # chat from the terminal
uv run python manage.py chat_with_data_setup  # create checkpointer tables (once per env)
```

Transports live outside this package: REST session endpoints in
`ddpui/api/chat_with_data_api.py`, the streaming WebSocket in
`ddpui/websockets/chat_with_data_consumer.py`, report summary endpoint in
`ddpui/api/report_api.py`. User-facing docs: `docs/docs/features/chat-with-data-dev.md`.

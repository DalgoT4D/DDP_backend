# `ddpui/core/ai` — Dalgo's AI features

This package holds every AI feature in Dalgo and the infrastructure they share.
Today that is one product feature — **Chat with Data** (a.k.a. Dalgo Copilot):
an NGO staff member asks "how many surveys did we run in Maharashtra?" and an
agent queries their warehouse and answers in plain language. It can also create
platform objects in-chat ("make that a chart on my dashboard") via a second
agent. (Report summaries and dashboard-scoped chat existed here once and were
removed 2026-08-18 — deferred to a follow-up; don't be surprised by their
ghosts in old migrations.)

New AI features should extend this package, not start a new one. Keep this file
updated when you add an agent, tool, or llm_call — it is the map new engineers
and agents read first.

## Folder map

| Folder | What it does | Start reading at |
|---|---|---|
| `agent/` | One module per agent, plus the loop infrastructure they share | `chat_data_agent.py` |
| `chat/` | The turn pipeline: graph, streaming, sessions, history, settings | `turn_runner.py` |
| `llm_calls/` | One-shot model calls: route, reflect, audit, title | `router.py` |
| `tools/` | Everything either agent can call (discover, query, create) | `registry.py` |
| `guards/` | Deterministic SQL safety — no LLM involved | `sql_guard.py` |
| `messages/` | Reading LangChain messages: text, artifacts, conversation views | `artifacts.py` |
| `evals/` | Golden-set eval runner + scorers; datasets as JSONL in git | `README.md` |
| `tracing.py` | Langfuse tracing — one trace per question, off unless keys are set | — |

## The journey of one question

Priya types "how many surveys in Maharashtra?" into the chat. The WebSocket
consumer (`ddpui/websockets/chat_with_data_consumer.py`) authenticates her,
builds a `RunContext` (her org's warehouse, allowed schemas, permissions, PII
rules, org memory), and hands the question to the turn runner.

```
question ──► route_node ──┬─ small talk       → casual_reply_node → END
   llm_calls/router.py    ├─ needs clarify*   → clarify_node      → END   (*first turn only)
   fail-open:             ├─ platform help    → guide_agent       → END
   data question          └─ data question    ↓
                          retrieve_context_node   (placeholder for table-card retrieval, M5)
                                    ↓
                               sql_agent          agent/chat_data_agent.py loop:
                                    │               list_tables → get_table_details → profile_column
                                    │               → execute_sql (guards/sql_guard.py validates first;
                                    │                 llm_calls/sql_reflection.py reviews complex SQL)
                                    ↓
                     handed off? ── yes → guide_agent → END
                                    ↓ no
                              validate_node       llm_calls/turn_audit.py: does the answer match
                                    ↓             the SQL and the result? (never blocks the answer)
                events stream to the UI           chat/turn_runner.py translates every step into
                                                  WS events: token, tool_start/end, input_required,
                                                  message_complete, validation — then writes a
                                                  ChatWithDataTurnAudit row (status: completed|
                                                  failed|aborted|paused)
```

The graph lives in `chat/turn_graph.py`; the event translation and audit row in
`chat/turn_runner.py`. Conversation memory is a LangGraph Postgres checkpointer
(`agent/checkpointer.py`), replayed for the UI by `chat/history.py`.

## Two agents, one conversation

The **SQL agent** (`agent/chat_data_agent.py`) answers questions FROM the data:
`list_schemas`, `list_tables`, `get_table_details`, `profile_column`,
`execute_sql`, `ask_user`. The **platform guide agent**
(`agent/platform_guide_agent.py`) works ON the platform: it explains features
(reading docs.dalgo.org via `get_dalgo_help`) and creates charts, dashboards,
metrics, KPIs, and reports through the same services the REST API uses.

They share one checkpointed thread, and three mechanisms keep the pair honest:

- **Routing.** The router sends creation/how-to requests to the guide
  (`platform_help`); a deterministic regex backstop in `router.py` catches
  unmistakable creation requests even when the model misroutes or fails open.
  The router is also told which agent wrote the previous answer, so short
  follow-ups ("yes", "make it monthly") stick to the right lane.
- **Mid-turn handoff.** A creation request that reaches the SQL agent anyway
  triggers its `handoff_to_platform_guide` tool (return_direct), and the
  TurnGraph continues the SAME turn in the guide agent — the user never
  re-asks. The handoff path skips validate_node (nothing SQL to audit).
- **Error repair.** When one agent hallucinates the other's tool, the "not a
  valid tool" error stays in the shared history and would convince the OTHER
  agent its own tool is broken. `middleware.repair_foreign_tool_errors`
  durably rewrites exactly those errors (tools the current agent owns).

## Human in the loop (`agent/hitl.py`)

Two pauses ride one mechanism (HumanInTheLoopMiddleware + the checkpointer):

- **approval** — `execute_sql` (SQL agent) and every creation tool (guide
  agent) wait for an approve/cancel card before running. Metadata lookups are
  deliberately not gated.
- **question** — `ask_user` never executes; the user's typed reply becomes the
  tool result.

A paused turn is durable: graph state in Postgres, the pending card in Redis
(24h TTL), so a page reload or backend restart re-sends the card and
`Command(resume=...)` continues the turn — as the SAME Langfuse trace.

## The three checks (they are not the same thing)

| Check | File | When | Who | Can it block? |
|---|---|---|---|---|
| Guard | `guards/sql_guard.py` | before every query | code (sqlglot AST) | yes — rejects unsafe SQL |
| Reflection | `llm_calls/sql_reflection.py` | before complex-lane queries only | small model | yes — sends SQL back for revision |
| Audit | `llm_calls/turn_audit.py` | after the answer | small model | no — adds a caveat, never blocks |

**Example:** the guard rejects `DELETE FROM surveys` outright (any forbidden
node anywhere in the tree — a DELETE inside a CTE is still a DELETE), enforces
one schema-qualified SELECT and clamps LIMIT. Reflection catches "this JOIN
double-counts surveys" before the query runs (complex questions only — the
router sets the lane). The audit notices "the question said Maharashtra but the
SQL has no state filter" after the answer, and the UI shows that as a caveat.

A failed or reflected query costs one of `MAX_SQL_ATTEMPTS` (5) tries per
question; `middleware.sql_retry_limiter` ends the loop deterministically after
that, and `RECURSION_LIMIT` (160 graph steps) is the runaway backstop.

## Design rules

- **Fail-open for helpers, fail-loud for deliverables.** If the router,
  reflection, audit, or title call fails, the turn continues as if the check
  found nothing. A creation tool the user clicked "approve" on reports its real
  error back into the chat.
- **The model never sees org identifiers or credentials.** Everything
  org-specific travels in `RunContext` (`agent/run_context.py`), resolved
  server-side by `agent/context_builder.py` — the only module here that reads
  the ORM for context. Discovery is **scan-then-ask** (since 2026-09-10): the
  allowlist is every non-system schema, the prompt steers the agent to scan
  prod/intermediate/staging first and to `ask_user` rather than comb the rest;
  an admin can pin the list via `ChatWithDataOrgConfig.allowed_schemas`.
- **The warehouse is read-only.** Creation tools write Dalgo metadata only,
  through the same services the REST API uses (identical validation).
  `execute_sql` is the single path to the warehouse.
- **One artifact contract.** Tools attach structured results to their messages;
  `messages/artifacts.py` is the only interpreter. The live stream, the audit,
  and history replay all read through it so they can never disagree.
- **PII is masked before the model sees it.** `agent/pii.py`: immovable
  defaults (emails, credit cards, Indian phone numbers, Aadhaar with Verhoeff
  checksum, PAN with holder-type check) plus additive per-org regex rules
  (`ChatWithDataOrgConfig.pii_rules`, validated at save time). Masking covers
  the user's message AND query results, and rewrites the checkpointed state —
  PII never reaches a model provider, the checkpoint DB, or traces. The UI's
  result table (from the tool artifact) is not masked.
- **Org memory is reference, not instructions.** Admin-curated facts
  (`ChatWithDataOrgMemory`, ≤5,000 chars) render into both system prompts
  inside an `<org_memory>` tag with an explicit injection disclaimer
  (`agent/org_memory.py`).

## Availability, settings, sessions

Chat is on when the org's `CHAT_WITH_DATA` feature flag is enabled AND a
warehouse exists (`chat/sessions.get_status`). The Copilot settings page
(GET/PUT `/api/chat-with-data/settings`, permission
`can_manage_chat_with_data_settings`) flips that flag and edits the org
memory — flipping the toggle IS the org's AI consent (`OrgPreferences.llm_optin`
deliberately does not gate chat; it still gates the other AI features).
Chatting needs `can_use_chat_with_data`; creation tools additionally check the
user's own `can_create_charts`/`can_create_metrics`/… permissions, resolved
into RunContext at context-build time.

Sessions (`ChatWithDataSession`) are owner-scoped rows holding a `thread_id`
into the checkpointer — message content lives ONLY there. Titles are
auto-generated after the first exchange (`llm_calls/session_title.py`). The
consumer rate-limits users (10 messages/min) and holds a Redis turn lock so a
crashed consumer can't wedge a session.

## How to extend

**Add a tool** (e.g. `export_csv`): one new module in `tools/`, decorated with
`@register_tool`, plus an entry in `registry.py`'s `_TOOL_MODULES` and the
owning agent's tool-name tuple (unknown names fail at build, not runtime).
The agent graph does not change. Follow `tools/chart_tools.py` as the template.

**Add an agent** (e.g. a data-quality summarizer): one new module in `agent/`,
built from `agent/base.py`'s model factory and the shared middleware stack.
`agent/platform_guide_agent.py` is the reference — same `create_agent`
assembly, its own tool tuple, approval tuple, and dynamic prompt.

## Models and configuration

Every job picks its model with an env var and a default, through
`agent/base.py::build_model`. The model id also picks the **provider**:
`claude-*` builds an Anthropic client (needs `ANTHROPIC_API_KEY`), `gpt-*`
builds an OpenAI client (needs `OPENAI_API_KEY`), and `openai:gpt-5.5` style
prefixes work for anything ambiguous. Users can also pick the chat model per
turn in the UI — from `chat_data_agent.MODEL_OPTIONS`, filtered to providers
whose key is set, never trusting the client's string.

| Env var | Used by | Default |
|---|---|---|
| `CHAT_WITH_DATA_MODEL` | both chat agents | `claude-sonnet-5` |
| `CHAT_WITH_DATA_ROUTER_MODEL` | router + casual replies | `claude-haiku-4-5` |
| `CHAT_WITH_DATA_VALIDATOR_MODEL` | turn audit | `claude-haiku-4-5` |
| `CHAT_WITH_DATA_REFLECTION_MODEL` | SQL reflection | `claude-haiku-4-5` |
| `CHAT_WITH_DATA_TITLE_MODEL` | session titles | `claude-haiku-4-5` |
| `DALGO_DOCS_BASE_URL` | get_dalgo_help | `https://docs.dalgo.org` |

Tracing needs `LANGFUSE_PUBLIC_KEY` + `LANGFUSE_SECRET_KEY`; without them it is
silently off and a tracing failure can never break a turn. The handler is
hand-rolled over the Langfuse v2 low-level client (the dbt stack pins
protobuf<5, which rules out v3/OTel — see `tracing.py`). One question is ONE
trace even across pause/resume; the span and generation names in `tracing.py`
are referenced by Langfuse dashboards (created by
`manage.py chat_with_data_dashboards`) — treat them as an API. Every trace is
stamped with which agent answered and whether a handoff happened.

## Running and testing

```bash
uv run pytest ddpui/tests/core/ai -v          # unit tests for this package
uv run python manage.py chat_with_data_repl --org <slug>    # chat from the terminal
uv run python manage.py chat_with_data_setup  # create checkpointer tables (once per env)
uv run python manage.py chat_with_data_eval --org <slug> --file ... --tag canary
                                              # golden-set evals (see evals/README.md)
uv run python manage.py chat_with_data_dashboards --org <slug>  # Langfuse dashboards
```

Transports live outside this package: REST status/settings/session endpoints in
`ddpui/api/chat_with_data_api.py`, the streaming WebSocket in
`ddpui/websockets/chat_with_data_consumer.py`. DB models in
`ddpui/models/chat_with_data.py` (session, turn audit, org memory, org config).
User-facing docs: `docs/docs/features/chat-with-data-dev.md`.

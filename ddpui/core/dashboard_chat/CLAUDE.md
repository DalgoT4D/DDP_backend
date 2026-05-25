# Dashboard Chat

This package implements "Chat with Dashboards" for one dashboard at a time.

The feature is intentionally scoped to the current dashboard only:
- the user can ask about this dashboard's charts, datasets, dbt lineage, and attached org/dashboard context
- the assistant can run read-only SQL against the allowlisted warehouse tables behind this dashboard
- the assistant should not jump to other dashboards or search the whole org blindly

## High-Level Request Flow

```
+---------------------+
| browser chat drawer |
+---------------------+
           |
           v
+--------------------------------+
| DashboardChatConsumer          |
| websocket entrypoint           |
+--------------------------------+
           |
           v
+--------------------------------+
| get/create session             |
| persist user message           |
+--------------------------------+
           |
           v
+--------------------------------+
| execute_dashboard_chat_turn    |
+--------------------------------+
           |
           v
+--------------------------------+
| DashboardChatRuntime.run       |
| - load context / bootstrap     |
| - route intent                 |
| - route-specific node          |
| - compose response             |
| - finalize metadata/citations  |
+--------------------------------+
           |
           v
+--------------------------------+
| persist assistant message      |
+--------------------------------+
           |
           v
+--------------------------------+
| websocket response event       |
+--------------------------------+
           |
           v
+--------------------------------+
| frontend renders markdown      |
| + structured tables            |
+--------------------------------+
```

## What Happens For Each User Question

Every user message is one turn. A turn always creates or reuses a `DashboardChatSession`, stores the user message, runs the LangGraph runtime, stores the assistant reply, and sends the reply back over websocket.

### 1. Small talk
Examples:
- "hi"
- "thanks"
- "what can you do?"
- "who are you?"

There are two ways a turn becomes `small_talk`:
- fast path: before LLM classification, the runtime checks the current user message only against `SMALL_TALK_FAST_PATH_PATTERN` (`hi`, `hello`, `hey`, `yo`, `good morning`, `good afternoon`, `good evening`, `thanks`, `thank you`, `what can you do`, `who are you`)
- normal routing: broader conversational prompts that are not in the fast path can still fall through to the intent classifier and come back as `small_talk`

Flow:
```
+---------------------+
| user message        |
+---------------------+
           |
           v
+---------------------+
| load_context        |
+---------------------+
           |
           v
+----------------------------------------------+
| route_intent                                 |
| - small-talk fast path match?                |
| - else run normal intent classification      |
| => final route = small_talk                  |
+----------------------------------------------+
           |
           v
+---------------------+
| handle_small_talk   |
+---------------------+
           |
           v
+---------------------+
| finalize            |
+---------------------+
```

No retrieval or SQL runs here.

### 2. Irrelevant question
Examples:
- "show me another dashboard"
- "which dashboard should I use for finance?"

Flow:
```
+---------------------+
| user message        |
+---------------------+
           |
           v
+---------------------+
| load_context        |
+---------------------+
           |
           v
+---------------------+
| route_intent        |
| => irrelevant       |
+---------------------+
           |
           v
+---------------------+
| handle_irrelevant   |
+---------------------+
           |
           v
+---------------------+
| finalize            |
+---------------------+
```

The runtime returns a scope-boundary answer and does not leave the current dashboard.

### 3. Needs clarification
Examples:
- "is this improving?"
- "show me the data"

Flow:
```
+--------------------------+
| user message             |
+--------------------------+
            |
            v
+--------------------------+
| load_context             |
+--------------------------+
            |
            v
+--------------------------+
| route_intent             |
| => needs_clarification   |
+--------------------------+
            |
            v
+--------------------------+
| handle_needs_clarification|
+--------------------------+
            |
            v
+--------------------------+
| finalize                 |
+--------------------------+
```

The assistant asks for the missing dimension, metric, filter, or timeframe.

### 4. Context / explanation question
Examples:
- "tell me about this dashboard"
- "what does this chart mean?"
- "what models are behind this chart?"

Flow:
```
+--------------------------+
| user message             |
+--------------------------+
            |
            v
+--------------------------+
| load_context             |
+--------------------------+
            |
            v
+--------------------------+
| route_intent             |
| => query_without_sql     |
+--------------------------+
            |
            v
+----------------------------------------------+
| handle_query_without_sql                     |
| -> tool loop                                 |
|    - inspect chart metadata                  |
|    - inspect enriched table metadata         |
|    - inspect related tables / join paths     |
| -> compose final answer                      |
+----------------------------------------------+
            |
            v
+--------------------------+
| finalize                 |
+--------------------------+
```

These questions are usually answered from:
- org/dashboard authored context
- chart metadata in the dashboard export
- dashboard-scoped enriched metadata artifacts

### 5. Data / SQL question
Examples:
- "give me district wise pass rates"
- "top 5 facilitators in q2"

Flow:
```
+--------------------------+
| user message             |
+--------------------------+
            |
            v
+--------------------------+
| load_context             |
+--------------------------+
            |
            v
+--------------------------+
| route_intent             |
| => query_with_sql        |
+--------------------------+
            |
            v
+----------------------------------------------+
| handle_query_with_sql                        |
| -> tool loop                                 |
|    - inspect chart metadata                  |
|    - inspect enriched table metadata         |
|    - get_schema_snippets                     |
|    - get_distinct_values                     |
|    - run_sql_query                           |
| -> compose final answer                      |
+----------------------------------------------+
            |
            v
+--------------------------+
| finalize                 |
+--------------------------+
```

Important behavior:
- SQL is allowlisted to the current dashboard's lineage
- only read-only SQL is allowed
- text filters should be validated with distinct values first
- the final answer is narrative markdown; structured SQL results are returned separately for table rendering in the UI
- non-fatal cautions are returned in `warnings` when the runtime had to adjust or flag something about the query

### 6. Follow-up SQL question
Examples:
- "now split by district"
- "same but only for q2"

Flow:
```
+-------------------------------------------+
| previous conversation + new message       |
+-------------------------------------------+
                    |
                    v
+--------------------------+
| load_context             |
+--------------------------+
            |
            v
+--------------------------+
| route_intent             |
| => follow_up_sql         |
+--------------------------+
            |
            v
+----------------------------------------------+
| handle_follow_up_sql                         |
| -> shorter tool loop                         |
| -> reuse conversation context + checkpointed state |
| -> run updated SQL                           |
| -> compose final answer                      |
+----------------------------------------------+
            |
            v
+--------------------------+
| finalize                 |
+--------------------------+
```

### 7. Follow-up context question
Examples:
- "what does that mean?"
- "explain that chart more"

Flow:
```
+-------------------------------------------+
| previous conversation + new message       |
+-------------------------------------------+
                    |
                    v
+--------------------------+
| load_context             |
+--------------------------+
            |
            v
+--------------------------+
| route_intent             |
| => follow_up_context     |
+--------------------------+
            |
            v
+----------------------------------------------+
| handle_follow_up_context                     |
| -> shorter tool loop                         |
| -> reuse prior context                       |
| -> compose final answer                      |
+----------------------------------------------+
            |
            v
+--------------------------+
| finalize                 |
+--------------------------+
```

## LangGraph Shape

The runtime uses a simple explicit intent graph, not a deeply branching agent graph.

```
+-------+     +----------------+     +----------------+
| START | --> | load_context   | --> | route_intent   |
+-------+     +----------------+     +----------------+
                                             |
                                             +--> handle_small_talk ---------+
                                             +--> handle_irrelevant ---------+
                                             +--> handle_needs_clarification +
                                             +--> handle_query_with_sql -----+
                                             +--> handle_query_without_sql --+
                                             +--> handle_follow_up_sql ------+
                                             +--> handle_follow_up_context --+
                                                                              |
                                                                              v
                                                                        +-----------+
                                                                        | finalize  |
                                                                        +-----------+
                                                                              |
                                                                              v
                                                                          +-------+
                                                                          |  END  |
                                                                          +-------+
```

The important design choice is that all non-trivial routes eventually go through the same answer contract:
- markdown answer text
- citations
- warnings
- optional SQL + SQL results
- metadata for the frontend

## Warnings

`warnings` are non-fatal runtime cautions attached to the final response payload.

They are not the same as hard errors:
- hard errors stop the current path and force the tool loop or the user to correct something
- warnings allow the turn to continue, but record something important about what happened

Current warning sources include:
- SQL guard adjustments, for example:
  - no `LIMIT` was present, so the guard added `LIMIT 200`
  - `SELECT *` was used
- tool/runtime exceptions that were caught and surfaced as cautionary context during the turn

Warnings are persisted with the assistant response and can be shown or inspected later from the payload.

## Runtime Limits

Current enforced limits:
- `get_distinct_values` tool request limit: capped to `200`
- SQL result row limit: `200`
- SQL/context tool-loop turns for new questions: `15`
- SQL/context tool-loop turns for follow-up questions: `6`

SQL-specific behavior:
- if generated SQL has no `LIMIT`, the SQL guard adds `LIMIT 200`
- if generated SQL asks for more than `200` rows, validation fails

These limits exist to keep warehouse queries bounded, keep tool loops from wandering indefinitely, and keep the response/UI payloads manageable.

## Tool Loop Shape

Inside the SQL/context routes, the model runs an explicit tool loop with a bounded number of turns.

```
+------------------------------------+
| build system + conversation msgs   |
+------------------------------------+
                  |
                  v
+------------------------------------+
| LLM chooses tools                  |
+------------------------------------+
                  |
                  v
+------------------------------------+
| get_chart_table_metadata           |
| search_metadata                    |
| get_table_metadata                 |
| get_column_metadata                |
| search_columns_by_name             |
| get_join_paths                     |
| get_related_tables                 |
| get_table_statistics               |
| resolve_time_scope                 |
| read_full_metadata                 |
| get_schema_snippets                |
| get_distinct_values                |
| check_table_row_count              |
| run_sql_query                      |
+------------------------------------+
                  |
                  v
+------------------------------------+
| append tool results to messages    |
+------------------------------------+
                  |
                  v
+------------------------------------+
| next step                          |
| - ask for more tools               |
| - finish with answer draft         |
| - stop after successful SQL        |
+------------------------------------+
```

## Metadata Build

Dashboard chat metadata is not rebuilt on every user message. It is rebuilt manually from AI settings.

Current flow:
- org admin opens AI settings
- trigger metadata build for one dashboard or all dashboards
- backend builds the dashboard-scoped artifact
- artifact is enriched and stored in Postgres JSONB
- runtime loads the persisted artifact on each chat turn

Eligibility is based on:
- org has dbt configured
- org has AI data sharing enabled
- org has `AI_DASHBOARD_CHAT` feature flag enabled

If dbt is not configured:
- metadata cannot be built for that org
- live chat is rejected with "Chat with dashboards is not available because dbt is not configured"

The feature flag and dbt requirement are separate:
- an org can have `AI_DASHBOARD_CHAT` enabled
- but if dbt is missing, chat is still unavailable at runtime

## Main Runtime Data Sources

The runtime combines several different kinds of context:

- dashboard export
  - chart metadata, filters, datasets for the current dashboard
- allowlist
  - dashboard tables plus upstream dbt lineage tables
- metadata artifact
  - observed physical facts plus inferred semantic guidance for the current dashboard tables
- warehouse tools
  - deterministic schema inspection, distinct validation, and SQL execution

These sources are intentionally different:
- dashboard export grounds the runtime in the visible charts
- the metadata artifact drives table, join, and column discovery
- warehouse tools are good for trustworthy data answers

## LangGraph Persistence

This feature now uses official LangGraph Postgres checkpoints for session continuity.

What is persisted in checkpoints:
- dashboard export payload
- allowlist payload
- chart registry payload
- metadata artifact payload + status
- schema snippet payloads
- validated distinct-value payloads
- turn state needed for follow-ups and resume

What is not stored there:
- product transcript rows

Chat history remains in `dashboard_chat_session` / `dashboard_chat_message`. LangGraph owns resumable workflow state; Django models still own the user-visible transcript.

Why this exists:
- a chat session should keep using stable dashboard/metadata context across follow-up turns
- schema lookups and distinct validations should carry across turns
- interrupted runs should be resumable at graph-step boundaries
- continuity should survive process restarts without relying on Django cache

### Shared process-level clients
Location:
- `orchestration/orchestrator.py`

What is reused:
- shared dashboard-chat runtime
- shared OpenAI clients inside their wrappers

Why this exists:
- avoid rebuilding heavy clients/graph objects for every request
- reduce connection churn

### 3. Prompt handling

Prompt lookup works like this:
- read the prompt row from the DB if present
- otherwise fall back to the built-in default in `agents/prompt_template_store.py`

## DB-Backed Logging / Trace

The main trace for this feature lives in Postgres, not just in application logs.

Primary tables:
- `dashboard_chat_session`
- `dashboard_chat_message`

Persisted trace fields:

- `dashboard_chat_session`
  - fields: `org`, `orguser`, `dashboard`, `session_id`
  - gives us: who the conversation belongs to and which dashboard it is scoped to
- `dashboard_chat_message` (`user`)
  - fields: `content`, `client_message_id`, `created_at`
  - gives us: what the user asked and when
- `dashboard_chat_message` (`assistant`)
  - fields: `content`, `payload`, `created_at`
  - gives us: the final answer plus structured trace data
- `dashboard_chat_message` (`assistant`)
  - field: `response_latency_ms`
  - gives us: end-to-end latency from persisted user message to persisted assistant reply
- `dashboard_chat_message` (`assistant`)
  - field: `timing_breakdown`
  - gives us: explicit backend step timings for retracing slow turns later

Assistant `payload` includes:
- intent
- citations
- warnings
- SQL
- SQL results
- usage
- tool call summaries
- response metadata

`timing_breakdown` currently captures:
- `graph_nodes_ms`
  - timings for graph steps like `load_context`, `route_intent`, the route handler node, and `finalize`
- `tool_loop_ms`
  - total time spent inside the tool loop for routed question turns
- `tool_calls_ms`
  - per-tool-call durations
- `runtime_total_ms`
  - total backend runtime time for the turn

This gives us a DB-backed turn trace for questions like:
- which user in which org asked what?
- which session/dashboard was it in?
- what tools ran?
- what SQL ran?
- how long did the response take?
- which part of the backend was slow?

This is the main place to look for later analysis of answer quality, latency, and flow behavior.

There are still normal backend logs as well, but those are secondary compared to the persisted session/message trace.

## File Guide

This is the quickest way to navigate the package.

### Root
- [`config.py`](./config.py)
  - environment-driven runtime and model configuration

### `agents/`
- [`llm_client_interface.py`](./agents/llm_client_interface.py)
  - LLM client protocol used by the runtime
- [`openai_llm_client.py`](./agents/openai_llm_client.py)
  - OpenAI-backed intent classification, tool-loop, and final-answer composition
- [`final_answer_formatting.py`](./agents/final_answer_formatting.py)
  - helpers for structured final answer composition
- [`prompt_template_store.py`](./agents/prompt_template_store.py)
  - DB-backed prompt lookup with built-in defaults

### `context/`
- [`dashboard_table_allowlist.py`](./context/dashboard_table_allowlist.py)
  - dashboard export -> allowlisted tables/dbt lineage and manifest dependency handling

### `metadata/`
- build, enrich, store, and search dashboard-scoped metadata artifacts

### `contracts/`
- [`conversation_contracts.py`](./contracts/conversation_contracts.py)
  - conversation history and follow-up context contracts
- [`intent_contracts.py`](./contracts/intent_contracts.py)
  - intent enums and routing decisions
- [`response_contracts.py`](./contracts/response_contracts.py)
  - final response, citations, usage, tool-call metadata
- [`retrieval_contracts.py`](./contracts/retrieval_contracts.py)
  - retrieved document contracts
- [`sql_contracts.py`](./contracts/sql_contracts.py)
  - SQL validation and schema snippet contracts

### `orchestration/`
- [`orchestrator.py`](./orchestration/orchestrator.py)
  - runtime entry point, shared runtime getter, and backend resume API
- [`checkpoints.py`](./orchestration/checkpoints.py)
  - official LangGraph Postgres checkpoint wiring
- [`state/`](./orchestration/state)
  - grouped graph-state definitions and typed runtime accessors
- [`nodes/`](./orchestration/nodes)
  - graph node handlers, including explicit query/follow-up route files plus `compose_response` and `finalize`
- [`retrieval_support.py`](./orchestration/retrieval_support.py)
  - citation and source-payload helpers
- [`conversation_context.py`](./orchestration/conversation_context.py)
  - conversation-context extraction and follow-up helpers
- [`response_composer.py`](./orchestration/response_composer.py)
  - response format selection and final answer assembly
- [`tool_loop_message_builder.py`](./orchestration/tool_loop_message_builder.py)
  - message-building helpers for the tool loop
- [`source_identifier_parsing.py`](./orchestration/source_identifier_parsing.py)
  - parsing helpers for stored source identifiers
- [`intent_routing.py`](./orchestration/intent_routing.py)
  - graph route selection after intent classification
- [`timing_breakdown.py`](./orchestration/timing_breakdown.py)
  - timing merge helpers for node and tool-loop execution
- [`llm_tools/`](./orchestration/llm_tools)
  - `runtime/` for tool-loop execution and turn context, `implementations/` for concrete tool handlers and SQL helpers

### `sessions/`
- [`session_service.py`](./sessions/session_service.py)
  - create/reuse sessions, persist messages, serialize message payloads

### `warehouse/`
- [`warehouse_access_tools.py`](./warehouse/warehouse_access_tools.py)
  - read-only warehouse helpers for schema, distincts, row counts, SQL execution
- [`sql_guard.py`](./warehouse/sql_guard.py)
  - allowlist enforcement and SQL safety checks

## Websocket + Persistence Integration

Main files:
- `ddpui/websockets/dashboard_chat_consumer.py`
- `sessions/session_service.py`
- `ddpui/celeryworkers/tasks.py`

Important behavior:
- websocket receives `send_message`
- session is validated against the current org/dashboard
- user message is persisted first
- assistant reply is persisted after runtime completion
- duplicate user messages reuse the existing assistant reply when possible
- frontend receives normalized websocket response envelopes, not custom ad hoc payloads

## Practical Debugging Notes

If chat is failing, the fastest places to inspect are:
- websocket consumer for auth/session issues
- `execute_dashboard_chat_turn` for persistence/runtime wiring
- `orchestrator/orchestrator.py` for runtime construction
- `orchestration/checkpoints.py` for LangGraph Postgres persistence setup
- `orchestration/nodes/` for route choice and final response creation
- `metadata/build_service.py` or `metadata/builder.py` if metadata is stale or missing
- `warehouse/sql_guard.py` if SQL is being rejected
- LangGraph checkpoint tables plus `orchestration/state/` if follow-up state behaves inconsistently

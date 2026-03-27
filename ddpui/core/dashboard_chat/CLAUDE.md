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
|    - usually retrieve_docs                   |
|    - may use search_dbt_models               |
|    - may use get_dbt_model_info              |
| -> compose final answer                      |
+----------------------------------------------+
            |
            v
+--------------------------+
| finalize                 |
+--------------------------+
```

These questions are usually answered from:
- vectorized dashboard/org/dbt docs
- deterministic dbt index lookups
- chart metadata in the dashboard export

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
|    - retrieve_docs                           |
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
- retrieval from Chroma per query: `6` results by default from runtime config
- `retrieve_docs` tool request limit: capped to `20`
- `search_dbt_models` tool request limit: capped to `20`
- `list_tables_by_keyword` tool request limit: capped to `50`
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
| retrieve_docs                      |
| get_schema_snippets                |
| search_dbt_models                  |
| get_dbt_model_info                 |
| get_distinct_values                |
| list_tables_by_keyword             |
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

## Chroma Integration

There are two layers here on purpose:

### 1. Generic Chroma transport
Location:
- [`ddpui/utils/vector/chroma/client.py`](../../utils/vector/chroma/client.py)
- [`ddpui/utils/vector/chroma/store.py`](../../utils/vector/chroma/store.py)
- [`ddpui/utils/vector/chroma/types.py`](../../utils/vector/chroma/types.py)

Responsibilities:
- shared HTTP client creation
- create/load/delete/list collections
- get/query/upsert/delete documents
- normalize Chroma result shapes

This layer knows how to talk to Chroma, but does not know anything about dashboards, orgs, or dbt business logic.

### 2. Dashboard-chat vector layer
Location:
- [`vector/org_vector_store.py`](./vector/org_vector_store.py)
- [`vector/vector_documents.py`](./vector/vector_documents.py)
- [`vector/builder.py`](./vector/builder.py)
- [`vector/building.py`](./vector/building.py)
- [`vector/embeddings.py`](./vector/embeddings.py)

Responsibilities:
- build dashboard-chat collection names
- define dashboard-chat document/source types
- build embeddings
- filter retrieval by source type and dashboard id
- build org-scoped collections and rebuild them from app/dbt context

### What Gets Vectorized

Current source types:
- `org_context`
- `dashboard_context`
- `dashboard_export`
- `dbt_manifest`
- `dbt_catalog`

At retrieval time, the runtime can search one or more of these source types depending on the question.

## Background Vector Refresh

Vector context is not rebuilt on every user message. It is refreshed in the background.

Main Celery tasks:
- `schedule_dashboard_chat_context_builds`
- `build_dashboard_chat_context_for_org`

Periodic schedule:
- every 3 hours via Celery beat

Flow:
```
+------------------------------+
| Celery beat                  |
+------------------------------+
               |
               v
+------------------------------+
| schedule_dashboard_chat_     |
| context_builds               |
+------------------------------+
               |
               v
+------------------------------+
| find eligible orgs           |
+------------------------------+
               |
               v
+------------------------------+
| enqueue one build per org    |
+------------------------------+
               |
               v
+------------------------------+
| build_dashboard_chat_        |
| context_for_org              |
+------------------------------+
               |
               v
+------------------------------+
| acquire Redis lock           |
| generate dbt docs if needed  |
| build vector documents       |
| write versioned collection   |
| update vector_last_ingested  |
| GC inactive collections      |
| release lock                 |
+------------------------------+
```

Eligibility is based on:
- org has dbt configured
- org has AI data sharing enabled
- org has `AI_DASHBOARD_CHAT` feature flag enabled

If dbt is not configured:
- vector context is not built for that org
- the background build task skips the org
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
- compact dbt index
  - deterministic model/column/lineage lookup for allowlisted dbt resources
- vector retrieval
  - semantic matching across org/dashboard/dbt docs
- warehouse tools
  - deterministic schema inspection, distinct validation, and SQL execution

These sources are intentionally different:
- vector retrieval is good for fuzzy semantic matching
- the compact dbt index is good for deterministic dbt lookups (ex: upstream models)
- warehouse tools are good for trustworthy data answers

## LangGraph Persistence

This feature now uses official LangGraph Postgres checkpoints for session continuity.

What is persisted in checkpoints:
- dashboard export payload
- allowlist payload
- compact dbt index
- schema snippet payloads
- validated distinct-value payloads
- turn state needed for follow-ups and resume

What is not stored there:
- product transcript rows

Chat history remains in `dashboard_chat_session` / `dashboard_chat_message`. LangGraph owns resumable workflow state; Django models still own the user-visible transcript.

Why this exists:
- a chat session should keep using stable dashboard/dbt context across follow-up turns
- schema lookups and distinct validations should carry across turns
- interrupted runs should be resumable at graph-step boundaries
- continuity should survive process restarts without relying on Django cache

### Shared process-level clients
Location:
- `orchestration/orchestrator.py`
- `ddpui/utils/vector/chroma/client.py`

What is reused:
- shared dashboard-chat runtime
- shared Chroma HTTP client
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
  - fields: `org`, `orguser`, `dashboard`, `session_id`, `vector_collection_name`
  - gives us: who the conversation belongs to, which dashboard it is scoped to, and which vector collection was pinned for that session
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
  - environment-driven runtime, vector, and source configuration
- [`events.py`](./events.py)
  - websocket event helpers / channel group naming

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
  - dashboard export -> allowlisted tables/dbt lineage -> compact dbt index
- [`dbt_docs_artifacts.py`](./context/dbt_docs_artifacts.py)
  - dbt docs generation/loading helpers for manifest/catalog artifacts

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
  - grouped graph-state definitions, payload codecs, and typed runtime accessors
- [`nodes/`](./orchestration/nodes)
  - graph node handlers, including explicit query/follow-up route files plus `compose_response` and `finalize`
- [`retrieval_support.py`](./orchestration/retrieval_support.py)
  - Chroma retrieval + citations
- [`conversation_context.py`](./orchestration/conversation_context.py)
  - conversation-context extraction and follow-up helpers
- [`response_composer.py`](./orchestration/response_composer.py)
  - response format selection and final answer assembly
- [`tool_loop_message_builder.py`](./orchestration/tool_loop_message_builder.py)
  - message-building helpers for the tool loop
- [`source_identifier_parsing.py`](./orchestration/source_identifier_parsing.py)
  - parsing helpers for chart/dbt source identifiers
- [`intent_routing.py`](./orchestration/intent_routing.py)
  - graph route selection after intent classification
- [`timing_breakdown.py`](./orchestration/timing_breakdown.py)
  - timing merge helpers for node and tool-loop execution
- [`llm_tools/`](./orchestration/llm_tools)
  - `runtime/` for tool-loop execution and turn context, `implementations/` for concrete tool handlers and SQL helpers

### `sessions/`
- [`session_service.py`](./sessions/session_service.py)
  - create/reuse sessions, persist messages, serialize message payloads

### `vector/`
- [`vector_documents.py`](./vector/vector_documents.py)
  - vector document dataclasses, source types, collection naming helpers
- [`org_vector_store.py`](./vector/org_vector_store.py)
  - dashboard-chat adapter on top of the shared Chroma wrapper
- [`org_vector_context_build_service.py`](./vector/org_vector_context_build_service.py)
  - document chunking, vector document building, and org-level rebuild workflow

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
- `vector/building.py` if retrieval data is stale or missing
- `warehouse/sql_guard.py` if SQL is being rejected
- LangGraph checkpoint tables plus `orchestration/state/` if follow-up state behaves inconsistently

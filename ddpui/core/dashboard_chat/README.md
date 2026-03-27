# Dashboard Chat

This package implements dashboard-scoped chat for one dashboard at a time.

The current backend shape is:
- `DashboardChatConsumer` handles the websocket/session/message lifecycle
- `DashboardChatRuntime` runs a LangGraph workflow per turn
- LangGraph state is checkpointed to Postgres through the official `PostgresSaver`
- product transcript history still lives in `DashboardChatSession` and `DashboardChatMessage`

## Runtime Flow

```
+-------------------------+
| websocket consumer      |
| persist user message    |
+-------------------------+
            |
            v
+-------------------------+
| DashboardChatRuntime    |
| run / resume            |
+-------------------------+
            |
            v
+-------------------------+
| load_context            |
+-------------------------+
            |
            v
+-------------------------+
| route_intent            |
+-------------------------+
            |
            v
+-----------------------------------------------+
| one route node                                |
| - handle_small_talk                           |
| - handle_irrelevant                           |
| - handle_needs_clarification                  |
| - handle_query_with_sql                       |
| - handle_query_without_sql                    |
| - handle_follow_up_sql                        |
| - handle_follow_up_context                    |
+-----------------------------------------------+
            |
            v
+-------------------------+
| compose_response        |
+-------------------------+
            |
            v
+-------------------------+
| finalize                |
+-------------------------+
            |
            v
+-------------------------+
| persist assistant reply |
+-------------------------+
```

## Package Layout

### `agents/`
- LLM-facing client abstractions and implementations
- `llm_client_interface.py` defines the runtime-facing LLM contract
- `openai_llm_client.py` contains the OpenAI-backed implementation
- `final_answer_formatting.py` formats structured final-answer payloads
- `prompt_template_store.py` loads prompt templates with DB-backed overrides

### `context/`
- `dashboard_table_allowlist.py` builds the dashboard-scoped table/dbt allowlist
- `dbt_docs_artifacts.py` generates and loads manifest/catalog artifacts for chat context builds

### `contracts/`
- `conversation_contracts.py`, `intent_contracts.py`, `response_contracts.py`, `retrieval_contracts.py`, `sql_contracts.py`

### `orchestration/`
- graph wiring and all per-turn runtime logic

- `conversation_context.py`: extracts reusable follow-up context from prior messages
- `tool_loop_message_builder.py`: builds tool-loop prompt stacks
- `response_composer.py`: builds final answer text and response-format decisions
- `retrieval_support.py`: retrieval normalization and citation helpers
- `source_identifier_parsing.py`: parses chart/dbt identifiers from stored vector sources
- `intent_routing.py`: maps classified intents to route node names
- `timing_breakdown.py`: merges timing payloads across node/tool execution

#### `orchestration/state/`
- `graph_state.py`: JSON-safe LangGraph state contract
- `payload_codec.py`: serialize/deserialize checkpoint payloads
- `accessors.py`: reconstruct typed runtime views from persisted payloads

#### `orchestration/nodes/`
- graph nodes only
- query/follow-up routes each have explicit node files in `nodes/`
- `compose_response.py` builds the final `DashboardChatResponse`
- `finalize.py` enriches the finished response with metadata and warehouse citations

#### `orchestration/llm_tools/`
- `runtime/`: tool-loop execution, turn context, tool specifications
- `implementations/`: concrete LLM-callable tools and SQL helpers

### `sessions/`
- `session_service.py`: session/message persistence and message serialization

### `vector/`
- `vector_documents.py`: vector document contracts and collection naming
- `org_vector_store.py`: org-scoped Chroma adapter
- `org_vector_context_build_service.py`: document building and org-level vector rebuilds

### `warehouse/`
- `warehouse_access_tools.py`: schema lookups, distincts, row counts, SQL execution
- `sql_guard.py`: allowlist enforcement and SQL safety checks

## Agents vs Nodes

These are different layers:

- `agents/` are LLM client adapters
  - classify intent
  - run the tool loop
  - compose final answer text
  - compose small-talk text

- `orchestration/nodes/` are LangGraph workflow steps
  - load context
  - route the turn
  - execute one route
  - compose/finalize the response

In short:
- `agents` = how the backend talks to the model
- `nodes` = how the workflow is structured

## Checkpointing

Checkpointing is wired in:
- `orchestration/checkpoints.py`
- `orchestration/orchestrator.py`

`checkpoints.py`:
- builds the Postgres connection info
- creates the LangGraph `PostgresSaver`
- calls `setup()` so LangGraph manages its own checkpoint tables

`orchestrator.py`:
- compiles a persistent graph with that saver
- uses `thread_id = session_id`
- exposes backend `resume(session_id, checkpoint_id=None)`

The checkpointed state is durable in Postgres and is separate from the product transcript tables.

## How Checkpoints Are Written

LangGraph does not write a checkpoint “for every edge”.

Edges are just routing links. The checkpointer persists state when the graph commits work for a step:
- after node execution updates state
- when LangGraph records checkpoint writes/versions for that step

So the mental model is:
- node runs
- state updates are committed
- checkpoint is saved
- graph follows the next edge

That is why checkpointing is meaningful at graph-step boundaries, not in the middle of a blocking OpenAI call or warehouse query.

## Resume Behavior

Backend resume is currently supported.

What exists now:
- durable checkpointed state in Postgres
- `thread_id = session_id`
- backend `resume(session_id, checkpoint_id=None)`

What does not exist yet:
- frontend resume UX
- mid-call interruption/resume inside a single blocking external call

Resume works at graph-step boundaries, not inside an in-flight tool/LLM/database call.

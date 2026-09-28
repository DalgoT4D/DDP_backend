"""Shared settings for the AI package: models, token budgets, loop limits.

Only values read by more than one module, or that someone tuning the agents
would look for together, live here. Values private to one module (chart grid
sizes, render caps, guard node lists) stay next to the code that uses them.

This module is import-light on purpose — the models module imports
MAX_ORG_MEMORY_CHARS at class-definition time, so nothing here may import
ddpui.
"""

# ---------------------------------------------------------------------------
# Feature flag
# ---------------------------------------------------------------------------

CHAT_WITH_DATA_FLAG = "CHAT_WITH_DATA"

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

# The agents' model (SQL agent + platform guide share it)
MODEL_ENV_VAR = "CHAT_WITH_DATA_MODEL"
DEFAULT_MODEL = "claude-sonnet-5"

# Max tokens per model response; answers are short prose + small tables
MODEL_MAX_TOKENS = 4096

# Models a user may pick in the chat UI. Only entries whose provider key is
# present in the environment are offered (credentials move to org settings
# later). The id doubles as the init_chat_model spec — provider inferred.
MODEL_OPTIONS = [
    {"id": "claude-sonnet-5", "label": "Claude Sonnet", "key_env": "ANTHROPIC_API_KEY"},
    {"id": "gpt-5.5", "label": "OpenAI GPT", "key_env": "OPENAI_API_KEY"},
]

# The cheap model behind every one-shot call in llm_calls/. Each call keeps
# its own env var so one can be upgraded without touching the others.
FAST_MODEL = "claude-haiku-4-5"

ROUTER_MODEL_ENV_VAR = "CHAT_WITH_DATA_ROUTER_MODEL"
ROUTER_MAX_TOKENS = 300

TITLE_MODEL_ENV_VAR = "CHAT_WITH_DATA_TITLE_MODEL"
TITLE_MAX_TOKENS = 50

VALIDATOR_MODEL_ENV_VAR = "CHAT_WITH_DATA_VALIDATOR_MODEL"
VALIDATOR_MAX_TOKENS = 400

REFLECTION_MODEL_ENV_VAR = "CHAT_WITH_DATA_REFLECTION_MODEL"
REFLECTION_MAX_TOKENS = 300

# ---------------------------------------------------------------------------
# Agent loop budgets
# ---------------------------------------------------------------------------

# Upper bound on GRAPH STEPS per turn — backstop against runaway loops.
# Every before_model/after_model hook is its own graph node (a wrap_model_call
# hook like org_system_prompt or clear_old_tool_results wraps the model call
# in place and adds none). One model⇄tool cycle now costs ~5 steps (2
# before_model: sql_retry_limiter, trim_history;
# + model; + 1 after_model: the HITL approval gate; + tools) — down from ~16
# when 5 PIIMiddleware instances each added a before_model AND an after_model
# node. 160 ≈ headroom for ~32 cycles now (was ~10); a legitimate heavy turn
# on a messy warehouse uses ~12 (schemas → tables → details ×3 → profile ×2 →
# sql ×5 with retries — MAX_SQL_ATTEMPTS is 5). profile_column and execute_sql
# both pausing for approval (Task 7) doesn't erode this: each pause/resume is
# a fresh invocation with its own step budget. The real runaway guard is
# sql_retry_limiter, not this ceiling.
# If you add middleware, re-check test_realistic_discovery_turn_fits_in_the_recursion_limit.
RECURSION_LIMIT = 160

# Failed execute_sql calls allowed per user question before the loop is stopped;
# the system prompt tells the model the same number so it stops gracefully first.
# 5 (was 3): real warehouses with case-sensitive Airbyte tables and non-obvious
# join paths burn 2-3 attempts on discovery before the query that works.
MAX_SQL_ATTEMPTS = 5

# Token budget for the model request; old turns beyond this are trimmed from the
# request (NOT from the checkpointed conversation, which the UI renders in full)
HISTORY_TOKEN_BUDGET = 60_000

# Clear bulky old query results from the request once total context passes this
TOOL_RESULT_CLEAR_TRIGGER_TOKENS = 40_000
# ...but always keep the most recent tool results intact
TOOL_RESULTS_KEPT = 5

# ---------------------------------------------------------------------------
# Org memory
# ---------------------------------------------------------------------------

# ~1,250 tokens. The system prompt is rebuilt for EVERY model call (~5-13 per
# turn), so memory tokens multiply — hence 5k, not PostHog's 10k.
MAX_ORG_MEMORY_CHARS = 5000

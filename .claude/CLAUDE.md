# DDP_backend — Project Guide

Django/Ninja backend for Dalgo. Multi-tenant ELT platform connecting Airbyte (ingest), Prefect (orchestration), dbt (transforms), and Postgres/BigQuery (warehouses).

## Architecture at a glance

```
ddpui/
├── api/{module}_api.py          # HTTP layer — thin: validate, permission, delegate
├── core/{module}/
│   ├── {module}_service.py      # Business logic, transactions, validation
│   ├── {module}_operations.py   # Domain operations (optional)
│   └── exceptions.py            # Feature-specific exceptions
├── schemas/{module}_schema.py   # Pydantic schemas
├── models/{module}.py           # Django ORM models
└── utils/                       # Shared helpers
```

| Layer         | Responsible for                                                                       | Should not                                          |
|---------------|---------------------------------------------------------------------------------------|-----------------------------------------------------|
| **API**       | HTTP I/O, permissions, schema validation, exception → HTTP, `api_response` wrapping   | Business logic, ORM queries, external calls         |
| **Core**      | Business logic, orchestration, schema-validation before external calls, transactions  | HTTP concerns, direct API responses                 |
| **Schema**    | Request/response contracts, data transformation                                       | Business logic, DB ops                              |
| **Model**     | DB schema, relationships, simple methods                                              | Business logic, API concerns, `to_json`/`to_dict`   |
| **Exception** | Feature-specific error classes                                                        | HTTP codes (mapped at API layer)                    |

Dependency direction: **API → Core → Models**. One-way only.

## Non-negotiable invariants

1. **Multi-tenancy.** Every domain object has `org` FK; every query filters by `org`. `request.orguser.org` is the source of truth — never trust client-supplied org IDs.
2. **Permissions.** Every endpoint has `@has_permission([...])`. No exceptions, even internal.
3. **Response wrapper.** Always return via `api_response(success=True, data=..., message=...)`. Never a raw dict.
4. **Schema validation at boundaries.** Pydantic via Ninja for inbound; schemas with `extra="forbid"` before external service calls (dbt, Airbyte, Prefect).
5. **No local imports.** All imports at the top of the file.
6. **No `to_json`/`to_dict` on models.** Use response schemas with `from_model()`.
7. **Core feature `__init__.py` stays empty** — no re-exports, no `__all__`.
8. **Service-layer signatures take a payload schema + org/orguser context.** Never `func(arg1, arg2, arg3, ...)` for fields that belong in a schema.
9. **Always chain exceptions with `from err`.** Inside any `except` block, re-raise with `from err` (or `from None` if intentionally suppressing). Bare `raise NewError(...)` severs the traceback.

## Skills (loaded on context match)

- **api-endpoint** — Scaffold a new endpoint (layout, router, response, mistakes)
- **testing** — Write pytest tests; subfiles: fixtures, api-tests, mocking
- **warehouse-client** — Talk to a client's warehouse (Postgres / BigQuery) via `WarehouseFactory.get_warehouse_client(...)`; never `old_client`
- **coding-standards** — Recurring rules: service signature shape, exception chaining, import placement

## Quick commands

Always invoke Python via `uv run` — tests, scripts, `manage.py`, everything.

```bash
uv sync                                                  # install deps

# Tests, lint, format
uv run pytest ddpui/tests -v
uv run pytest ddpui/tests/api_tests/test_X.py::test_Y -v
uv run pytest ddpui/tests --cov
uv run black .
uv run pylint ddpui/

# Django management
uv run python manage.py migrate
uv run python manage.py loaddata seed/*.json
uv run python manage.py create-system-orguser
```

## Conventions across the codebase

- **Logging.** `CustomLogger("ddpui.module_name")` — JSON-structured, auto-includes org/user/request correlation.
- **Roles & permissions.** Defined in fixtures `001_roles.json` / `002_permissions.json` / `003_role_permissions.json`. Load via `seed_db` fixture in tests.
- **External services** (module imports + attribute calls):
  - `from ddpui.ddpairbyte import airbyte_service` → `airbyte_service.create_source(...)`
  - `from ddpui.ddpprefect import prefect_service` → `prefect_service.get_flow_run(...)`
  - `from ddpui.utils.warehouse.client.warehouse_factory import WarehouseFactory` → `WarehouseFactory.get_warehouse_client(org_warehouse)`
- **Webhooks.** Prefect → `POST /webhooks/v1/notification/` with `X-Notification-Key` header. Handler queues a Celery task.
- **Redis.** TaskProgress / SingleTaskProgress, TaskLock, caches. Access via `RedisClient.get_instance()`.

## Testing workflow

Write one test → run it → fix → run again → next test. Never batch-write multiple tests before running each. See the `testing` skill for full patterns.

## Daily Briefing Email

When composing a daily briefing email, always:

1. **Read `.claude/daily_briefing_template.html` first** — use it as the outer shell (header, accent stripe, body card, footer). Never regenerate the shell from scratch.
2. **Do not alter the template file itself** — only fill in the `{{GREETING}}`, `{{DATE_LINE}}`, and `{{BODY_CONTENT}}` placeholders.
3. **Key fixes baked into the template (do not revert):**
   - Header `td` has `background-color:#1B2A4A` declared *before* the gradient — Gmail fallback so white title text stays readable on a white background.
   - Accent stripe uses three separate solid-colour `td` cells, not a single gradient (Gmail strips gradients).
4. Use `background-color:` not the `background:` shorthand for all other coloured elements.

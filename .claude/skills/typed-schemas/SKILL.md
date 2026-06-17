---
name: typed-schemas
description: Prefer typed Pydantic/Ninja schemas over generic dicts when passing structured data through Dalgo backend code. Use when writing or modifying functions whose parameters represent a known shape (configs, payloads, deserialized JSONField values, frontend payloads). Skip for genuinely free-form / variable-shape data.
---

# Typed Schemas Over Generic Dicts

When a function in `DDP_backend/` accepts structured data, prefer a typed schema (Pydantic / Django Ninja `Schema`) over `dict` or `dict[str, Any]`. This catches malformed inputs at the boundary, gives editor autocomplete, and prevents the silent typo class of bugs (`cfg.get("schmea_name")` returning `None`).

## When this applies

Trigger this skill whenever you are about to:

- Add a function/method parameter typed as `dict`, `dict[str, Any]`, or `Optional[dict]` that represents a known shape.
- Touch code that reads a Django `JSONField` and does `cfg.get("foo")` style access downstream.
- Write a new dry-run / preview / evaluate function that mirrors an existing API payload.
- Refactor a function that uses `**kwargs` to receive structured config.

Skip this skill for:
- Genuinely free-form data (e.g., raw warehouse query rows, arbitrary user metadata, Sentry context dicts).
- Throwaway scripts / one-off management commands.
- Internal helpers where the dict never leaves the function.

## The procedure

### Step 1 — check if a schema already exists

Look in `ddpui/schemas/` for a Pydantic / Ninja `Schema` covering the shape you need. The frontend payload schemas and the DB JSONField shapes are usually already defined there because the API layer needs them. Don't define a new schema if one exists.

```python
# Existing: ddpui/schemas/alert_schema.py
class StandaloneConfig(Schema):
    schema_name: str
    table_name: str
    column: Optional[str] = None
    aggregation: Optional[str] = None
    column_expression: Optional[str] = None
    filters: List[FilterClause] = []
```

If you find one, use it. Skip to Step 3.

### Step 2 — create a schema only if no equivalent exists

Add it to the relevant module under `ddpui/schemas/`. Use `Schema` (the Ninja re-export of Pydantic `BaseModel`) so it works in API endpoints if it ever needs to. Mark optional fields with `Optional[...] = None`; give sensible defaults for lists/dicts (`[]`, `{}`).

If your schema mirrors a DB JSONField, name it after the field's purpose (e.g., `StandaloneConfig` for `Alert.standalone_config`).

### Step 3 — type the function signature

Replace `dict` with the schema class:

```python
# Before
def _execute_standalone(standalone_config: dict, ...) -> ...:
    schema_name = standalone_config.get("schema_name")
    table_name = standalone_config.get("table_name")
    if not schema_name or not table_name:
        raise AlertValidationError("schema_name and table_name are required")

# After
def _execute_standalone(standalone_config: StandaloneConfig, ...) -> ...:
    # Pydantic already validated required fields at parse time
    qb.fetch_from(standalone_config.table_name, standalone_config.schema_name)
```

The required-field validation moves from your function body up to the schema parse call. Don't reimplement what Pydantic already does.

### Step 4 — parse at the boundary

The function body should never see a raw dict. Convert at the entry point:

- **Reading from a Django JSONField:** parse once at the call site.
  ```python
  if alert.standalone_config:
      cfg = StandaloneConfig(**alert.standalone_config)
      _execute_standalone(cfg, org_warehouse)
  ```
- **Receiving a dict from an external function (dry-run, API):** parse it before passing on.
  ```python
  cfg = StandaloneConfig(**raw_dict)  # raises Pydantic ValidationError on bad input
  ```

Let `Pydantic.ValidationError` propagate or catch it once at a sensible layer (API handler, evaluator wrapper) — don't sprinkle field-by-field `if not foo: raise` checks in business logic.

### Step 5 — use typed attribute access in the body

Replace every `cfg.get("field")` with `cfg.field`. Iterate typed sub-objects instead of dicts:

```python
# Before
for f in standalone_config.get("filters") or []:
    fcol = f.get("column")
    op = (f.get("operator") or "").lower()

# After
for f in standalone_config.filters:           # List[FilterClause], never None
    op = (f.operator or "").lower()
    if not f.column or not op:
        continue
```

### Step 6 — handle cross-field rules in the function, not the schema

Pydantic validates per-field. Cross-field rules ("either A or B must be set", "if X then Y must be set") stay in the consuming function:

```python
def _execute_standalone(cfg: StandaloneConfig, ...) -> ...:
    if not cfg.column_expression and not cfg.aggregation:
        raise AlertValidationError(
            "aggregation is required when column_expression is empty"
        )
```

Don't try to express these as `@model_validator` unless they're truly schema-level invariants that every consumer needs.

## Anti-patterns to avoid

**Don't** wrap the parse in a custom helper that re-implements Pydantic's validation:

```python
# Bad — duplicates what Pydantic already does
def _parse_standalone_config(cfg) -> StandaloneConfig:
    if not cfg.get("schema_name"):
        raise AlertValidationError("schema_name required")
    if not cfg.get("table_name"):
        raise AlertValidationError("table_name required")
    return StandaloneConfig(**cfg)
```

Just call `StandaloneConfig(**cfg)` directly. Pydantic raises `ValidationError` with the exact field that failed.

**Don't** accept `Union[dict, StandaloneConfig]` "for convenience":

```python
# Bad — pushes the type uncertainty into the function body
def _execute_standalone(cfg: dict | StandaloneConfig, ...) -> ...:
    if isinstance(cfg, dict):
        cfg = StandaloneConfig(**cfg)
```

Parse at the boundary; the function takes the typed object only.

**Don't** stringify-then-parse:

```python
# Bad — round-trips through JSON for no reason
cfg = StandaloneConfig(**json.loads(json.dumps(alert.standalone_config)))
```

Django JSONFields return plain dicts. Pass directly.

## Quick reference

| Situation | Do |
|---|---|
| Function param is a known config shape | Type it as the schema class |
| Reading `JSONField` | `Schema(**field_value)` at the call site |
| Want to validate required fields | Let Pydantic raise `ValidationError` |
| Cross-field rule ("A xor B required") | Raise `AlertValidationError` in the body |
| Dict shape doesn't have a stable contract | `dict[str, Any]` is honest — keep it |
| Frontend sends typed payload | Schema already exists in `ddpui/schemas/` — find it |

## Related context

- Pydantic / Ninja `Schema` is the same class — Ninja just re-exports `BaseModel`.
- Pydantic `ValidationError` is a clean structured error; serialize it via `e.errors()` if surfacing to a user.
- For Django models, the source of truth is the Django model field. Schemas are for the *wire format* (API payloads + JSONField contents).

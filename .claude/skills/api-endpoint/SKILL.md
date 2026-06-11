---
name: api-endpoint
description: Scaffold and ship a new HTTP endpoint in the Django/Ninja backend using the API → Core → Schema → Exceptions layout. Use when adding a new route, creating a new module's routes, or extending an existing router with another verb.
---

# api-endpoint

Use this when adding or modifying an HTTP route. Look up the four-layer scaffold, then implement.

## The four places you'll edit

```
ddpui/
├── api/{module}_api.py              # HTTP layer
├── core/{module}/
│   ├── {module}_service.py          # Business logic
│   └── exceptions.py                # Custom exceptions
├── schemas/{module}_schema.py       # Pydantic schemas
└── models/{module}.py               # Django model (often pre-existing)
```

## Canonical endpoint

```python
from ninja import Router
from ninja.errors import HttpError

from ddpui.auth import has_permission
from ddpui.models.org_user import OrgUser
from ddpui.utils.custom_logger import CustomLogger
from ddpui.utils.response_wrapper import api_response, ApiResponse

from ddpui.core.widget.widget_service import WidgetService
from ddpui.core.widget.exceptions import WidgetValidationError
from ddpui.schemas.widget_schema import WidgetCreate, WidgetResponse

logger = CustomLogger("ddpui.widget_api")
widget_router = Router()


@widget_router.post("/", response=ApiResponse[WidgetResponse])
@has_permission(["can_create_widgets"])
def create_widget(request, payload: WidgetCreate):
    """Create a new widget"""
    orguser: OrgUser = request.orguser
    try:
        widget = WidgetService.create_widget(payload, orguser)
        return api_response(
            success=True,
            data=WidgetResponse.from_model(widget),
            message="Widget created successfully",
        )
    except WidgetValidationError as err:
        raise HttpError(400, str(err)) from err
```

## Rules

1. **Router name** = `{module}_router`. Never plain `router`.
2. **Every endpoint** has `@has_permission([...])`. No exceptions.
3. **Wrap responses** with `api_response(success=True, data=..., message=...)`. Never return a raw dict.
4. **Validate via Pydantic** — annotate `payload: SomeSchema` in the function signature; Ninja validates automatically. Don't manually parse a `dict`.
5. **Delegate to a service** — the API function only does: parse → call service → convert model → wrap. No ORM queries, no business logic, no external service calls in this file.
6. **Map exceptions explicitly:**

   | Core exception              | HTTP |
   |-----------------------------|------|
   | `*NotFoundError`            | 404  |
   | `*ValidationError`          | 400  |
   | `*PermissionError`          | 403  |
   | `*ExternalServiceError`     | 502  |
   | other                       | 500 (log + generic message) |

7. **No local/inline imports.** All imports at the top of the file.
8. **`from_model()` on every response schema** — defined on the schema, called by the API.
9. **Filter by org** in every service query (`Widget.objects.get(id=..., org=org)`). Multi-tenancy is enforced by the service, not the framework.

## Pre-flight checklist

- [ ] Router registered under `/api/{module}/` in the main NinjaAPI setup
- [ ] Every endpoint has `@has_permission`
- [ ] Every endpoint uses `api_response`
- [ ] Every known exception has a `try/except` with the right HTTP code
- [ ] Service-level tests written
- [ ] `uv run black .` clean, `uv run pylint ddpui/` clean

## Deeper material

For the full multi-endpoint CRUD module (list/get/create/update/delete), exception class boilerplate, pagination, the `api_response` JSON shape, schema field cheatsheet, and validating payloads before external service calls — see [REFERENCE.md](REFERENCE.md).

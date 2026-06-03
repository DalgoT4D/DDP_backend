---
name: coding-standards
description: Python coding standards for the DDP_backend covering service-layer function signatures (payload + orguser shape), exception chaining with `raise ... from err`, and import placement (top-of-file only). Use when writing or refactoring service-layer code, reviewing a diff or PR, asking "is this the right pattern", or finalizing code.
---

# coding-standards

## 1. Service-layer signatures: `func(payload, orguser)`

The API layer calls the core/service layer through a Pydantic schema (the payload) plus the org/orguser context. Service functions do not accept loose args for fields that belong in a schema.

```python
# ✗ BAD
def create_widget(title: str, description: str, connector: str, orguser: OrgUser) -> Widget:
    ...
WidgetService.create_widget(payload.title, payload.description, payload.connector, orguser)
```

```python
# ✓ GOOD
def create_widget(data: WidgetCreate, orguser: OrgUser) -> Widget:
    ...
WidgetService.create_widget(payload, orguser)
```

Single-value lookups like `get_widget(widget_id, org)` are fine. The rule applies when a function takes multiple fields that belong together.

## 2. `raise ... from err` — always chain exceptions

In an `except` block, re-raise with `from err` to preserve the original cause.

```python
# ✗ BAD
try:
    widget = WidgetService.get_widget(widget_id, org)
except WidgetNotFoundError:
    raise HttpError(404, "widget not found")
```

```python
# ✓ GOOD
try:
    widget = WidgetService.get_widget(widget_id, org)
except WidgetNotFoundError as err:
    raise HttpError(404, "widget not found") from err
```

To intentionally suppress the cause, use `from None`.

## 3. Global imports — never inline

All imports at the top of the file.

```python
# ✗ BAD
def send_reset_link(email: str) -> None:
    from django.conf import settings
    from ddpui.utils import awsses
    awsses.send_signup_email(email, settings.FRONTEND_URL + "/reset")
```

```python
# ✓ GOOD
from django.conf import settings
from ddpui.utils import awsses


def send_reset_link(email: str) -> None:
    awsses.send_signup_email(email, settings.FRONTEND_URL + "/reset")
```

Only break this rule for a genuine circular import or an optional/slow dependency you don't want at module load — and comment why.

# api-endpoint — Reference

Full code samples for a CRUD module called `widget`. Copy and rename.

## Folder layout

```
ddpui/
├── api/widget_api.py
├── core/widget/
│   ├── __init__.py                  # keep empty — no re-exports, no __all__
│   ├── widget_service.py
│   └── exceptions.py
├── schemas/widget_schema.py
└── models/widget.py
```

## `core/widget/exceptions.py`

```python
class WidgetError(Exception):
    def __init__(self, message: str, error_code: str = "WIDGET_ERROR"):
        self.message = message
        self.error_code = error_code
        super().__init__(self.message)


class WidgetNotFoundError(WidgetError):
    def __init__(self, widget_id: int):
        super().__init__(f"Widget with id {widget_id} not found", "WIDGET_NOT_FOUND")
        self.widget_id = widget_id


class WidgetValidationError(WidgetError):
    def __init__(self, message: str):
        super().__init__(message, "WIDGET_VALIDATION_ERROR")


class WidgetPermissionError(WidgetError):
    def __init__(self, message: str = "Permission denied"):
        super().__init__(message, "WIDGET_PERMISSION_DENIED")


class WidgetExternalServiceError(WidgetError):
    def __init__(self, service: str, message: str):
        super().__init__(f"{service} error: {message}", "WIDGET_EXTERNAL_ERROR")
        self.service = service
```

## `schemas/widget_schema.py`

```python
from datetime import datetime
from typing import List, Optional

from ninja import Field, Schema


class WidgetCreate(Schema):
    title: str = Field(..., min_length=1, max_length=255)
    description: Optional[str] = Field(None, max_length=1000)


class WidgetUpdate(Schema):
    title: Optional[str] = Field(None, min_length=1, max_length=255)
    description: Optional[str] = Field(None, max_length=1000)


class WidgetResponse(Schema):
    id: int
    title: str
    description: Optional[str]
    created_at: datetime
    updated_at: datetime

    @classmethod
    def from_model(cls, widget) -> "WidgetResponse":
        return cls(
            id=widget.id,
            title=widget.title,
            description=widget.description,
            created_at=widget.created_at,
            updated_at=widget.updated_at,
        )


class WidgetListResponse(Schema):
    data: List[WidgetResponse]
    total: int
    page: int
    page_size: int
```

## `core/widget/widget_service.py`

```python
from typing import List, Optional, Tuple

from django.db.models import Q

from ddpui.core.widget.exceptions import (
    WidgetNotFoundError,
    WidgetPermissionError,
    WidgetValidationError,
)
from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser
from ddpui.models.widget import Widget
from ddpui.schemas.widget_schema import WidgetCreate, WidgetUpdate
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui.core.widget")


class WidgetService:
    @staticmethod
    def get_widget(widget_id: int, org: Org) -> Widget:
        try:
            return Widget.objects.get(id=widget_id, org=org)
        except Widget.DoesNotExist:
            raise WidgetNotFoundError(widget_id)

    @staticmethod
    def list_widgets(
        org: Org,
        page: int = 1,
        page_size: int = 10,
        search: Optional[str] = None,
    ) -> Tuple[List[Widget], int]:
        query = Q(org=org)
        if search:
            query &= Q(title__icontains=search)
        qs = Widget.objects.filter(query).order_by("-updated_at")
        total = qs.count()
        offset = (page - 1) * page_size
        return list(qs[offset : offset + page_size]), total

    @staticmethod
    def create_widget(data: WidgetCreate, orguser: OrgUser) -> Widget:
        if Widget.objects.filter(org=orguser.org, title=data.title).exists():
            raise WidgetValidationError(
                f"Widget with title '{data.title}' already exists"
            )
        widget = Widget.objects.create(
            title=data.title,
            description=data.description,
            org=orguser.org,
            created_by=orguser,
            last_modified_by=orguser,
        )
        logger.info(f"Created widget {widget.id} for org {orguser.org.id}")
        return widget

    @staticmethod
    def update_widget(
        widget_id: int,
        org: Org,
        orguser: OrgUser,
        data: WidgetUpdate,
    ) -> Widget:
        widget = WidgetService.get_widget(widget_id, org)
        if data.title is not None:
            widget.title = data.title
        if data.description is not None:
            widget.description = data.description
        widget.last_modified_by = orguser
        widget.save()
        return widget

    @staticmethod
    def delete_widget(widget_id: int, org: Org, orguser: OrgUser) -> bool:
        widget = WidgetService.get_widget(widget_id, org)
        if widget.created_by != orguser:
            raise WidgetPermissionError("You can only delete widgets you created.")
        widget.delete()
        return True
```

## `api/widget_api.py`

```python
from typing import Optional

from ninja import Router
from ninja.errors import HttpError

from ddpui.auth import has_permission
from ddpui.core.widget.exceptions import (
    WidgetNotFoundError,
    WidgetPermissionError,
    WidgetValidationError,
)
from ddpui.core.widget.widget_service import WidgetService
from ddpui.models.org_user import OrgUser
from ddpui.schemas.widget_schema import (
    WidgetCreate,
    WidgetListResponse,
    WidgetResponse,
    WidgetUpdate,
)
from ddpui.utils.custom_logger import CustomLogger
from ddpui.utils.response_wrapper import ApiResponse, api_response

logger = CustomLogger("ddpui.widget_api")
widget_router = Router()


@widget_router.get("/", response=ApiResponse[WidgetListResponse])
@has_permission(["can_view_widgets"])
def list_widgets(
    request,
    page: int = 1,
    page_size: int = 10,
    search: Optional[str] = None,
):
    orguser: OrgUser = request.orguser
    widgets, total = WidgetService.list_widgets(orguser.org, page, page_size, search)
    return api_response(
        success=True,
        data=WidgetListResponse(
            data=[WidgetResponse.from_model(w) for w in widgets],
            total=total,
            page=page,
            page_size=page_size,
        ),
    )


@widget_router.get("/{widget_id}/", response=ApiResponse[WidgetResponse])
@has_permission(["can_view_widgets"])
def get_widget(request, widget_id: int):
    orguser: OrgUser = request.orguser
    try:
        widget = WidgetService.get_widget(widget_id, orguser.org)
        return api_response(success=True, data=WidgetResponse.from_model(widget))
    except WidgetNotFoundError as err:
        raise HttpError(404, str(err)) from err


@widget_router.post("/", response=ApiResponse[WidgetResponse])
@has_permission(["can_create_widgets"])
def create_widget(request, payload: WidgetCreate):
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


@widget_router.put("/{widget_id}/", response=ApiResponse[WidgetResponse])
@has_permission(["can_edit_widgets"])
def update_widget(request, widget_id: int, payload: WidgetUpdate):
    orguser: OrgUser = request.orguser
    try:
        widget = WidgetService.update_widget(widget_id, orguser.org, orguser, payload)
        return api_response(success=True, data=WidgetResponse.from_model(widget))
    except WidgetNotFoundError as err:
        raise HttpError(404, str(err)) from err
    except WidgetValidationError as err:
        raise HttpError(400, str(err)) from err


@widget_router.delete("/{widget_id}/", response=ApiResponse)
@has_permission(["can_delete_widgets"])
def delete_widget(request, widget_id: int):
    orguser: OrgUser = request.orguser
    try:
        WidgetService.delete_widget(widget_id, orguser.org, orguser)
        return api_response(success=True, message="Widget deleted successfully")
    except WidgetNotFoundError as err:
        raise HttpError(404, str(err)) from err
    except WidgetPermissionError as err:
        raise HttpError(403, str(err)) from err
```

## Register the router

In the main NinjaAPI setup:

```python
src_api.add_router("/api/widgets/", widget_router)
```

## Response wrapper shape

`api_response()` returns this envelope:

**Success with data:**
```json
{
  "success": true,
  "message": "Widget created successfully",
  "data": { "id": 1, "title": "...", "...": "..." }
}
```

**Error from `HttpError`:**
```json
{
  "success": false,
  "message": "Widget with id 99 not found",
  "error_code": "WIDGET_NOT_FOUND"
}
```

**Paginated list:**
```json
{
  "success": true,
  "data": {
    "data": [ { "id": 1, "...": "..." } ],
    "total": 42,
    "page": 1,
    "page_size": 10
  }
}
```

## Validating payloads before external service calls

Whenever a service calls an external system (dbt, Airbyte, Prefect), validate via a schema first. Don't pass raw `dict` through.

```python
from ddpui.schemas.dbt_schema import DbtOperationPayload


class DbtService:
    @staticmethod
    def run_operation(payload: dict, org: Org) -> dict:
        validated = DbtOperationPayload(**payload)  # raises on invalid input
        return dbt_client.run(validated.dict())
```

For external-service-bound schemas, use strict validation so unknown fields are caught:

```python
class DbtOperationPayload(Schema):
    operation: str
    config: dict

    class Config:
        extra = "forbid"
```

## Schema cheatsheet

| Field declaration                              | Meaning                          |
|------------------------------------------------|----------------------------------|
| `Field(..., min_length=1, max_length=255)`     | required string with bounds      |
| `Field(1, ge=1, le=100)`                       | int with bounds, default 1       |
| `Optional[str] = Field(None)`                  | nullable, default None           |
| `Field(default_factory=dict)`                  | mutable default                  |
| `class Config: extra = "forbid"`               | reject extra keys (use for external payloads) |
| `class Config: json_schema_extra = {...}`      | example for OpenAPI docs         |

## Common mistakes

| Bad                                                       | Good                                                       |
|-----------------------------------------------------------|------------------------------------------------------------|
| `router = Router()`                                       | `widget_router = Router()`                                 |
| `return {"success": True, "data": ...}`                   | `return api_response(success=True, data=...)`              |
| Skipping `@has_permission` for "internal" endpoints       | Every endpoint has one; use `can_view_*` for read          |
| Catching `Exception` and always returning 500             | Map each error class to its HTTP code                      |
| `Widget.objects.get(...)` inside the API function         | `WidgetService.get_widget(...)`                            |
| `from django.conf import settings` inside a function      | Import at top of file                                      |
| `widget.to_dict()` / `widget.to_json()`                   | `WidgetResponse.from_model(widget)`                        |
| `Widget.objects.get(id=widget_id)` (no org filter)        | `Widget.objects.get(id=widget_id, org=org)`                |

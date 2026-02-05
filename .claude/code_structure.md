# API Design Best Practices for Dalgo Platform

## Table of Contents

1. [Overview](#overview)
2. [Layer Architecture](#layer-architecture)
3. [API Layer Design](#api-layer-design)
4. [Core Layer Design](#core-layer-design)
5. [Schema Design](#schema-design)
6. [Model Design](#model-design)
7. [Exception Handling](#exception-handling)
8. [API Response Wrapper](#api-response-wrapper)
9. [Request-Response Flow](#request-response-flow)
10. [Migration Guide](#migration-guide)
11. [Code Examples](#code-examples)
12. [Checklist](#checklist)

---

## Overview

This document establishes best practices for API design in the Dalgo platform, focusing on proper separation of concerns across API, Core, Schema, and Model layers. These practices ensure maintainability, testability, and consistency across the codebase.

### Core Principles

1. **Separation of Concerns**: Each layer has a single, well-defined responsibility
2. **Dependency Direction**: API → Core → Models (one-way dependency)
3. **Testability**: Business logic is isolated and easily testable
4. **Consistency**: All modules follow the same patterns
5. **Maintainability**: Clear structure makes code easier to understand and modify
6. **Schema Validation**: Validate data using schemas before any database/external operations

### Key Architectural Decisions

1. **API and Core use schemas** to validate data before hitting DBT or any external service
2. **API Response Wrapper** provides consistent response format across all endpoints
3. **All features consolidated in Core** - business logic, services, and domain operations
4. **Services folder removed** - merged into core with feature-based organization
5. **Schemas and Models in feature folders** - co-located with related core logic
6. **Exceptions in Core** - each feature has its own exceptions file

---

## Layer Architecture

### Standard Module Structure

```
ddpui/
├── api/
│   └── {module}_api.py              # HTTP request/response handling
│
├── core/
│   └── {module}/                    # Feature module (all business logic here)
│       ├── __init__.py
│       ├── {module}_service.py      # Business logic and orchestration
│       ├── {module}_operations.py   # Domain operations (optional)
│       ├── schemas.py               # Request/response validation (Pydantic)
│       ├── exceptions.py            # Custom exceptions for this feature
│       └── models.py                # Database models (Django ORM) - optional, can stay in models/
│
├── models/
│   └── {module}.py                  # Shared/legacy models (migrate to core/{module}/)
│
└── utils/
    └── response_wrapper.py          # API response wrapper utility
```

### Layer Responsibilities Matrix

| Layer | Responsibility | Should NOT |
|-------|---------------|------------|
| **API** | HTTP handling, schema validation, permissions, error conversion, response wrapping | Business logic, database queries, external calls |
| **Core** | Business logic, domain operations, service orchestration, schema validation before external calls | HTTP concerns, direct API responses |
| **Schema** | Request/response validation, API contracts, data transformation | Business logic, database operations |
| **Model** | Database schema, relationships, basic methods | Business logic, API concerns |
| **Exceptions** | Feature-specific error definitions | Business logic, HTTP status codes |

---

## API Layer Design

### Location
`ddpui/api/{module}_api.py`

### Responsibilities

✅ **DO**:
- Handle HTTP requests and responses
- Validate request data using schemas from `core/{module}/schemas.py`
- Check permissions (`@has_permission`)
- Convert core exceptions to HTTP errors
- Use response wrapper for consistent API responses
- Handle HTTP-specific concerns (status codes, headers)

❌ **DON'T**:
- Contain business logic
- Direct database queries (except simple lookups)
- Complex data transformations
- External service calls
- Transaction management

### Structure Template

```python
"""{Module} API endpoints"""

from typing import Optional, List
from ninja import Router
from ninja.errors import HttpError

from ddpui.auth import has_permission
from ddpui.models.org_user import OrgUser
from ddpui.utils.custom_logger import CustomLogger
from ddpui.utils.response_wrapper import api_response, ApiResponse

# Import from core module
from ddpui.core.{module}.{module}_service import {Module}Service
from ddpui.core.{module}.schemas import (
    {Module}Create,
    {Module}Update,
    {Module}Response,
    {Module}ListResponse,
)
from ddpui.core.{module}.exceptions import (
    {Module}NotFoundError,
    {Module}ValidationError,
    {Module}PermissionError,
)

logger = CustomLogger("ddpui.{module}_api")

{module}_router = Router()


@{module}_router.get("/", response=ApiResponse[{Module}ListResponse])
@has_permission(["can_view_{module}s"])
def list_{module}s(request, page: int = 1, page_size: int = 10):
    """List {module}s with pagination"""
    orguser: OrgUser = request.orguser
    
    {module}s, total = {Module}Service.list_{module}s(
        org=orguser.org,
        page=page,
        page_size=page_size,
    )
    
    return api_response(
        success=True,
        data={Module}ListResponse(
            data=[{Module}Response.from_model(m) for m in {module}s],
            total=total,
            page=page,
            page_size=page_size,
        )
    )


@{module}_router.get("/{{id}}/", response=ApiResponse[{Module}Response])
@has_permission(["can_view_{module}s"])
def get_{module}(request, id: int):
    """Get a specific {module}"""
    orguser: OrgUser = request.orguser
    
    try:
        {module} = {Module}Service.get_{module}(id, orguser.org)
        return api_response(
            success=True,
            data={Module}Response.from_model({module})
        )
    except {Module}NotFoundError as err:
        raise HttpError(404, str(err)) from err


@{module}_router.post("/", response=ApiResponse[{Module}Response])
@has_permission(["can_create_{module}s"])
def create_{module}(request, payload: {Module}Create):
    """Create a new {module}"""
    orguser: OrgUser = request.orguser
    
    try:
        # Schema validation happens automatically via Pydantic
        {module} = {Module}Service.create_{module}(payload, orguser)
        return api_response(
            success=True,
            data={Module}Response.from_model({module}),
            message="{Module} created successfully"
        )
    except {Module}ValidationError as err:
        raise HttpError(400, str(err)) from err
    except Exception as e:
        logger.error(f"Error creating {module}: {e}")
        raise HttpError(500, "Failed to create {module}") from e


@{module}_router.put("/{{id}}/", response=ApiResponse[{Module}Response])
@has_permission(["can_edit_{module}s"])
def update_{module}(request, id: int, payload: {Module}Update):
    """Update a {module}"""
    orguser: OrgUser = request.orguser
    
    try:
        {module} = {Module}Service.update_{module}(
            {module}_id=id,
            org=orguser.org,
            orguser=orguser,
            data=payload,
        )
        return api_response(
            success=True,
            data={Module}Response.from_model({module}),
            message="{Module} updated successfully"
        )
    except {Module}NotFoundError as err:
        raise HttpError(404, str(err)) from err
    except {Module}ValidationError as err:
        raise HttpError(400, str(err)) from err


@{module}_router.delete("/{{id}}/", response=ApiResponse)
@has_permission(["can_delete_{module}s"])
def delete_{module}(request, id: int):
    """Delete a {module}"""
    orguser: OrgUser = request.orguser
    
    try:
        {Module}Service.delete_{module}(id, orguser.org, orguser)
        return api_response(success=True, message="{Module} deleted successfully")
    except {Module}NotFoundError as err:
        raise HttpError(404, str(err)) from err
    except {Module}PermissionError as err:
        raise HttpError(403, str(err)) from err
```

### Best Practices

1. **Router Naming**: Always use `{module}_router` pattern
   ```python
   # ✅ GOOD
   charts_router = Router()
   dashboard_router = Router()
   
   # ❌ BAD
   router = Router()  # Too generic
   ```

2. **Permission Decorators**: Always use `@has_permission` on all endpoints
   ```python
   # ✅ GOOD
   @charts_router.get("/")
   @has_permission(["can_view_charts"])
   def list_charts(request):
       pass
   ```

3. **Use Response Wrapper**: Always wrap responses
   ```python
   # ✅ GOOD
   return api_response(success=True, data=result)
   
   # ❌ BAD
   return {"success": True, "data": result}  # Manual dict
   ```

4. **Schema Validation**: Let Pydantic handle validation
   ```python
   # ✅ GOOD - Pydantic validates automatically
   def create_chart(request, payload: ChartCreate):
       chart = ChartService.create_chart(payload, orguser)
   
   # ❌ BAD - Manual validation in API
   def create_chart(request, payload: dict):
       if not payload.get("title"):
           raise HttpError(400, "Title required")
   ```

5. **Keep API Layer Thin**: Delegate everything to core
   ```python
   # ✅ GOOD
   chart = ChartService.get_chart(chart_id, orguser.org)
   
   # ❌ BAD
   chart = Chart.objects.get(id=chart_id, org=orguser.org)  # Direct model access
   ```

---

## Core Layer Design

### Location
`ddpui/core/{module}/`

### Structure

```
ddpui/core/{module}/
├── __init__.py              # Export public interfaces
├── {module}_service.py      # Business logic and orchestration
├── {module}_operations.py   # Domain-specific operations (optional)
├── schemas.py               # Pydantic schemas for this feature
├── exceptions.py            # Custom exceptions
└── models.py                # Django models (optional - can stay in models/)
```

### Responsibilities

✅ **DO**:
- Implement business logic and rules
- Validate data using schemas before external operations (DBT, Airbyte, etc.)
- Manage transactions
- Orchestrate multiple operations
- Handle cross-cutting concerns (caching, logging)
- Coordinate between models and external services
- Domain operations and algorithms

❌ **DON'T**:
- Handle HTTP concerns
- Make direct API responses
- Access request objects

### Service Template (`{module}_service.py`)

```python
"""{Module} service for business logic

This module encapsulates all {module}-related business logic,
separating it from the API layer for better testability and maintainability.
"""

from typing import Optional, List, Tuple
from dataclasses import dataclass

from django.db.models import Q
from django.db import transaction

from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser
from ddpui.utils.custom_logger import CustomLogger

# Import from same core module
from .schemas import {Module}Create, {Module}Update
from .exceptions import (
    {Module}NotFoundError,
    {Module}ValidationError,
    {Module}PermissionError,
)

# Import model (can be from models/ or from .models)
from ddpui.models.{module} import {Module}

logger = CustomLogger("ddpui.core.{module}")


class {Module}Service:
    """Service class for {module}-related operations"""
    
    @staticmethod
    def get_{module}({module}_id: int, org: Org) -> {Module}:
        """Get a {module} by ID for an organization.
        
        Args:
            {module}_id: The {module} ID
            org: The organization
            
        Returns:
            {Module} instance
            
        Raises:
            {Module}NotFoundError: If {module} doesn't exist or doesn't belong to org
        """
        try:
            return {Module}.objects.get(id={module}_id, org=org)
        except {Module}.DoesNotExist:
            raise {Module}NotFoundError({module}_id)
    
    @staticmethod
    def list_{module}s(
        org: Org,
        page: int = 1,
        page_size: int = 10,
        search: Optional[str] = None,
    ) -> Tuple[List[{Module}], int]:
        """List {module}s for an organization with pagination and filtering.
        
        Args:
            org: The organization
            page: Page number (1-indexed)
            page_size: Number of items per page
            search: Optional search term
            
        Returns:
            Tuple of ({module}s list, total count)
        """
        query = Q(org=org)
        
        if search:
            query &= Q(title__icontains=search) | Q(description__icontains=search)
        
        queryset = {Module}.objects.filter(query).order_by("-updated_at")
        total = queryset.count()
        
        offset = (page - 1) * page_size
        {module}s = list(queryset[offset : offset + page_size])
        
        return {module}s, total
    
    @staticmethod
    @transaction.atomic
    def create_{module}(data: {Module}Create, orguser: OrgUser) -> {Module}:
        """Create a new {module}.
        
        Args:
            data: {Module} creation data (already validated by Pydantic)
            orguser: The user creating the {module}
            
        Returns:
            Created {Module} instance
            
        Raises:
            {Module}ValidationError: If {module} configuration is invalid
        """
        # Additional business logic validation
        {Module}Service._validate_business_rules(data, orguser.org)
        
        # Create {module}
        {module} = {Module}.objects.create(
            title=data.title,
            description=data.description,
            created_by=orguser,
            last_modified_by=orguser,
            org=orguser.org,
        )
        
        logger.info(f"Created {module} {{{module}.id}} for org {{{orguser.org.id}}}")
        return {module}
    
    @staticmethod
    @transaction.atomic
    def update_{module}(
        {module}_id: int,
        org: Org,
        orguser: OrgUser,
        data: {Module}Update,
    ) -> {Module}:
        """Update an existing {module}.
        
        Args:
            {module}_id: The {module} ID
            org: The organization
            orguser: The user making the update
            data: Update data (already validated by Pydantic)
            
        Returns:
            Updated {Module} instance
            
        Raises:
            {Module}NotFoundError: If {module} doesn't exist
            {Module}ValidationError: If updated configuration is invalid
        """
        {module} = {Module}Service.get_{module}({module}_id, org)
        
        # Apply updates only for provided fields
        if data.title is not None:
            {module}.title = data.title
        if data.description is not None:
            {module}.description = data.description
        
        {module}.last_modified_by = orguser
        {module}.save()
        
        logger.info(f"Updated {module} {{{module}.id}}")
        return {module}
    
    @staticmethod
    @transaction.atomic
    def delete_{module}({module}_id: int, org: Org, orguser: OrgUser) -> bool:
        """Delete a {module}.
        
        Args:
            {module}_id: The {module} ID
            org: The organization
            orguser: The user deleting the {module}
            
        Returns:
            True if deletion was successful
            
        Raises:
            {Module}NotFoundError: If {module} doesn't exist
            {Module}PermissionError: If user doesn't have permission
        """
        {module} = {Module}Service.get_{module}({module}_id, org)
        
        # Permission check (example: only creator can delete)
        if {module}.created_by != orguser:
            raise {Module}PermissionError("You can only delete {module}s you created.")
        
        {module}_title = {module}.title
        {module}.delete()
        
        logger.info(f"Deleted {module} '{{{module}_title}}' (id={{{module}_id}}) by {{{orguser.user.email}}}")
        return True
    
    @staticmethod
    def _validate_business_rules(data: {Module}Create, org: Org) -> None:
        """Validate business rules before creating {module}.
        
        This is where you add domain-specific validation that goes beyond
        schema validation.
        
        Raises:
            {Module}ValidationError: If validation fails
        """
        # Example: Check for duplicate titles
        if {Module}.objects.filter(org=org, title=data.title).exists():
            raise {Module}ValidationError(f"{Module} with title '{data.title}' already exists")
```

### Schema Validation Before External Operations

When calling DBT, Airbyte, or any external service, **always validate using schemas first**:

```python
# ✅ GOOD: Validate before calling DBT
from .schemas import DBTOperationPayload

class DBTService:
    @staticmethod
    def run_operation(payload: dict, org: Org) -> dict:
        # Validate using schema before hitting DBT
        validated_payload = DBTOperationPayload(**payload)
        
        # Now safe to call DBT
        result = dbt_client.run(validated_payload.dict())
        return result

# ❌ BAD: Calling DBT without validation
class DBTService:
    @staticmethod
    def run_operation(payload: dict, org: Org) -> dict:
        # Directly calling DBT without validation
        result = dbt_client.run(payload)  # Dangerous!
        return result
```

---

## Schema Design

### Location
`ddpui/core/{module}/schemas.py`

### Responsibilities

✅ **DO**:
- Define request/response contracts
- Validate input data
- Transform data for API responses
- Document API structure
- Provide type safety
- Include `from_model()` class methods for easy conversion

❌ **DON'T**:
- Contain business logic
- Perform database operations
- Make external service calls

### Structure Template

```python
"""{Module} schemas for request/response validation"""

from typing import Optional, List, Dict, Any
from datetime import datetime
from ninja import Schema, Field


# =============================================================================
# Request Schemas
# =============================================================================

class {Module}Create(Schema):
    """Schema for creating a {module}"""
    title: str = Field(..., min_length=1, max_length=255, description="Title of the {module}")
    description: Optional[str] = Field(None, max_length=1000, description="Description")
    
    class Config:
        json_schema_extra = {
            "example": {
                "title": "Example {Module}",
                "description": "Example description",
            }
        }


class {Module}Update(Schema):
    """Schema for updating a {module} (all fields optional)"""
    title: Optional[str] = Field(None, min_length=1, max_length=255)
    description: Optional[str] = Field(None, max_length=1000)


class {Module}ListQuery(Schema):
    """Schema for list query parameters"""
    page: int = Field(1, ge=1, description="Page number (1-indexed)")
    page_size: int = Field(10, ge=1, le=100, description="Items per page")
    search: Optional[str] = Field(None, description="Search term")


# =============================================================================
# Response Schemas
# =============================================================================

class {Module}Response(Schema):
    """Schema for {module} response"""
    id: int
    title: str
    description: Optional[str]
    created_at: datetime
    updated_at: datetime
    
    @classmethod
    def from_model(cls, model) -> "{Module}Response":
        """Create response from Django model instance"""
        return cls(
            id=model.id,
            title=model.title,
            description=model.description,
            created_at=model.created_at,
            updated_at=model.updated_at,
        )
    
    class Config:
        json_schema_extra = {
            "example": {
                "id": 1,
                "title": "Example {Module}",
                "description": "Example description",
                "created_at": "2025-01-22T10:00:00Z",
                "updated_at": "2025-01-22T10:00:00Z",
            }
        }


class {Module}ListResponse(Schema):
    """Schema for paginated {module} list response"""
    data: List[{Module}Response]
    total: int
    page: int
    page_size: int
    
    @property
    def total_pages(self) -> int:
        return (self.total + self.page_size - 1) // self.page_size


# =============================================================================
# Internal/Operation Schemas (for validating before external calls)
# =============================================================================

class {Module}DBTPayload(Schema):
    """Schema for validating payload before sending to DBT"""
    operation: str
    config: Dict[str, Any]
    
    class Config:
        extra = "forbid"  # Strict validation - no extra fields allowed
```

### Best Practices

1. **Separate Request/Response Schemas**: Don't reuse models directly
   ```python
   # ✅ GOOD
   class ChartCreate(Schema):
       title: str
   
   class ChartResponse(Schema):
       id: int
       title: str
       
       @classmethod
       def from_model(cls, chart):
           return cls(id=chart.id, title=chart.title)
   ```

2. **Use Field Validation**: Leverage Pydantic's validation
   ```python
   # ✅ GOOD
   title: str = Field(..., min_length=1, max_length=255)
   page: int = Field(1, ge=1, le=100)
   ```

3. **Add `from_model()` Method**: For easy model-to-schema conversion
   ```python
   # ✅ GOOD
   @classmethod
   def from_model(cls, model):
       return cls(id=model.id, title=model.title)
   ```

4. **Use Strict Validation for External Payloads**:
   ```python
   # ✅ GOOD - Strict for external service payloads
   class DBTPayload(Schema):
       class Config:
           extra = "forbid"  # Reject unknown fields
   ```

---

## Model Design

### Location
- **Preferred**: `ddpui/core/{module}/models.py` (co-located with feature)
- **Legacy/Shared**: `ddpui/models/{module}.py`

### Responsibilities

✅ **DO**:
- Define database schema (Django ORM)
- Define model relationships
- Provide basic model methods (`__str__()`)
- Model-level validations (using `clean()` method)

❌ **DON'T**:
- Contain business logic
- Make API calls
- Perform complex operations
- Access external services
- Include `to_json()` or `to_dict()` methods (use schemas instead)

### Structure Template

```python
"""{Module} model for Dalgo platform"""

from django.db import models
from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser


class {Module}(models.Model):
    """{Module} configuration model"""
    
    id = models.BigAutoField(primary_key=True)
    title = models.CharField(max_length=255)
    description = models.TextField(blank=True, null=True)
    
    # Relationships
    org = models.ForeignKey(Org, on_delete=models.CASCADE)
    created_by = models.ForeignKey(
        OrgUser,
        on_delete=models.CASCADE,
        db_column="created_by",
        related_name="{module}s_created"
    )
    last_modified_by = models.ForeignKey(
        OrgUser,
        on_delete=models.CASCADE,
        db_column="last_modified_by",
        null=True,
        related_name="{module}s_modified"
    )
    
    # Timestamps
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        db_table = "{module}"
        ordering = ["-updated_at"]
        indexes = [
            models.Index(fields=["org", "created_at"]),
        ]
    
    def __str__(self):
        return f"{self.title} ({self.id})"
    
    def clean(self):
        """Model-level validation"""
        from django.core.exceptions import ValidationError
        
        if not self.title or not self.title.strip():
            raise ValidationError({"title": "Title cannot be empty"})
```

### Key Point: No `to_json()` or `to_dict()`

Use schemas for serialization instead:

```python
# ❌ BAD - to_json in model
class Chart(models.Model):
    def to_json(self):
        return {"id": self.id, "title": self.title}

# ✅ GOOD - Use schema
from ddpui.core.charts.schemas import ChartResponse

chart = Chart.objects.get(id=1)
response = ChartResponse.from_model(chart)
```

---

## Exception Handling

### Location
`ddpui/core/{module}/exceptions.py`

### Structure Template

```python
"""{Module} exceptions

Custom exceptions for {module} feature.
"""


class {Module}Error(Exception):
    """Base exception for {module} errors"""
    
    def __init__(self, message: str, error_code: str = "{MODULE}_ERROR"):
        self.message = message
        self.error_code = error_code
        super().__init__(self.message)


class {Module}NotFoundError({Module}Error):
    """Raised when {module} is not found"""
    
    def __init__(self, {module}_id: int):
        super().__init__(
            f"{Module} with id {{{module}_id}} not found",
            "{MODULE}_NOT_FOUND"
        )
        self.{module}_id = {module}_id


class {Module}ValidationError({Module}Error):
    """Raised when {module} validation fails"""
    
    def __init__(self, message: str):
        super().__init__(message, "{MODULE}_VALIDATION_ERROR")


class {Module}PermissionError({Module}Error):
    """Raised when user doesn't have permission"""
    
    def __init__(self, message: str = "Permission denied"):
        super().__init__(message, "{MODULE}_PERMISSION_DENIED")


class {Module}ExternalServiceError({Module}Error):
    """Raised when external service (DBT, Airbyte) call fails"""
    
    def __init__(self, service: str, message: str):
        super().__init__(
            f"{service} error: {message}",
            "{MODULE}_EXTERNAL_ERROR"
        )
        self.service = service
```

### Exception Hierarchy and HTTP Mapping

```
{Module}Error (base)
  ├── {Module}NotFoundError      → 404 Not Found
  ├── {Module}ValidationError    → 400 Bad Request
  ├── {Module}PermissionError    → 403 Forbidden
  ├── {Module}ExternalServiceError → 502 Bad Gateway
  └── {Module}Error (generic)    → 500 Internal Server Error
```

### Usage in API Layer

```python
from ddpui.core.{module}.exceptions import (
    {Module}NotFoundError,
    {Module}ValidationError,
    {Module}PermissionError,
    {Module}ExternalServiceError,
)

@{module}_router.get("/{{id}}/")
@has_permission(["can_view_{module}s"])
def get_{module}(request, id: int):
    try:
        {module} = {Module}Service.get_{module}(id, request.orguser.org)
        return api_response(success=True, data={Module}Response.from_model({module}))
    except {Module}NotFoundError as err:
        raise HttpError(404, str(err)) from err
    except {Module}ValidationError as err:
        raise HttpError(400, str(err)) from err
    except {Module}PermissionError as err:
        raise HttpError(403, str(err)) from err
    except {Module}ExternalServiceError as err:
        raise HttpError(502, str(err)) from err
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise HttpError(500, "Internal server error") from e
```

---

## API Response Wrapper

### Location
`ddpui/utils/response_wrapper.py`

### Implementation

```python
"""API Response Wrapper

Provides consistent response format across all API endpoints.
"""

from typing import TypeVar, Generic, Optional, Any
from ninja import Schema


T = TypeVar("T")


class ApiResponse(Schema, Generic[T]):
    """Standard API response wrapper"""
    success: bool
    message: Optional[str] = None
    data: Optional[T] = None
    error_code: Optional[str] = None


def api_response(
    success: bool,
    data: Any = None,
    message: Optional[str] = None,
    error_code: Optional[str] = None,
) -> dict:
    """Create a standardized API response.
    
    Args:
        success: Whether the operation was successful
        data: Response data (can be schema instance or dict)
        message: Optional message
        error_code: Optional error code for failures
        
    Returns:
        Dict with standard response structure
    """
    response = {"success": success}
    
    if message is not None:
        response["message"] = message
    
    if data is not None:
        if hasattr(data, "dict"):
            response["data"] = data.dict()
        else:
            response["data"] = data
    
    if error_code is not None:
        response["error_code"] = error_code
    
    return response
```

### Response Format

**Success Response:**
```json
{
    "success": true,
    "message": "Chart created successfully",
    "data": {
        "id": 1,
        "title": "Sales Overview",
        "created_at": "2025-01-22T10:00:00Z"
    }
}
```

**Error Response:**
```json
{
    "success": false,
    "message": "Chart not found",
    "error_code": "CHART_NOT_FOUND"
}
```

**List Response:**
```json
{
    "success": true,
    "data": {
        "data": [...],
        "total": 100,
        "page": 1,
        "page_size": 10
    }
}
```

### Usage Examples

```python
# Simple success
return api_response(success=True, message="Operation completed")

# Success with data
return api_response(
    success=True,
    data=ChartResponse.from_model(chart),
    message="Chart created"
)

# List response
return api_response(
    success=True,
    data=ChartListResponse(
        data=[ChartResponse.from_model(c) for c in charts],
        total=total,
        page=page,
        page_size=page_size,
    )
)
```

---

## Request-Response Flow

### Standard Flow

```
1. Client Request
   ↓
2. API Layer (api/{module}_api.py)
   ├── Validate request (Pydantic schema - automatic)
   ├── Check permissions (@has_permission)
   ├── Extract orguser from request
   └── Call core service method
   ↓
3. Core Layer (core/{module}/{module}_service.py)
   ├── Business logic validation
   ├── Validate payloads before external calls (using schemas)
   ├── Transaction management
   ├── Call operations (if needed)
   ├── Database operations (via models)
   └── Return model instance
   ↓
4. Core Operations (core/{module}/{module}_operations.py) - Optional
   ├── Domain operations
   ├── External service calls (DBT, Airbyte)
   └── Data transformations
   ↓
5. Model Layer (models/ or core/{module}/models.py)
   └── Database operations
   ↓
6. Core Layer (return)
   └── Return model instance
   ↓
7. API Layer (serialize)
   ├── Convert model to response schema
   ├── Wrap with api_response()
   └── Return HTTP response
   ↓
8. Client Response
```

### Example Flow: Creating a Chart

```python
# 1. Client sends POST /api/charts/
{
    "title": "Sales Overview",
    "chart_type": "bar",
    "schema_name": "public",
    "table_name": "sales"
}

# 2. API Layer (api/charts_api.py)
@charts_router.post("/", response=ApiResponse[ChartResponse])
@has_permission(["can_create_charts"])
def create_chart(request, payload: ChartCreate):
    orguser = request.orguser
    # Schema validation happens automatically
    
    chart = ChartService.create_chart(payload, orguser)
    return api_response(
        success=True,
        data=ChartResponse.from_model(chart),
        message="Chart created successfully"
    )

# 3. Core Service Layer (core/charts/chart_service.py)
class ChartService:
    @staticmethod
    @transaction.atomic
    def create_chart(data: ChartCreate, orguser: OrgUser) -> Chart:
        # Business validation
        ChartService._validate_business_rules(data, orguser.org)
        
        # Validate before calling external service (if needed)
        dbt_payload = ChartDBTPayload(
            operation="validate_table",
            schema=data.schema_name,
            table=data.table_name,
        )
        # Schema ensures payload is valid before DBT call
        
        chart = Chart.objects.create(
            title=data.title,
            chart_type=data.chart_type,
            schema_name=data.schema_name,
            table_name=data.table_name,
            created_by=orguser,
            org=orguser.org,
        )
        
        return chart

# 4. Response Schema (core/charts/schemas.py)
class ChartResponse(Schema):
    id: int
    title: str
    chart_type: str
    
    @classmethod
    def from_model(cls, chart):
        return cls(
            id=chart.id,
            title=chart.title,
            chart_type=chart.chart_type,
        )

# 5. Final Response
{
    "success": true,
    "message": "Chart created successfully",
    "data": {
        "id": 1,
        "title": "Sales Overview",
        "chart_type": "bar"
    }
}
```

---

## Migration Guide

### Migrating from `services/` to `core/`

1. **Create feature folder in core:**
   ```bash
   mkdir -p ddpui/core/{module}
   touch ddpui/core/{module}/__init__.py
   ```

2. **Move service file:**
   ```bash
   mv ddpui/services/{module}_service.py ddpui/core/{module}/{module}_service.py
   ```

3. **Create exceptions file:**
   ```bash
   # Extract exceptions from service file to:
   ddpui/core/{module}/exceptions.py
   ```

4. **Move schemas:**
   ```bash
   mv ddpui/schemas/{module}_schema.py ddpui/core/{module}/schemas.py
   ```

5. **Update imports in API:**
   ```python
   # Before
   from ddpui.services.{module}_service import {Module}Service
   from ddpui.schemas.{module}_schema import {Module}Create
   
   # After
   from ddpui.core.{module}.{module}_service import {Module}Service
   from ddpui.core.{module}.schemas import {Module}Create
   from ddpui.core.{module}.exceptions import {Module}NotFoundError
   ```

6. **Update `core/{module}/__init__.py`:**
   ```python
   from .{module}_service import {Module}Service
   from .schemas import {Module}Create, {Module}Response
   from .exceptions import {Module}Error, {Module}NotFoundError
   
   __all__ = [
       "{Module}Service",
       "{Module}Create",
       "{Module}Response",
       "{Module}Error",
       "{Module}NotFoundError",
   ]
   ```

### Adding Response Wrapper to Existing API

1. **Import wrapper:**
   ```python
   from ddpui.utils.response_wrapper import api_response, ApiResponse
   ```

2. **Update endpoint signature:**
   ```python
   # Before
   @router.get("/", response=List[ChartResponse])
   
   # After
   @router.get("/", response=ApiResponse[ChartListResponse])
   ```

3. **Wrap return value:**
   ```python
   # Before
   return [ChartResponse(**c.to_dict()) for c in charts]
   
   # After
   return api_response(
       success=True,
       data=ChartListResponse(
           data=[ChartResponse.from_model(c) for c in charts],
           total=total,
           page=page,
           page_size=page_size,
       )
   )
   ```

---

## Code Examples

### Complete Example: Charts Module Structure

```
ddpui/core/charts/
├── __init__.py
├── chart_service.py
├── chart_operations.py      # Query building, data transformation
├── chart_validator.py       # Validation logic
├── schemas.py               # All chart schemas
├── exceptions.py            # Chart-specific exceptions
└── echarts_config_generator.py  # ECharts configuration
```

#### `__init__.py`
```python
from .chart_service import ChartService
from .schemas import ChartCreate, ChartUpdate, ChartResponse, ChartListResponse
from .exceptions import ChartError, ChartNotFoundError, ChartValidationError

__all__ = [
    "ChartService",
    "ChartCreate",
    "ChartUpdate", 
    "ChartResponse",
    "ChartListResponse",
    "ChartError",
    "ChartNotFoundError",
    "ChartValidationError",
]
```

#### `exceptions.py`
```python
"""Chart exceptions"""


class ChartError(Exception):
    """Base exception for chart errors"""
    
    def __init__(self, message: str, error_code: str = "CHART_ERROR"):
        self.message = message
        self.error_code = error_code
        super().__init__(self.message)


class ChartNotFoundError(ChartError):
    """Raised when chart is not found"""
    
    def __init__(self, chart_id: int):
        super().__init__(f"Chart with id {chart_id} not found", "CHART_NOT_FOUND")
        self.chart_id = chart_id


class ChartValidationError(ChartError):
    """Raised when chart validation fails"""
    
    def __init__(self, message: str):
        super().__init__(message, "CHART_VALIDATION_ERROR")


class ChartPermissionError(ChartError):
    """Raised when user doesn't have permission"""
    
    def __init__(self, message: str = "Permission denied"):
        super().__init__(message, "CHART_PERMISSION_DENIED")
```

#### `schemas.py`
```python
"""Chart schemas"""

from typing import Optional, List, Dict, Any
from datetime import datetime
from ninja import Schema, Field


class ChartCreate(Schema):
    """Schema for creating a chart"""
    title: str = Field(..., min_length=1, max_length=255)
    chart_type: str = Field(..., description="Type of chart (bar, line, pie, etc.)")
    schema_name: str = Field(..., description="Database schema name")
    table_name: str = Field(..., description="Database table name")
    description: Optional[str] = None
    extra_config: Optional[Dict[str, Any]] = Field(default_factory=dict)


class ChartUpdate(Schema):
    """Schema for updating a chart"""
    title: Optional[str] = Field(None, min_length=1, max_length=255)
    description: Optional[str] = None
    extra_config: Optional[Dict[str, Any]] = None


class ChartResponse(Schema):
    """Schema for chart response"""
    id: int
    title: str
    chart_type: str
    description: Optional[str]
    created_at: datetime
    updated_at: datetime
    
    @classmethod
    def from_model(cls, chart) -> "ChartResponse":
        return cls(
            id=chart.id,
            title=chart.title,
            chart_type=chart.chart_type,
            description=chart.description,
            created_at=chart.created_at,
            updated_at=chart.updated_at,
        )


class ChartListResponse(Schema):
    """Schema for paginated chart list"""
    data: List[ChartResponse]
    total: int
    page: int
    page_size: int
```

---

### Key Changes from Previous Architecture

| Before | After |
|--------|-------|
| `services/{module}_service.py` | `core/{module}/{module}_service.py` |
| `schemas/{module}_schema.py` | `core/{module}/schemas.py` |
| Exceptions in service file | `core/{module}/exceptions.py` |
| `model.to_json()` | `Schema.from_model(model)` |
| Direct response dict | `api_response()` wrapper |
| No validation before DBT | Schema validation required |



---

**Document Version**: 2.0  
**Last Updated**: 2025-01-23  
**Maintained By**: Dalgo Platform Team

# API Design Best Practices for Dalgo Platform

## Table of Contents

1. [Overview](#overview)
2. [Layer Architecture](#layer-architecture)
3. [API Layer Design](#api-layer-design)
4. [Schema Layer Design](#schema-layer-design)
5. [Model Layer Design](#model-layer-design)
6. [Business Logic Layer Design](#business-logic-layer-design)
7. [Request-Response Flow](#request-response-flow)
8. [Error Handling](#error-handling)
9. [Current Structure Analysis](#current-structure-analysis)
10. [Improvement Recommendations](#improvement-recommendations)
11. [Code Examples](#code-examples)
12. [Checklist](#checklist)

---

## Overview

This document establishes best practices for API design in the Dalgo platform, focusing on proper separation of concerns across API, Schema, Model, and Business Logic layers. These practices ensure maintainability, testability, and consistency across the codebase.

### Core Principles

1. **Separation of Concerns**: Each layer has a single, well-defined responsibility
2. **Dependency Direction**: API → Service → Core → Models (one-way dependency)
3. **Testability**: Business logic is isolated and easily testable
4. **Consistency**: All modules follow the same patterns
5. **Maintainability**: Clear structure makes code easier to understand and modify

---

## Layer Architecture

### Standard Module Structure

```
ddpui/
├── api/
│   └── {module}_api.py          # HTTP request/response handling
│
├── services/
│   └── {module}_service.py      # Business logic & orchestration
│
├── core/
│   └── {module}/                # Domain operations (optional)
│       └── {module}_operations.py
│
├── models/
│   └── {module}.py              # Database schema (Django ORM)
│
└── schemas/
    └── {module}_schema.py       # Request/response validation (Pydantic)
```

### Layer Responsibilities Matrix

| Layer | Responsibility | Should NOT |
|-------|---------------|------------|
| **API** | HTTP handling, validation, permissions, error conversion | Business logic, database queries, external calls |
| **Service** | Business logic, transactions, orchestration, custom exceptions | HTTP concerns, direct API responses |
| **Core** | Domain operations, algorithms, external integrations | Business logic, CRUD operations |
| **Model** | Database schema, relationships, basic methods | Business logic, API concerns |
| **Schema** | Request/response validation, API contracts | Business logic, database operations |

---

## API Layer Design

### Location
`ddpui/api/{module}_api.py`

### Responsibilities

✅ **DO**:
- Handle HTTP requests and responses
- Validate request data using schemas
- Check permissions (`@has_permission`)
- Convert service exceptions to HTTP errors
- Serialize responses using schemas
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
from ddpui.services.{module}_service import (
    {Module}Service,
    {Module}Data,
    {Module}NotFoundError,
    {Module}ValidationError,
    {Module}PermissionError,
)
from ddpui.schemas.{module}_schema import (
    {Module}Create,
    {Module}Update,
    {Module}Response,
)

logger = CustomLogger("ddpui.{module}_api")

{module}_router = Router()


@{module}_router.get("/", response=List[{Module}Response])
@has_permission(["can_view_{module}s"])
def list_{module}s(request, page: int = 1, page_size: int = 10):
    """List {module}s with pagination"""
    orguser: OrgUser = request.orguser
    
    {module}s, total = {Module}Service.list_{module}s(
        org=orguser.org,
        page=page,
        page_size=page_size,
    )
    
    return [
        {Module}Response(**{module}.to_dict())
        for {module} in {module}s
    ]


@{module}_router.get("/{{id}}/", response={Module}Response)
@has_permission(["can_view_{module}s"])
def get_{module}(request, id: int):
    """Get a specific {module}"""
    orguser: OrgUser = request.orguser
    
    try:
        {module} = {Module}Service.get_{module}(id, orguser.org)
        return {Module}Response(**{module}.to_dict())
    except {Module}NotFoundError as err:
        raise HttpError(404, "{Module} not found") from err


@{module}_router.post("/", response={Module}Response)
@has_permission(["can_create_{module}s"])
def create_{module}(request, payload: {Module}Create):
    """Create a new {module}"""
    orguser: OrgUser = request.orguser
    
    try:
        {module}_data = {Module}Data(
            title=payload.title,
            # ... map payload to data class
        )
        {module} = {Module}Service.create_{module}({module}_data, orguser)
        return {Module}Response(**{module}.to_dict())
    except {Module}ValidationError as err:
        raise HttpError(400, str(err)) from err
    except Exception as e:
        logger.error(f"Error creating {module}: {e}")
        raise HttpError(500, "Failed to create {module}") from e


@{module}_router.put("/{{id}}/", response={Module}Response)
@has_permission(["can_edit_{module}s"])
def update_{module}(request, id: int, payload: {Module}Update):
    """Update a {module}"""
    orguser: OrgUser = request.orguser
    
    try:
        {module} = {Module}Service.update_{module}(
            id=id,
            org=orguser.org,
            orguser=orguser,
            data=payload,
        )
        return {Module}Response(**{module}.to_dict())
    except {Module}NotFoundError as err:
        raise HttpError(404, "{Module} not found") from err
    except {Module}ValidationError as err:
        raise HttpError(400, str(err)) from err


@{module}_router.delete("/{{id}}/")
@has_permission(["can_delete_{module}s"])
def delete_{module}(request, id: int):
    """Delete a {module}"""
    orguser: OrgUser = request.orguser
    
    try:
        {Module}Service.delete_{module}(id, orguser.org, orguser)
        return {"success": True}
    except {Module}NotFoundError as err:
        raise HttpError(404, "{Module} not found") from err
    except {Module}PermissionError as err:
        raise HttpError(403, str(err)) from err
```

### Best Practices

1. **Router Naming**: Always use `{module}_router` pattern
   ```python
   # ✅ GOOD
   charts_router = Router()
   dashboard_native_router = Router()
   
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

3. **Error Handling**: Convert service exceptions to HTTP errors
   ```python
   # ✅ GOOD
   try:
       chart = ChartService.get_chart(chart_id, orguser.org)
   except ChartNotFoundError as err:
       raise HttpError(404, "Chart not found") from err
   ```

4. **Response Serialization**: Always use response schemas
   ```python
   # ✅ GOOD
   return ChartResponse(**chart.to_dict())
   
   # ❌ BAD
   return chart.to_dict()  # No schema validation
   ```

5. **Keep API Layer Thin**: Delegate to service layer
   ```python
   # ✅ GOOD
   chart = ChartService.get_chart(chart_id, orguser.org)
   
   # ❌ BAD
   chart = Chart.objects.get(id=chart_id, org=orguser.org)  # Direct model access
   ```

---

## Schema Layer Design

### Location
`ddpui/schemas/{module}_schema.py`

### Responsibilities

✅ **DO**:
- Define request/response contracts
- Validate input data
- Transform data for API responses
- Document API structure
- Provide type safety

❌ **DON'T**:
- Contain business logic
- Perform database operations
- Make external service calls

### Structure Template

```python
"""{Module} API schemas"""

from typing import Optional, List, Dict, Any
from datetime import datetime
from ninja import Schema, Field


# Request Schemas
class {Module}Create(Schema):
    """Schema for creating a {module}"""
    title: str = Field(..., min_length=1, max_length=255, description="Title of the {module}")
    description: Optional[str] = Field(None, max_length=1000, description="Description")
    # ... other fields with validation
    
    class Config:
        json_schema_extra = {
            "example": {
                "title": "Example {Module}",
                "description": "Example description",
            }
        }


class {Module}Update(Schema):
    """Schema for updating a {module}"""
    title: Optional[str] = Field(None, min_length=1, max_length=255)
    description: Optional[str] = Field(None, max_length=1000)
    # ... other optional fields


class {Module}ListQuery(Schema):
    """Schema for list query parameters"""
    page: int = Field(1, ge=1, description="Page number (1-indexed)")
    page_size: int = Field(10, ge=1, le=100, description="Items per page")
    search: Optional[str] = Field(None, description="Search term")
    filter_type: Optional[str] = Field(None, description="Filter by type")


# Response Schemas
class {Module}Response(Schema):
    """Schema for {module} response"""
    id: int
    title: str
    description: Optional[str]
    created_at: datetime
    updated_at: datetime
    # ... other fields
    
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
    total_pages: int
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
   
   # ❌ BAD
   class ChartCreate(Chart):  # Don't inherit from model
       pass
   ```

2. **Use Field Validation**: Leverage Pydantic's validation
   ```python
   # ✅ GOOD
   title: str = Field(..., min_length=1, max_length=255)
   page: int = Field(1, ge=1, le=100)
   
   # ❌ BAD
   title: str  # No validation
   ```

3. **Document Schemas**: Add descriptions and examples
   ```python
   # ✅ GOOD
   title: str = Field(..., description="Chart title", example="Sales Overview")
   
   class Config:
       json_schema_extra = {
           "example": {"title": "Sales Overview"}
       }
   ```

4. **Keep Schemas Focused**: One schema file per module
   ```python
   # ✅ GOOD
   # schemas/chart_schema.py - all chart schemas
   
   # ❌ BAD
   # schemas/chart_create_schema.py
   # schemas/chart_response_schema.py  # Too fragmented
   ```

---

## Model Layer Design

### Location
`ddpui/models/{module}.py`

### Responsibilities

✅ **DO**:
- Define database schema (Django ORM)
- Define model relationships
- Provide basic model methods (`to_json()`, `__str__()`)
- Model-level validations (using `clean()` method)

❌ **DON'T**:
- Contain business logic
- Make API calls
- Perform complex operations
- Access external services

### Structure Template

```python
"""{Module} model for Dalgo platform"""

from django.db import models
from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui.models.{module}")


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
    
    # Metadata
    class Meta:
        db_table = "{module}"
        ordering = ["-updated_at"]
        indexes = [
            models.Index(fields=["org", "created_at"]),
        ]
    
    def __str__(self):
        return f"{self.title} ({self.id})"
    
    def to_json(self):
        """Return JSON representation"""
        return {
            "id": self.id,
            "title": self.title,
            "description": self.description,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }
    
    def clean(self):
        """Model-level validation"""
        from django.core.exceptions import ValidationError
        
        if not self.title or not self.title.strip():
            raise ValidationError({"title": "Title cannot be empty"})
```

### Best Practices

1. **Use Proper Field Types**: Choose appropriate Django field types
   ```python
   # ✅ GOOD
   title = models.CharField(max_length=255)
   description = models.TextField(blank=True, null=True)
   is_active = models.BooleanField(default=True)
   created_at = models.DateTimeField(auto_now_add=True)
   
   # ❌ BAD
   title = models.TextField()  # Use CharField for short text
   ```

2. **Define Relationships Properly**: Use appropriate `on_delete` behavior
   ```python
   # ✅ GOOD
   org = models.ForeignKey(Org, on_delete=models.CASCADE)
   created_by = models.ForeignKey(OrgUser, on_delete=models.CASCADE)
   
   # ❌ BAD
   org = models.ForeignKey(Org)  # Missing on_delete
   ```

3. **Add Meta Information**: Include ordering, indexes, constraints
   ```python
   # ✅ GOOD
   class Meta:
       db_table = "chart"
       ordering = ["-updated_at"]
       indexes = [
           models.Index(fields=["org", "created_at"]),
       ]
   ```

4. **Keep Models Simple**: No business logic
   ```python
   # ✅ GOOD
   def to_json(self):
       return {"id": self.id, "title": self.title}
   
   # ❌ BAD
   def validate_and_save(self):  # Business logic in model
       if self.needs_validation:
           validate(self)
       self.save()
   ```

---

## Business Logic Layer Design

### Location
`ddpui/services/{module}_service.py`

### Responsibilities

✅ **DO**:
- Implement business logic and rules
- Manage transactions
- Orchestrate multiple operations
- Handle cross-cutting concerns (caching, logging)
- Define custom exceptions
- Coordinate between models and core operations

❌ **DON'T**:
- Handle HTTP concerns
- Make direct API responses
- Contain domain algorithms (use core layer)

### Structure Template

```python
"""{Module} service for business logic

This module encapsulates all {module}-related business logic,
separating it from the API layer for better testability and maintainability.
"""

from typing import Optional, List, Dict, Any, Tuple
from dataclasses import dataclass

from django.db.models import Q
from django.db import transaction

from ddpui.models.{module} import {Module}
from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui.{module}_service")


# =============================================================================
# Custom Exceptions
# =============================================================================

class {Module}ServiceError(Exception):
    """Base exception for {module} service errors"""
    def __init__(self, message: str, error_code: str = "{MODULE}_ERROR"):
        self.message = message
        self.error_code = error_code
        super().__init__(self.message)


class {Module}NotFoundError({Module}ServiceError):
    """Raised when {module} is not found"""
    def __init__(self, {module}_id: int):
        super().__init__(
            f"{Module} with id {module}_id not found",
            "{MODULE}_NOT_FOUND"
        )
        self.{module}_id = {module}_id


class {Module}ValidationError({Module}ServiceError):
    """Raised when {module} validation fails"""
    def __init__(self, message: str):
        super().__init__(message, "VALIDATION_ERROR")


class {Module}PermissionError({Module}ServiceError):
    """Raised when user doesn't have permission"""
    def __init__(self, message: str = "Permission denied"):
        super().__init__(message, "PERMISSION_DENIED")


# =============================================================================
# Data Classes
# =============================================================================

@dataclass
class {Module}Data:
    """Data class for {module} creation/update"""
    title: str
    description: Optional[str] = None
    # ... other fields


# =============================================================================
# Service Class
# =============================================================================

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
    def create_{module}(data: {Module}Data, orguser: OrgUser) -> {Module}:
        """Create a new {module}.
        
        Args:
            data: {Module} creation data
            orguser: The user creating the {module}
            
        Returns:
            Created {Module} instance
            
        Raises:
            {Module}ValidationError: If {module} configuration is invalid
        """
        # Business logic validation
        if not data.title or not data.title.strip():
            raise {Module}ValidationError("Title is required")
        
        # Create {module}
        {module} = {Module}.objects.create(
            title=data.title,
            description=data.description,
            created_by=orguser,
            last_modified_by=orguser,
            org=orguser.org,
        )
        
        logger.info(f"Created {module} {module}.id for org {orguser.org.id}")
        return {module}
    
    @staticmethod
    @transaction.atomic
    def update_{module}(
        {module}_id: int,
        org: Org,
        orguser: OrgUser,
        data: {Module}Data,
    ) -> {Module}:
        """Update an existing {module}.
        
        Args:
            {module}_id: The {module} ID
            org: The organization
            orguser: The user making the update
            data: Update data
            
        Returns:
            Updated {Module} instance
            
        Raises:
            {Module}NotFoundError: If {module} doesn't exist
            {Module}ValidationError: If updated configuration is invalid
        """
        {module} = {Module}Service.get_{module}({module}_id, org)
        
        # Business logic validation
        if data.title and not data.title.strip():
            raise {Module}ValidationError("Title cannot be empty")
        
        # Apply updates
        if data.title is not None:
            {module}.title = data.title
        if data.description is not None:
            {module}.description = data.description
        
        {module}.last_modified_by = orguser
        {module}.save()
        
        logger.info(f"Updated {module} {module}.id")
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
        
        # Permission check
        if {module}.created_by != orguser:
            raise {Module}PermissionError("You can only delete {module}s you created.")
        
        {module}_title = {module}.title
        {module}.delete()
        
        logger.info(f"Deleted {module} '{module}_title' (id={module}_id) by {orguser.user.email}")
        return True
```

### Best Practices

1. **Use Custom Exceptions**: Define service-specific exceptions
   ```python
   # ✅ GOOD
   class ChartServiceError(Exception):
       pass
   
   class ChartNotFoundError(ChartServiceError):
       pass
   ```

2. **Use Data Classes**: For structured data transfer
   ```python
   # ✅ GOOD
   @dataclass
   class ChartData:
       title: str
       chart_type: str
   ```

3. **Use Transactions**: For operations that modify data
   ```python
   # ✅ GOOD
   @transaction.atomic
   def create_chart(data, orguser):
       # Multiple database operations
       pass
   ```

4. **Keep Business Logic Here**: Not in API or models
   ```python
   # ✅ GOOD
   def create_chart(data, orguser):
       # Business logic here
       if data.needs_validation:
           validate_chart_data(data)
       return Chart.objects.create(...)
   ```

---

## Request-Response Flow

### Standard Flow

```
1. Client Request
   ↓
2. API Layer (api/{module}_api.py)
   ├── Validate request (Pydantic schema)
   ├── Check permissions (@has_permission)
   ├── Extract orguser from request
   └── Call service method
   ↓
3. Service Layer (services/{module}_service.py)
   ├── Business logic validation
   ├── Transaction management
   ├── Call core operations (if needed)
   ├── Database operations (via models)
   └── Return model/data class
   ↓
4. Core Layer (core/{module}/) - Optional
   ├── Domain operations
   ├── Query building
   ├── Data transformations
   └── External service calls
   ↓
5. Model Layer (models/{module}.py)
   └── Database operations
   ↓
6. Service Layer (return)
   └── Return model/data class
   ↓
7. API Layer (serialize)
   ├── Convert model to response schema
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
@charts_router.post("/", response=ChartResponse)
@has_permission(["can_create_charts"])
def create_chart(request, payload: ChartCreate):
    orguser = request.orguser
    
    # Validate payload (automatic via Pydantic)
    # Check permissions (automatic via decorator)
    
    chart_data = ChartData(
        title=payload.title,
        chart_type=payload.chart_type,
        # ...
    )
    
    chart = ChartService.create_chart(chart_data, orguser)
    return ChartResponse(**chart.to_dict())

# 3. Service Layer (services/chart_service.py)
@staticmethod
@transaction.atomic
def create_chart(data: ChartData, orguser: OrgUser) -> Chart:
    # Business logic validation
    is_valid, error = ChartValidator.validate_chart_config(...)
    if not is_valid:
        raise ChartValidationError(error)
    
    # Create chart
    chart = Chart.objects.create(
        title=data.title,
        chart_type=data.chart_type,
        created_by=orguser,
        org=orguser.org,
        # ...
    )
    
    return chart

# 4. Model Layer (models/visualization.py)
class Chart(models.Model):
    title = models.CharField(max_length=255)
    # ... fields
    
    def to_dict(self):
        return {
            "id": self.id,
            "title": self.title,
            # ...
        }

# 5. Response
{
    "id": 1,
    "title": "Sales Overview",
    "chart_type": "bar",
    "created_at": "2025-01-22T10:00:00Z"
}
```

---

## Error Handling

### Error Handling Flow

```
Service Layer Exception
  ↓
API Layer catches exception
  ↓
Convert to HttpError
  ↓
Return HTTP response with appropriate status code
```

### Exception Hierarchy

```python
# Service Layer Exceptions
{Module}ServiceError (base)
  ├── {Module}NotFoundError → 404 Not Found
  ├── {Module}ValidationError → 400 Bad Request
  ├── {Module}PermissionError → 403 Forbidden
  └── {Module}ServiceError → 500 Internal Server Error
```

### Error Handling Pattern

```python
# ✅ GOOD: Service Layer
class ChartService:
    @staticmethod
    def get_chart(chart_id: int, org: Org) -> Chart:
        try:
            return Chart.objects.get(id=chart_id, org=org)
        except Chart.DoesNotExist:
            raise ChartNotFoundError(chart_id)

# ✅ GOOD: API Layer
@charts_router.get("/{chart_id}/")
@has_permission(["can_view_charts"])
def get_chart(request, chart_id: int):
    try:
        chart = ChartService.get_chart(chart_id, request.orguser.org)
        return ChartResponse(**chart.to_dict())
    except ChartNotFoundError as err:
        raise HttpError(404, "Chart not found") from err
    except ChartValidationError as err:
        raise HttpError(400, str(err)) from err
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise HttpError(500, "Internal server error") from e

# ❌ BAD: Direct exception in API
@charts_router.get("/{chart_id}/")
def get_chart(request, chart_id: int):
    chart = Chart.objects.get(id=chart_id)  # No error handling
    return chart.to_dict()
```

---

## Current Structure Analysis

### ✅ Well-Structured Modules

#### Charts Module
- ✅ API: `api/charts_api.py` - Thin layer
- ✅ Service: `services/chart_service.py` - Business logic
- ✅ Core: `core/charts/` - Domain operations
- ✅ Models: `models/visualization.py` - Clean models
- ✅ Schemas: `schemas/chart_schema.py` - Complete schemas

#### Dashboard Module
- ✅ API: `api/dashboard_native_api.py` - Thin layer
- ✅ Service: `services/dashboard_service.py` - Business logic
- ✅ Models: `models/dashboard.py` - Clean models
- ✅ Schemas: `schemas/dashboard_schema.py` - Complete schemas

### ⚠️ Partially Structured Modules

#### Warehouse Module
- ✅ API: `api/warehouse_api.py`
- ⚠️ Service: `services/warehouse_service.py` - Minimal implementation
- ⚠️ Core: `core/warehousefunctions.py` - Mixed patterns
- ✅ Models: `models/org.py` (OrgWarehouse)
- ✅ Schemas: `schemas/warehouse_api_schemas.py`

**Issues**:
- Business logic scattered between API and core
- Service layer is minimal
- Unclear separation between core and service

#### DBT Module
- ✅ API: `api/dbt_api.py`
- ❌ Service: Missing
- ⚠️ Core: `core/dbtfunctions.py` - Contains business logic
- ✅ Models: `models/dbt_workflow.py`
- ⚠️ Schemas: Partial

**Issues**:
- No service layer
- Business logic in core layer
- Some logic in API layer

### ❌ Unstructured Modules

#### AI Module
- ✅ API: `api/ai_api.py`
- ❌ Service: Missing
- ✅ Core: `core/ai/` - Well-organized
- ✅ Models: `models/ai_chat_logging.py`
- ❌ Schemas: Embedded in API file

**Issues**:
- Business logic in API layer
- No service layer
- Schemas in API file instead of separate file

#### Org Settings Module
- ✅ API: `api/org_settings_api.py`
- ❌ Service: Missing
- ❌ Core: Not needed
- ✅ Models: `models/org_settings.py`
- ⚠️ Schemas: In models file

**Issues**:
- Business logic directly in API
- No service layer
- Schemas in models file instead of schemas/

#### Dashboard Chat Module
- ✅ API: `api/dashboard_chat_api.py`
- ❌ Service: Missing
- ✅ Core: `core/ai/` - Uses AI core
- ✅ Models: `models/ai_chat_logging.py`
- ❌ Schemas: Missing

**Issues**:
- No service layer
- Business logic in API
- Missing schemas

---

## Improvement Recommendations

### Priority 1: Create Missing Service Layers

#### 1. AI Service (`services/ai_service.py`)

**Current**: Business logic in `api/ai_api.py`

**Recommended**:
```python
# services/ai_service.py
class AIService:
    @staticmethod
    def ensure_org_ai_enabled(org_settings: OrgSettings) -> None:
        """Ensure organization has AI features enabled"""
        pass
    
    @staticmethod
    def chat_completion(messages: List[AIMessage], **kwargs) -> AIResponse:
        """Generate chat completion"""
        pass
```

**Benefits**:
- Testable business logic
- Consistent with charts/dashboard pattern
- Easier to maintain

#### 2. Org Settings Service (`services/org_settings_service.py`)

**Current**: Business logic in `api/org_settings_api.py`

**Recommended**:
```python
# services/org_settings_service.py
class OrgSettingsService:
    @staticmethod
    def get_settings(org: Org) -> OrgSettings:
        """Get org settings"""
        pass
    
    @staticmethod
    def update_settings(org: Org, orguser: OrgUser, data: OrgSettingsUpdateData) -> OrgSettings:
        """Update org settings with business logic"""
        # Move notification logic here
        pass
```

**Benefits**:
- Remove business logic from API
- Better testability
- Consistent error handling

#### 3. DBT Service (`services/dbt_service.py`)

**Current**: Business logic in `core/dbtfunctions.py` and `api/dbt_api.py`

**Recommended**:
```python
# services/dbt_service.py
class DBTService:
    @staticmethod
    def setup_workspace(org: Org, data: DBTWorkspaceData) -> Task:
        """Setup DBT workspace"""
        pass
    
    @staticmethod
    def update_github_config(org: Org, data: DBTGitHubData) -> None:
        """Update GitHub configuration"""
        pass
```

**Benefits**:
- Clear separation of concerns
- Business logic in one place
- Easier to test

### Priority 2: Extract Schemas

#### Move Schemas from API Files

**Current**:
- AI schemas in `api/ai_api.py`
- Org Settings schemas in `models/org_settings.py`

**Recommended**:
- `schemas/ai_schema.py` - All AI schemas
- `schemas/org_settings_schema.py` - All org settings schemas

### Priority 3: Standardize Patterns

#### Router Naming
```python
# ✅ Standardize to:
ai_router = Router()
dashboard_chat_router = Router()
org_settings_router = Router()
```

#### Error Handling
```python
# ✅ Standard pattern:
try:
    result = Service.method(...)
except ServiceNotFoundError as err:
    raise HttpError(404, "Not found") from err
except ServiceValidationError as err:
    raise HttpError(400, str(err)) from err
```

#### Import Patterns
```python
# ✅ Preferred:
from ddpui.core.charts import charts_service
charts_service.build_chart_query(...)

# ⚠️ Acceptable but less preferred:
from ddpui.core.charts.charts_service import build_chart_query
build_chart_query(...)
```

---

## Code Examples

### Complete Example: Org Settings Module

#### Current Implementation (❌ Bad)

```python
# api/org_settings_api.py
@router.put("/", response=dict)
def update_org_settings(request, payload: UpdateOrgSettingsSchema):
    orguser = request.orguser
    org_settings, created = OrgSettings.objects.get_or_create(...)
    
    # Business logic in API ❌
    ai_settings_changed = False
    if payload.ai_data_sharing_enabled is not None:
        if not org_settings.ai_data_sharing_enabled and payload.ai_data_sharing_enabled:
            ai_chat_being_enabled = True
        # ...
    
    # Notification logic in API ❌
    if ai_chat_being_enabled:
        send_ai_chat_enabled_notification(...)
    
    return {"success": True, "res": org_settings.to_dict()}
```

#### Recommended Implementation (✅ Good)

```python
# schemas/org_settings_schema.py
class UpdateOrgSettingsSchema(Schema):
    ai_data_sharing_enabled: Optional[bool] = None
    ai_logging_acknowledged: Optional[bool] = None

class OrgSettingsResponse(Schema):
    organization_logo_filename: Optional[str]
    ai_data_sharing_enabled: bool
    ai_logging_acknowledged: bool
    ai_settings_accepted_by_email: Optional[str]
    ai_settings_accepted_at: Optional[datetime]

# services/org_settings_service.py
@dataclass
class OrgSettingsUpdateData:
    ai_data_sharing_enabled: Optional[bool] = None
    ai_logging_acknowledged: Optional[bool] = None

class OrgSettingsService:
    @staticmethod
    @transaction.atomic
    def update_settings(
        org: Org,
        orguser: OrgUser,
        data: OrgSettingsUpdateData
    ) -> OrgSettings:
        org_settings = OrgSettingsService.get_settings(org)
        
        # Business logic
        ai_settings_changed = False
        ai_chat_being_enabled = False
        
        if data.ai_data_sharing_enabled is not None:
            if not org_settings.ai_data_sharing_enabled and data.ai_data_sharing_enabled:
                ai_chat_being_enabled = True
            if org_settings.ai_data_sharing_enabled != data.ai_data_sharing_enabled:
                ai_settings_changed = True
            org_settings.ai_data_sharing_enabled = data.ai_data_sharing_enabled
        
        if data.ai_logging_acknowledged is not None:
            if org_settings.ai_logging_acknowledged != data.ai_logging_acknowledged:
                ai_settings_changed = True
            org_settings.ai_logging_acknowledged = data.ai_logging_acknowledged
        
        if ai_settings_changed:
            org_settings.ai_settings_accepted_by = orguser.user
            org_settings.ai_settings_accepted_at = timezone.now()
        
        org_settings.save()
        
        # Notification logic
        if ai_chat_being_enabled:
            OrgSettingsService._send_ai_enabled_notification(org, orguser.user.email)
        
        return org_settings

# api/org_settings_api.py
@org_settings_router.put("/", response=OrgSettingsResponse)
@has_permission(["can_manage_org_settings"])
def update_org_settings(request, payload: UpdateOrgSettingsSchema):
    orguser: OrgUser = request.orguser
    
    try:
        update_data = OrgSettingsUpdateData(
            ai_data_sharing_enabled=payload.ai_data_sharing_enabled,
            ai_logging_acknowledged=payload.ai_logging_acknowledged,
        )
        
        org_settings = OrgSettingsService.update_settings(
            org=orguser.org,
            orguser=orguser,
            data=update_data
        )
        
        return OrgSettingsResponse(**org_settings.to_dict())
    except OrgSettingsServiceError as e:
        raise HttpError(400, str(e)) from e
    except Exception as e:
        logger.error(f"Error updating org settings: {e}")
        raise HttpError(500, "Failed to update org settings") from e
```

---

## Checklist

### When Creating a New Module

- [ ] **API Layer** (`api/{module}_api.py`)
  - [ ] Router named `{module}_router`
  - [ ] All endpoints have `@has_permission` decorator
  - [ ] Request validation using schemas
  - [ ] Service exceptions converted to HttpError
  - [ ] Response serialization using schemas
  - [ ] No business logic in API layer

- [ ] **Service Layer** (`services/{module}_service.py`)
  - [ ] Custom exceptions defined (`{Module}ServiceError`, etc.)
  - [ ] Data classes for structured data (`@dataclass`)
  - [ ] Service class with static methods
  - [ ] Business logic implemented
  - [ ] Transaction management where needed
  - [ ] Proper logging

- [ ] **Schema Layer** (`schemas/{module}_schema.py`)
  - [ ] Request schemas (Create, Update, Query)
  - [ ] Response schemas (Response, ListResponse)
  - [ ] Field validation and descriptions
  - [ ] Examples in Config

- [ ] **Model Layer** (`models/{module}.py`)
  - [ ] Django ORM model definition
  - [ ] Proper field types and relationships
  - [ ] Meta class with ordering, indexes
  - [ ] `to_json()` or `to_dict()` method
  - [ ] `__str__()` method
  - [ ] No business logic

- [ ] **Core Layer** (`core/{module}/`) - Optional
  - [ ] Domain operations only
  - [ ] No business logic
  - [ ] Reusable utilities

- [ ] **General**
  - [ ] Module-specific logger names
  - [ ] Consistent import patterns
  - [ ] Error handling follows patterns
  - [ ] Documentation/docstrings

### When Reviewing Existing Code

- [ ] Business logic is in service layer, not API
- [ ] Schemas are in `schemas/` directory, not embedded
- [ ] Router naming follows `{module}_router` pattern
- [ ] Error handling converts service exceptions to HttpError
- [ ] Models don't contain business logic
- [ ] Core layer only contains domain operations

---

## Summary

### Key Takeaways

1. **API Layer**: Thin layer for HTTP handling only
2. **Service Layer**: Business logic and orchestration
3. **Schema Layer**: Request/response validation
4. **Model Layer**: Database schema only
5. **Core Layer**: Domain operations (optional)

### Current Status

- ✅ **Charts & Dashboard**: Excellent examples to follow
- ⚠️ **Warehouse, DBT**: Need service layer improvements
- ❌ **AI, Org Settings, Dashboard Chat**: Need service layers

### Next Steps

1. Create service layers for missing modules
2. Extract schemas from API files
3. Standardize router naming
4. Refactor business logic from API to services
5. Update documentation with patterns

---

**Document Version**: 1.0  
**Last Updated**: 2025-01-22  
**Maintained By**: Dalgo Platform Team

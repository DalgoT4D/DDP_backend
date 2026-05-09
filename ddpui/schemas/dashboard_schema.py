"""Dashboard schemas for API request/response validation

This module contains all Pydantic schemas for dashboard-related API endpoints.
"""

from datetime import datetime
from typing import Optional, List

from ninja import Schema


# =============================================================================
# Dashboard Schemas
# =============================================================================


class DashboardCreate(Schema):
    """Schema for creating a dashboard"""

    title: str
    description: Optional[str] = None
    grid_columns: int = 12


class DashboardUpdate(Schema):
    """Schema for updating a dashboard"""

    title: Optional[str] = None
    description: Optional[str] = None
    grid_columns: Optional[int] = None
    target_screen_size: Optional[str] = None
    layout_config: Optional[list[dict]] = None
    components: Optional[dict] = None
    filter_layout: Optional[str] = None
    is_published: Optional[bool] = None


class DashboardFilterResponse(Schema):
    """Response schema for dashboard filter"""

    id: int
    dashboard_id: int
    name: str
    filter_type: str
    schema_name: str
    table_name: str
    column_name: str
    settings: dict
    order: int
    created_at: datetime
    updated_at: datetime

    @classmethod
    def from_model(cls, filter_obj) -> "DashboardFilterResponse":
        """Build response from a DashboardFilter model instance."""
        return cls(
            id=filter_obj.id,
            dashboard_id=filter_obj.dashboard_id,
            name=filter_obj.name,
            filter_type=filter_obj.filter_type,
            schema_name=filter_obj.schema_name,
            table_name=filter_obj.table_name,
            column_name=filter_obj.column_name,
            settings=filter_obj.settings,
            order=filter_obj.order,
            created_at=filter_obj.created_at,
            updated_at=filter_obj.updated_at,
        )


class DashboardResponse(Schema):
    """Response schema for dashboard"""

    id: int
    title: str
    description: Optional[str] = None
    dashboard_type: str
    grid_columns: int
    target_screen_size: str
    filter_layout: str
    layout_config: list[dict]
    components: dict
    is_published: bool
    published_at: Optional[datetime] = None
    is_locked: bool = False
    locked_by: Optional[str] = None
    created_by: str
    org_id: int
    last_modified_by: Optional[str] = None
    created_at: datetime
    updated_at: datetime
    filters: List[DashboardFilterResponse] = []


# =============================================================================
# Filter Schemas
# =============================================================================


class FilterCreate(Schema):
    """Schema for creating a filter"""

    name: Optional[str] = None
    filter_type: str
    schema_name: str
    table_name: str
    column_name: str
    settings: dict = {}
    order: int = 0


class FilterUpdate(Schema):
    """Schema for updating a filter"""

    name: Optional[str] = None
    filter_type: Optional[str] = None
    schema_name: Optional[str] = None
    table_name: Optional[str] = None
    column_name: Optional[str] = None
    settings: Optional[dict] = None
    order: Optional[int] = None


class FilterOptionResponse(Schema):
    """Schema for individual filter option"""

    label: str
    value: str
    count: Optional[int] = None


class FilterOptionsResponse(Schema):
    """Response schema for filter options"""

    options: List[FilterOptionResponse]
    total_count: int


# =============================================================================
# Lock Schemas
# =============================================================================


class LockResponse(Schema):
    """Response schema for dashboard lock"""

    lock_token: str
    expires_at: datetime
    locked_by: str


# =============================================================================
# Sharing Schemas
# =============================================================================


class ShareToggle(Schema):
    """Schema for toggling public sharing (used by dashboards and reports)"""

    is_public: bool


class ShareResponse(Schema):
    """Schema for share response (used by dashboards and reports)"""

    is_public: bool
    public_url: Optional[str] = None
    public_share_token: Optional[str] = None
    message: str


class ShareStatus(Schema):
    """Schema for share status response (used by dashboards and reports)"""

    is_public: bool
    public_url: Optional[str] = None
    public_access_count: int
    last_public_accessed: Optional[datetime] = None
    public_shared_at: Optional[datetime] = None


# Backwards-compatible aliases
DashboardShareToggle = ShareToggle
DashboardShareResponse = ShareResponse
DashboardShareStatus = ShareStatus


# =============================================================================
# Landing Page Schemas
# =============================================================================


class LandingPageResponse(Schema):
    """Response schema for landing page operations"""

    success: bool
    message: str = ""


class LandingPageResolveResponse(Schema):
    """Response schema for resolved landing page"""

    dashboard_id: Optional[int] = None
    dashboard_title: Optional[str] = None
    dashboard_type: Optional[str] = None
    source: str  # "personal", "org_default", or "none"

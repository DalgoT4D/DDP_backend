"""Report schemas for request/response validation"""

from typing import Optional, Dict, Any
from datetime import date, datetime
from ninja import Schema, Field


# Request schemas


class SnapshotCreate(Schema):
    """Schema for creating a snapshot from a dashboard"""

    title: str = Field(..., min_length=1, max_length=255)
    dashboard_id: int
    period_start: date
    period_end: Optional[date] = None  # None = "till today" (rolling)


class SnapshotUpdate(Schema):
    """Schema for updating a snapshot (all fields optional)"""

    summary: Optional[str] = Field(None, max_length=10000)


# Response schemas


class SnapshotListResponse(Schema):
    """Schema for snapshot list item"""

    id: int
    title: str
    dashboard_title: Optional[str]  # From frozen_dashboard, not a live FK
    period_start: date
    period_end: date  # Resolved: stored value or today if rolling
    is_rolling_end: bool  # True = "till today" (period_end was NULL in DB)
    status: str
    summary: Optional[str]
    created_by: Optional[str]
    created_at: datetime


class SnapshotViewResponse(Schema):
    """Schema for snapshot view data for rendering"""

    dashboard_data: Dict[str, Any]
    report_metadata: Dict[str, Any]
    frozen_chart_configs: Dict[str, Any]

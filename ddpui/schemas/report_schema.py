"""Report schemas for request/response validation"""

from typing import Optional, Dict, Any
from datetime import date, datetime
from ninja import Schema, Field

# Sharing schemas are shared with dashboards — import from dashboard_schema
from ddpui.schemas.dashboard_schema import ShareToggle, ShareResponse, ShareStatus  # noqa: F401


# Shared schemas


class DateColumnSchema(Schema):
    """Identifies a datetime column on a dashboard filter"""

    schema_name: str
    table_name: str
    column_name: str


# Request schemas


class SnapshotCreate(Schema):
    """Schema for creating a snapshot from a dashboard"""

    title: str = Field(..., min_length=1, max_length=255)
    dashboard_id: int
    date_column: DateColumnSchema
    period_start: Optional[date] = None  # None = no lower bound
    period_end: date


class SnapshotUpdate(Schema):
    """Schema for updating a snapshot (all fields optional)"""

    summary: Optional[str] = Field(None, max_length=10000)


# Response schemas


class SnapshotListResponse(Schema):
    """Schema for snapshot list item"""

    id: int
    title: str
    dashboard_title: Optional[str]  # From frozen_dashboard, not a live FK
    date_column: Optional[Dict[str, str]]
    period_start: Optional[date]
    period_end: date
    status: str
    summary: Optional[str]
    created_by: Optional[str]
    created_at: datetime

    @classmethod
    def from_model(cls, snapshot) -> "SnapshotListResponse":
        """Create response from ReportSnapshot model instance"""
        return cls(
            id=snapshot.id,
            title=snapshot.title,
            dashboard_title=snapshot.frozen_dashboard.get("title") if snapshot.frozen_dashboard else None,
            date_column=snapshot.date_column or None,
            period_start=snapshot.period_start,
            period_end=snapshot.period_end,
            status=snapshot.status,
            summary=snapshot.summary,
            created_by=snapshot.created_by.user.email if snapshot.created_by else None,
            created_at=snapshot.created_at,
        )


class SnapshotViewResponse(Schema):
    """Schema for snapshot view data for rendering"""

    dashboard_data: Dict[str, Any]
    report_metadata: Dict[str, Any]
    frozen_chart_configs: Dict[str, Any]


class DatetimeColumnResponse(Schema):
    """A datetime column discovered from a dashboard's chart tables"""

    schema_name: str
    table_name: str
    column_name: str
    data_type: str
    is_dashboard_filter: bool = False


class SnapshotUpdateResponse(Schema):
    """Schema for snapshot update response"""

    summary: Optional[str]


class SnapshotDeleteResponse(Schema):
    """Schema for snapshot delete response - empty success response"""

    pass

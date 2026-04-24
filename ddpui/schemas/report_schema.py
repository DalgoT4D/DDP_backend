"""Report schemas for request/response validation"""

from typing import Optional, Dict, Any, List
from datetime import date, datetime
from ninja import Schema, Field

# Sharing schemas are shared with dashboards — import from dashboard_schema
from ddpui.schemas.dashboard_schema import ShareToggle, ShareResponse, ShareStatus  # noqa: F401


# Shared schemas


class DateColumnSchema(Schema):
    """Identifies a datetime column on a dashboard filter"""

    schema_name: str = Field(..., min_length=1)
    table_name: str = Field(..., min_length=1)
    column_name: str = Field(..., min_length=1)


class FrozenDashboardConfig(Schema):
    """Schema for frozen dashboard config stored in snapshots"""

    dashboard_id: int
    title: str
    description: Optional[str] = None
    grid_columns: Optional[int] = None
    target_screen_size: Optional[str] = None
    layout_config: Optional[Any] = None
    components: Optional[Dict[str, Any]] = None
    filter_layout: Optional[str] = None
    filters: List[Dict[str, Any]] = []


class FrozenChartConfig(Schema):
    """Schema for a single frozen chart config stored in snapshots"""

    id: int
    title: str
    description: Optional[str] = None
    chart_type: str
    schema_name: str
    table_name: str
    extra_config: Optional[Dict[str, Any]] = None


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


class SnapshotResponse(Schema):
    """Schema for snapshot list item"""

    id: int
    title: str
    dashboard_title: Optional[str] = None  # From frozen_dashboard, not a live FK
    date_column: Optional[Dict[str, str]] = None
    period_start: Optional[date] = None
    period_end: date
    summary: Optional[str] = None
    created_by: Optional[str] = None
    created_at: datetime
    updated_at: datetime

    @classmethod
    def from_model(cls, snapshot) -> "SnapshotResponse":
        """Create response from ReportSnapshot model instance"""
        return cls(
            id=snapshot.id,
            title=snapshot.title,
            dashboard_title=snapshot.frozen_dashboard.get("title")
            if snapshot.frozen_dashboard
            else None,
            date_column=snapshot.date_column or None,
            period_start=snapshot.period_start,
            period_end=snapshot.period_end,
            summary=snapshot.summary,
            created_by=snapshot.created_by.user.email if snapshot.created_by else None,
            created_at=snapshot.created_at,
            updated_at=snapshot.updated_at,
        )


class SnapshotViewResponse(Schema):
    """Schema for snapshot view data for rendering"""

    dashboard_data: Dict[str, Any]
    report_metadata: Dict[str, Any]
    frozen_chart_configs: Dict[str, Any]

    @classmethod
    def from_view_data(cls, view_data: dict) -> "SnapshotViewResponse":
        """Create response from view data dict"""
        return cls(**view_data)


class DatetimeColumnResponse(Schema):
    """A datetime column discovered from a dashboard's chart tables"""

    schema_name: str
    table_name: str
    column_name: str
    data_type: str
    is_dashboard_filter: bool = False


class SnapshotUpdateResponse(Schema):
    """Schema for snapshot update response"""

    summary: Optional[str] = None

    @classmethod
    def from_model(cls, snapshot) -> "SnapshotUpdateResponse":
        """Create response from ReportSnapshot model instance"""
        return cls(summary=snapshot.summary)


class SnapshotDeleteResponse(Schema):
    """Schema for snapshot delete response - empty success response"""

    pass


# =============================================================================
# Comment Schemas
# =============================================================================


class CommentCreate(Schema):
    """Schema for creating a comment on a report snapshot"""

    target_type: str = Field(..., description="'summary' or 'chart'")
    chart_id: Optional[int] = Field(None, description="Required when target_type='chart'")
    content: str = Field(..., min_length=1, max_length=5000)
    mentioned_emails: List[str] = []


class CommentUpdate(Schema):
    """Schema for updating a comment"""

    content: str = Field(..., min_length=1, max_length=5000)
    mentioned_emails: List[str] = []


class MarkReadRequest(Schema):
    """Schema for marking comments as read"""

    target_type: str = Field(..., description="'summary' or 'chart'")
    chart_id: Optional[int] = None


class CommentResponse(Schema):
    """Schema for a single comment"""

    id: int
    target_type: str
    snapshot_id: int
    chart_id: Optional[int] = None
    content: str
    author_email: str
    is_new: bool = False
    is_deleted: bool = False
    created_at: datetime
    updated_at: datetime
    mentioned_emails: List[str] = []

    @classmethod
    def from_model(cls, comment) -> "CommentResponse":
        """Create response from Comment model instance"""
        return cls(
            id=comment.id,
            target_type=comment.target_type,
            snapshot_id=comment.snapshot_id,
            chart_id=comment.snapshot_chart_id,
            content=comment.content,
            author_email=comment.author.user.email,
            is_new=getattr(comment, "is_new", False),
            is_deleted=comment.is_deleted,
            created_at=comment.created_at,
            updated_at=comment.updated_at,
            mentioned_emails=comment.mentioned_emails or [],
        )


class CommentStateEntry(Schema):
    """Icon state for a single target"""

    target_type: str  # "summary" | "chart"
    chart_id: Optional[int] = None  # set when target_type="chart"
    state: str  # "none" | "unread" | "read" | "mentioned"


class CommentStatesResponse(Schema):
    """Icon states per target for a snapshot"""

    states: List[CommentStateEntry]


class MentionableUserResponse(Schema):
    """Org user available for @mention"""

    email: str

    @classmethod
    def from_orguser(cls, orguser) -> "MentionableUserResponse":
        return cls(email=orguser.user.email)


# =============================================================================
# Share via Email Schemas
# =============================================================================


class ReportShareViaEmailRequest(Schema):
    """Schema for sharing a report via email"""

    recipient_emails: list[str] = Field(..., min_items=1, max_items=20)
    subject: Optional[str] = Field(None, max_length=255)


class ReportShareViaEmailResponse(Schema):
    """Schema for share-via-email response"""

    recipients_count: int
    message: str

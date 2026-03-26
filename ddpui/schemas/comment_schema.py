"""Comment schemas for request/response validation"""

from typing import Optional, List
from datetime import datetime
from ninja import Schema, Field


# =============================================================================
# Request Schemas
# =============================================================================


class CommentCreate(Schema):
    """Schema for creating a comment on a report snapshot"""

    snapshot_id: int
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

    snapshot_id: int
    target_type: str = Field(..., description="'summary' or 'chart'")
    chart_id: Optional[int] = None


# =============================================================================
# Response Schemas
# =============================================================================


class CommentAuthorResponse(Schema):
    """Author info for a comment"""

    email: str
    name: Optional[str] = None


class CommentResponse(Schema):
    """Schema for a single comment"""

    id: int
    target_type: str
    snapshot_id: int
    chart_id: Optional[int] = None
    content: str
    author: CommentAuthorResponse
    is_new: bool = False
    is_deleted: bool = False
    created_at: datetime
    updated_at: datetime
    mentioned_emails: List[str] = []

    @classmethod
    def from_model(cls, comment) -> "CommentResponse":
        """Create response from Comment model instance"""
        author = CommentAuthorResponse(
            email=comment.author.user.email,
            name=_get_user_name(comment.author),
        )

        return cls(
            id=comment.id,
            target_type=comment.target_type,
            snapshot_id=comment.snapshot_id,
            chart_id=comment.snapshot_chart_id,
            content=comment.content,
            author=author,
            is_new=getattr(comment, "is_new", False),
            is_deleted=comment.is_deleted,
            created_at=comment.created_at,
            updated_at=comment.updated_at,
            mentioned_emails=comment.mentioned_emails or [],
        )


class CommentStateEntry(Schema):
    """State and counts for a single target"""

    target_type: str  # "summary" | "chart"
    chart_id: Optional[int] = None  # set when target_type="chart"
    state: str  # "none" | "unread" | "read" | "mentioned"
    count: int  # total number of comments
    unread_count: int  # number of unread comments


class CommentStatesResponse(Schema):
    """Icon states per target for a snapshot"""

    states: List[CommentStateEntry]


class MentionableUserResponse(Schema):
    """Org user available for @mention"""

    email: str
    name: Optional[str] = None

    @classmethod
    def from_orguser(cls, orguser) -> "MentionableUserResponse":
        return cls(
            email=orguser.user.email,
            name=_get_user_name(orguser),
        )


# =============================================================================
# Helpers
# =============================================================================


def _get_user_name(orguser) -> Optional[str]:
    """Get display name from OrgUser"""
    user = orguser.user
    parts = [user.first_name, user.last_name]
    name = " ".join(p for p in parts if p).strip()
    return name or None

from ninja import Schema
from pydantic import Field


class UpdateOrganizationSchema(Schema):
    """Schema for updating organization details."""

    name: str = Field(..., min_length=1)
    slug: str = Field(..., min_length=1)
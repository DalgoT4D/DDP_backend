from ninja import Schema
from pydantic import Field, field_validator


class UpdateOrganizationSchema(Schema):
    """Schema for updating organization details."""

    name: str = Field(..., min_length=1)
    slug: str = Field(..., min_length=1)

    @field_validator("name", "slug")
    @classmethod
    def must_not_be_blank(cls, value: str) -> str:
        """Ensure fields are not blank or whitespace-only."""
        value = value.strip()

        if not value:
            raise ValueError("Field cannot be blank")

        return value

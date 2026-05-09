from ninja import Schema


class UpdateOrganizationSchema(Schema):
    """Schema for updating organization details."""

    name: str
    slug: str

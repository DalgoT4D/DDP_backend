"""Groups service exceptions — raised by ``user_groups_service``, mapped to
HTTP status codes at the API layer (``groups_api``)."""


class UserGroupError(Exception):
    """Base exception for Groups errors."""

    def __init__(self, message: str):
        self.message = message
        super().__init__(self.message)


class GroupNotFoundError(UserGroupError):
    """Group id doesn't exist for this org — maps to 404."""


class GroupValidationError(UserGroupError):
    """Invalid group request (e.g. blank name) — maps to 400."""


class GroupNameCollisionError(UserGroupError):
    """Another group in this org already has this name — maps to 400."""


class GroupPermissionError(UserGroupError):
    """Caller is neither the group's creator nor an Admin — maps to 403."""


class MemberNotFoundError(UserGroupError):
    """OrgUser to add isn't in this org, or the membership row doesn't
    belong to this group — maps to 404."""

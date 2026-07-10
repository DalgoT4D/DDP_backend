"""Resource Sharing exceptions — raised by ``sharing_actions``, mapped to
HTTP status codes at the API layer (``access_api``)."""


class SharingError(Exception):
    """Base exception for Resource Sharing errors."""

    def __init__(self, message: str):
        self.message = message
        super().__init__(self.message)


class SharingValidationError(SharingError):
    """Invalid sharing request (unsupported capability, bad principal/level,
    re-share above the grantor's own level, ...) — maps to 400."""


class GrantNotFoundError(SharingError):
    """Grant id doesn't exist for this org + resource — maps to 404."""


class PrincipalNotFoundError(SharingError):
    """Grant target isn't an OrgUser of this org — maps to 404."""

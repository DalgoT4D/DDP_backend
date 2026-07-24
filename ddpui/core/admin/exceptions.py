"""Admin Portal service exceptions.

Mirrors the canonical shape in ddpui/core/alerts/exceptions.py: a base carrying a
message + error_code, with typed subclasses the API layer maps to HTTP status codes.
Using classes instead of (value, error_string) tuples is what lets the API pick the
status code from the exception TYPE rather than by comparing the error message string
(the fragile pattern PR #44f76ffa removed from put_admin_org_user_role).
"""


class AdminServiceError(Exception):
    """Base admin-service error carrying a message + error_code (mapped to HTTP by the API)."""

    def __init__(self, message: str, error_code: str = "ADMIN_ERROR"):
        self.message = message
        self.error_code = error_code
        super().__init__(self.message)


class AdminInvalidCredentialsError(AdminServiceError):
    """wrong username/password — maps to 401"""

    def __init__(self, message: str = "invalid credentials"):
        super().__init__(message, "ADMIN_INVALID_CREDENTIALS")


class AdminNotPlatformAdminError(AdminServiceError):
    """valid credentials but the user is not a platform admin — maps to 403"""

    def __init__(self, message: str = "not a platform admin"):
        super().__init__(message, "ADMIN_NOT_PLATFORM_ADMIN")


class AdminSessionError(AdminServiceError):
    """an admin refresh token that is unreadable, not an admin session, or blacklisted — 401"""

    def __init__(self, message: str):
        super().__init__(message, "ADMIN_SESSION_ERROR")


class AdminOrgCreateError(AdminServiceError):
    """org creation failed (Airbyte or plan step) — maps to 400"""

    def __init__(self, message: str):
        super().__init__(message, "ADMIN_ORG_CREATE_ERROR")

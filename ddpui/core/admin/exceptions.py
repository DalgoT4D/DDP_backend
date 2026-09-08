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


class AdminOrgCreateError(AdminServiceError):
    """org creation failed (Airbyte or plan step) — maps to 400"""

    def __init__(self, message: str):
        super().__init__(message, "ADMIN_ORG_CREATE_ERROR")


class AdminOrgDeleteError(AdminServiceError):
    """org deletion failed (OrgCleanupService raised) — maps to 400"""

    def __init__(self, message: str):
        super().__init__(message, "ADMIN_ORG_DELETE_ERROR")

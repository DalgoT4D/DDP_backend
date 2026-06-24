"""Org logo exceptions"""


class OrgLogoError(Exception):
    """Base exception for org logo errors"""

    def __init__(self, message: str, error_code: str = "ORG_LOGO_ERROR"):
        self.message = message
        self.error_code = error_code
        super().__init__(self.message)


class OrgLogoNotFoundError(OrgLogoError):
    """Raised when no logo exists for the org"""

    def __init__(self):
        super().__init__("No logo found for this organization", "ORG_LOGO_NOT_FOUND")


class OrgLogoValidationError(OrgLogoError):
    """Raised when file type or size validation fails"""

    def __init__(self, message: str):
        super().__init__(message, "ORG_LOGO_VALIDATION_ERROR")


class OrgLogoS3Error(OrgLogoError):
    """Raised when an S3 upload or delete operation fails"""

    def __init__(self, message: str):
        super().__init__(message, "ORG_LOGO_S3_ERROR")


class OrgLogoFetchError(OrgLogoError):
    """Raised when fetching logo bytes from org.logo_url via HTTP fails"""

    def __init__(self, message: str):
        super().__init__(message, "ORG_LOGO_FETCH_ERROR")

"""Report exceptions"""


class ReportError(Exception):
    """Base exception for report errors"""

    def __init__(self, message: str, error_code: str = "REPORT_ERROR"):
        self.message = message
        self.error_code = error_code
        super().__init__(self.message)


class SnapshotNotFoundError(ReportError):
    """Raised when snapshot is not found"""

    def __init__(self, snapshot_id: int):
        super().__init__(f"Snapshot with id {snapshot_id} not found", "SNAPSHOT_NOT_FOUND")


class SnapshotValidationError(ReportError):
    """Raised when snapshot validation fails"""

    def __init__(self, message: str):
        super().__init__(message, "SNAPSHOT_VALIDATION_ERROR")


class SnapshotPermissionError(ReportError):
    """Raised when user doesn't have permission for snapshot operation"""

    def __init__(self, message: str = "Permission denied"):
        super().__init__(message, "SNAPSHOT_PERMISSION_DENIED")


class SnapshotExternalServiceError(ReportError):
    """Raised when external service (warehouse, etc.) call fails"""

    def __init__(self, service: str, message: str):
        super().__init__(
            f"{service} error: {message}",
            "SNAPSHOT_EXTERNAL_ERROR",
        )
        self.service = service


# --- Comment exceptions ---


class CommentError(Exception):
    """Base exception for comment errors"""

    def __init__(self, message: str, error_code: str = "COMMENT_ERROR"):
        self.message = message
        self.error_code = error_code
        super().__init__(self.message)


class CommentNotFoundError(CommentError):
    """Raised when comment is not found"""

    def __init__(self, comment_id: int):
        super().__init__(f"Comment with id {comment_id} not found", "COMMENT_NOT_FOUND")
        self.comment_id = comment_id


class CommentValidationError(CommentError):
    """Raised when comment validation fails"""

    def __init__(self, message: str):
        super().__init__(message, "COMMENT_VALIDATION_ERROR")


class CommentPermissionError(CommentError):
    """Raised when user doesn't have permission"""

    def __init__(self, message: str = "Permission denied"):
        super().__init__(message, "COMMENT_PERMISSION_DENIED")

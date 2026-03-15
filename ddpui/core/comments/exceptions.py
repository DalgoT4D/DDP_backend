"""Comment exceptions"""


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

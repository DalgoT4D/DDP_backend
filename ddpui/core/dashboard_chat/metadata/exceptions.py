"""Dashboard chat metadata exceptions."""


class DashboardChatPIIColumnNotFoundError(Exception):
    """Raised when a reviewed PII column is not present in ready metadata."""

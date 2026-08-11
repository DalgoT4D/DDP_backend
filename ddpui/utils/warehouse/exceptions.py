"""Custom exceptions for warehouse operations."""


class TableNotFoundError(Exception):
    """Raised when a requested table or relation does not exist in the warehouse."""
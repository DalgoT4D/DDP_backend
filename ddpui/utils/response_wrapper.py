from typing import TypeVar, Generic, Optional, Any
from ninja import Schema


T = TypeVar("T")


class ApiResponse(Schema, Generic[T]):
    """Standard API response wrapper"""

    success: bool
    message: Optional[str] = None
    data: Optional[T] = None


def api_response(
    success: bool,
    data: Any = None,
    message: Optional[str] = None,
) -> dict:
    """Create a standardized API response.

    Args:
        success: Whether the operation was successful
        data: Response data (can be schema instance or dict)
        message: Optional message

    Returns:
        Dict with standard response structure
    """
    response = {"success": success}

    if message is not None:
        response["message"] = message

    if data is not None:
        if hasattr(data, "dict"):
            response["data"] = data.dict()
        else:
            response["data"] = data

    return response

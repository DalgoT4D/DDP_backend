"""Error handling utilities for chart APIs"""
from functools import wraps
import logging
from ninja.errors import HttpError
from django.db import IntegrityError, DatabaseError
from sqlalchemy.exc import SQLAlchemyError

logger = logging.getLogger(__name__)


def handle_chart_errors(func):
    """Decorator to handle common chart API errors"""

    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except HttpError:
            # Re-raise HTTP errors as-is
            raise
        except IntegrityError as e:
            logger.error(f"Database integrity error in {func.__name__}: {str(e)}", exc_info=True)
            if "duplicate key" in str(e).lower():
                raise HttpError(409, "A chart with this configuration already exists")
            raise HttpError(400, "Invalid data provided")
        except DatabaseError as e:
            logger.error(f"Database error in {func.__name__}: {str(e)}", exc_info=True)
            raise HttpError(500, "Database error occurred")
        except SQLAlchemyError as e:
            logger.error(f"SQLAlchemy error in {func.__name__}: {str(e)}", exc_info=True)
            raise HttpError(500, "Query execution failed")
        except ValueError as e:
            logger.error(f"Value error in {func.__name__}: {str(e)}", exc_info=True)
            raise HttpError(400, str(e))
        except Exception as e:
            logger.error(f"Unexpected error in {func.__name__}: {str(e)}", exc_info=True)
            raise HttpError(500, f"An unexpected error occurred in {func.__name__}")

    return wrapper


def validate_query_results(results, max_rows=10000):
    """Validate query results before processing"""
    if not isinstance(results, list):
        raise ValueError("Query results must be a list")

    if len(results) > max_rows:
        raise HttpError(
            400, f"Query returned too many rows ({len(results)}). Maximum allowed is {max_rows}"
        )

    return results


def sanitize_sql_identifier(identifier: str) -> str:
    """Sanitize SQL identifiers to prevent injection"""
    # Remove any quotes and escape special characters
    sanitized = identifier.replace('"', "").replace("'", "").replace(";", "").replace("--", "")

    # Wrap in double quotes for PostgreSQL
    return f'"{sanitized}"'

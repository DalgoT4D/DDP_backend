"""
Unified logging solution for Dalgo with structured JSON output,
correlation tracking, and observability platform compatibility.
"""

import json
import logging
import traceback
import uuid
import inspect
import threading
import os
import sys
from datetime import datetime, timezone
from typing import Optional, Dict, Any
from django.conf import settings
from logging.handlers import RotatingFileHandler

# Global configuration for stack inspection
MAX_STACK_DEPTH = 10  # Maximum frames to traverse when inspecting the call stack


class StructuredFormatter(logging.Formatter):
    """JSON formatter for structured logging"""

    def format(self, record):
        """Format log record as structured JSON"""

        # Base log structure - use caller info from extra if available
        log_entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": getattr(record, "caller_module", record.module),
            "function": getattr(record, "caller_function", record.funcName),
            "file": getattr(record, "caller_file", record.filename),
            "line": getattr(record, "caller_line", record.lineno),
            "thread": threading.current_thread().name,
        }

        # Add correlation context from auto-detected values
        if org_slug := getattr(record, "org_slug", None):
            log_entry["org_slug"] = org_slug
        if user_id := getattr(record, "user_id", None):
            log_entry["user_id"] = user_id
        if request_id := getattr(record, "request_id", None):
            log_entry["request_id"] = request_id
        if inspect_stack_depth := getattr(record, "inspect_stack_depth", None):
            log_entry["inspect_stack_depth"] = inspect_stack_depth

        # Add exception info with full stack trace
        if record.exc_info:
            log_entry["exception"] = {
                "type": record.exc_info[0].__name__ if record.exc_info[0] else None,
                "message": str(record.exc_info[1]) if record.exc_info[1] else None,
                "stack_trace": traceback.format_exception(*record.exc_info),
            }

        return json.dumps(log_entry, default=str, ensure_ascii=False)


class MaxLevelFilter(logging.Filter):
    """Allow log records with levelno <= configured max_level (int or name).

    Intended for routing lower-severity records (e.g. DEBUG/INFO/WARNING)
    to a specific handler (like stdout).
    """

    def __init__(self, max_level=logging.WARNING):
        super().__init__()
        # accept int or named level
        if isinstance(max_level, str):
            try:
                self.max_level = int(logging.getLevelName(max_level))
            except Exception:
                self.max_level = logging.WARNING
        elif isinstance(max_level, int):
            self.max_level = max_level
        else:
            self.max_level = logging.WARNING

    def filter(self, record: logging.LogRecord) -> bool:
        return record.levelno <= self.max_level


class DalgoLogger:
    """Unified logger for Dalgo with context awareness and structured output."""

    def __init__(self, name: str, base_level: int = None):
        # Determine default level from Django settings.DEBUG if not provided
        if base_level is None:
            try:
                if getattr(settings, "DEBUG", False):
                    base_level = logging.DEBUG
                else:
                    base_level = logging.INFO
            except Exception:
                base_level = logging.INFO

        self.name = name
        self.base_level = base_level
        self.logger = logging.getLogger(name)
        self.logger.setLevel(self.base_level)

        # Ensure we don't add duplicate handlers
        if not self.logger.handlers:
            self._setup_handlers()

    def _setup_handlers(self):
        """Setup console and file handlers with structured formatting"""
        formatter = StructuredFormatter()

        # stdout handler: logs at configured base level up to and including WARNING
        stdout_handler = logging.StreamHandler(sys.stdout)
        stdout_handler.setLevel(self.base_level)

        stdout_handler.addFilter(MaxLevelFilter(logging.WARNING))
        stdout_handler.setFormatter(formatter)
        self.logger.addHandler(stdout_handler)

        # stderr handler: errors and above
        stderr_handler = logging.StreamHandler(sys.stderr)
        stderr_handler.setLevel(logging.ERROR)
        stderr_handler.setFormatter(formatter)
        self.logger.addHandler(stderr_handler)

        # Prevent propagation to root logger (avoid duplicate logs)
        self.logger.propagate = False

        # File handler - controlled by ENABLE_FILE_LOGGING env var
        file_logging_enabled = os.getenv("ENABLE_FILE_LOGGING", "False") == "True"

        if file_logging_enabled and hasattr(settings, "BASE_DIR"):
            log_dir = settings.BASE_DIR / "ddpui/logs"
            log_dir.mkdir(exist_ok=True)

            file_handler = RotatingFileHandler(
                log_dir / f"{self.name.replace('.', '_')}.log",
                maxBytes=10 * 1024 * 1024,  # 10MB
                backupCount=5,
            )
            # Use configured base level for file logging as well
            file_handler.setLevel(self.base_level)
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)

    def _get_caller_info(self) -> Dict[str, str]:
        """Get caller function and auto-detect org context with comprehensive stack analysis"""
        caller_info = {}
        depth = 0  # Initialize depth outside try block

        # Single stack walk to get both caller info and context detection
        try:
            frame = inspect.currentframe()
            current_frame = frame
            caller_found = False

            while current_frame and depth < MAX_STACK_DEPTH:
                current_frame = current_frame.f_back
                depth += 1
                if current_frame and current_frame.f_code:
                    filename = current_frame.f_code.co_filename
                    function_name = current_frame.f_code.co_name

                    # Skip frames from logger files, Sentry patches, and Python logging (same logic as old formatter)
                    skip_conditions = [
                        filename.endswith(("unified_logger.py", "custom_logger.py")),
                        "/logging/" in filename,  # Python logging module
                        "sentry" in filename.lower(),  # Sentry files
                        function_name.startswith("sentry_"),  # Sentry patched functions
                        function_name
                        in ("callHandlers", "_log", "handle", "emit"),  # Logging internals
                        "/site-packages/" in filename
                        and ("logging" in filename or "sentry" in filename.lower()),
                    ]

                    # Capture caller info from first non-logger frame
                    if not caller_found and not any(skip_conditions):
                        caller_info["caller_function"] = function_name
                        caller_info["caller_file"] = filename.split("/")[-1]
                        caller_info["caller_module"] = filename.split("/")[-1].replace(".py", "")
                        caller_info["caller_line"] = current_frame.f_lineno
                        caller_found = True

                    # Auto-detect org_slug separately
                    if not caller_info.get("org_slug"):
                        # Try orguser.org.slug first
                        orguser = current_frame.f_locals.get("orguser")
                        if orguser and hasattr(orguser, "org") and orguser.org:
                            if hasattr(orguser.org, "slug"):
                                caller_info["org_slug"] = orguser.org.slug
                        else:
                            # Try standalone org.slug
                            org = current_frame.f_locals.get("org")
                            if org and hasattr(org, "slug"):
                                caller_info["org_slug"] = org.slug

                    # Auto-detect user_id separately
                    if not caller_info.get("user_id"):
                        # Try orguser.user.id first
                        orguser = current_frame.f_locals.get("orguser")
                        if orguser and hasattr(orguser, "user") and orguser.user:
                            if hasattr(orguser.user, "id"):
                                caller_info["user_id"] = orguser.user.id
                        else:
                            # Try standalone user.id
                            user = current_frame.f_locals.get("user")
                            if user and hasattr(user, "id"):
                                caller_info["user_id"] = user.id

                    # Stop if we have everything we need
                    if caller_found and caller_info.get("org_slug") and caller_info.get("user_id"):
                        break

        except Exception:
            pass

        # Add the depth we reached for debugging/monitoring
        caller_info["inspect_stack_depth"] = depth

        return caller_info

    def _log(self, level: int, message: str, *args, **kwargs):
        """Internal logging with context enrichment"""
        extra = kwargs.pop("extra", {})
        extra.update(self._get_caller_info())

        # Handle exception logging
        if level == logging.ERROR and "exc_info" not in kwargs:
            kwargs["exc_info"] = True

        self.logger.log(level, message, *args, extra=extra, **kwargs)

    def debug(self, message: str, *args, **kwargs):
        """Log debug message with context"""
        self._log(logging.DEBUG, message, *args, **kwargs)

    def info(self, message: str, *args, **kwargs):
        """Log info message with context"""
        self._log(logging.INFO, message, *args, **kwargs)

    def warning(self, message: str, *args, **kwargs):
        """Log warning message with context"""
        self._log(logging.WARNING, message, *args, **kwargs)

    def error(self, message: str, *args, **kwargs):
        """Log error message with context and stack trace"""
        self._log(logging.ERROR, message, *args, **kwargs)

    def exception(self, message: str, *args, **kwargs):
        """Log exception with full stack trace"""
        kwargs["exc_info"] = True
        self._log(logging.ERROR, message, *args, **kwargs)

    def critical(self, message: str, *args, **kwargs):
        """Log critical message with context"""
        self._log(logging.CRITICAL, message, *args, **kwargs)

    def get_slug(self) -> str:
        """Get the current organization slug from stack frames"""
        try:
            stack = inspect.stack()
            for frame_info in stack:
                frame = frame_info.frame
                # Check for orguser object
                if orguser := frame.f_locals.get("orguser"):
                    if hasattr(orguser, "org") and hasattr(orguser.org, "slug"):
                        return orguser.org.slug
                # Check for standalone org object
                elif org := frame.f_locals.get("org"):
                    if hasattr(org, "slug"):
                        return org.slug
        except Exception:
            pass

        # Default to empty string
        return ""


def get_logger() -> DalgoLogger:
    """Get a Dalgo logger instance"""
    return DalgoLogger("ddp")

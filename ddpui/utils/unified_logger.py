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
from contextvars import ContextVar
from django.conf import settings
from logging.handlers import RotatingFileHandler

# Context variables for request correlation
request_id_context: ContextVar[Optional[str]] = ContextVar("request_id", default=None)
org_slug_context: ContextVar[Optional[str]] = ContextVar("org_slug", default=None)
user_id_context: ContextVar[Optional[str]] = ContextVar("user_id", default=None)


class StructuredFormatter(logging.Formatter):
    """JSON formatter for structured logging"""

    def format(self, record):
        """Format log record as structured JSON"""

        # Get the actual caller info (not the logger itself)
        actual_caller = self._get_actual_caller()

        # Base log structure
        log_entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": actual_caller.get("module", record.module),
            "function": actual_caller.get("function", record.funcName),
            "file": actual_caller.get("file", record.filename),
            "line": actual_caller.get("line", record.lineno),
            "thread": threading.current_thread().name,
        }

        # Add correlation context
        if request_id := request_id_context.get(None):
            log_entry["request_id"] = request_id
        if org_slug := org_slug_context.get(None):
            log_entry["org_slug"] = org_slug
        if user_id := user_id_context.get(None):
            log_entry["user_id"] = user_id

        # Add exception info with full stack trace
        if record.exc_info:
            log_entry["exception"] = {
                "type": record.exc_info[0].__name__ if record.exc_info[0] else None,
                "message": str(record.exc_info[1]) if record.exc_info[1] else None,
                "stack_trace": traceback.format_exception(*record.exc_info),
            }

        # Add any extra fields from the record
        for key, value in record.__dict__.items():
            if key not in {
                "name",
                "msg",
                "args",
                "levelname",
                "levelno",
                "pathname",
                "filename",
                "module",
                "funcName",
                "lineno",
                "exc_info",
                "exc_text",
                "stack_info",
                "created",
                "msecs",
                "relativeCreated",
                "thread",
                "threadName",
                "processName",
                "process",
                "message",
            } and not key.startswith("_"):
                log_entry["extra"] = log_entry.get("extra", {})
                log_entry["extra"][key] = value

        return json.dumps(log_entry, default=str, ensure_ascii=False)

    def _get_actual_caller(self):
        """Get the actual caller info, skipping logger and framework files"""
        try:
            frame = inspect.currentframe()
            # Walk up the stack to find the first frame outside logger/framework files
            current_frame = frame
            while current_frame:
                current_frame = current_frame.f_back
                if current_frame and current_frame.f_code:
                    filename = current_frame.f_code.co_filename
                    function_name = current_frame.f_code.co_name

                    # Skip frames from logger files, Sentry patches, and Python logging
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

                    if not any(skip_conditions):
                        return {
                            "function": function_name,
                            "file": filename.split("/")[-1],
                            "module": filename.split("/")[-1].replace(".py", ""),
                            "line": current_frame.f_lineno,
                        }
        except Exception:
            pass
        return {}


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
        """Get caller function and auto-detect org context"""
        caller_info = {}

        # Get the actual caller (skip through logger methods)
        try:
            frame = inspect.currentframe()
            # Walk up the stack to find the first frame outside unified_logger.py
            current_frame = frame
            while current_frame:
                current_frame = current_frame.f_back
                if current_frame and current_frame.f_code:
                    filename = current_frame.f_code.co_filename
                    # Skip frames from unified_logger.py and custom_logger.py
                    if not filename.endswith(("unified_logger.py", "custom_logger.py")):
                        caller_info["caller_function"] = current_frame.f_code.co_name
                        caller_info["caller_file"] = filename.split("/")[-1]
                        break
        except Exception:
            pass

        # Auto-detect org and user from frame locals if not in context
        if not org_slug_context.get(None) or not user_id_context.get(None):
            try:
                stack = inspect.stack()
                for frame_info in stack:
                    frame = frame_info.frame
                    if orguser := frame.f_locals.get("orguser"):
                        if orguser.org and not org_slug_context.get(None):
                            caller_info["auto_detected_org"] = orguser.org.slug
                        if orguser.user and not user_id_context.get(None):
                            caller_info["auto_detected_user"] = str(orguser.user.id)
                        break
                    elif org := frame.f_locals.get("org"):
                        if hasattr(org, "slug") and not org_slug_context.get(None):
                            caller_info["auto_detected_org"] = org.slug

                    # Also check for standalone user objects
                    if not user_id_context.get(None):
                        if user := frame.f_locals.get("user"):
                            if hasattr(user, "id"):
                                caller_info["auto_detected_user"] = str(user.id)
            except Exception:
                pass

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
        """Get the current organization slug"""
        # First try to get from context
        context_slug = org_slug_context.get(None)
        if context_slug:
            return context_slug

        # Try to auto-detect from stack frames (similar to _get_caller_info)
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


# Context managers for setting correlation context
class LogContext:
    """Context manager for setting logging context"""

    def __init__(self, request_id: str = None, org_slug: str = None, user_id: str = None):
        self.request_id = request_id or str(uuid.uuid4())
        self.org_slug = org_slug
        self.user_id = user_id
        self.tokens = {}

    def __enter__(self):
        self.tokens["request_id"] = request_id_context.set(self.request_id)
        if self.org_slug:
            self.tokens["org_slug"] = org_slug_context.set(self.org_slug)
        if self.user_id:
            self.tokens["user_id"] = user_id_context.set(self.user_id)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        for token in self.tokens.values():
            try:
                token.var.reset(token)
            except LookupError:
                pass


def get_logger() -> DalgoLogger:
    """Get a Dalgo logger instance"""
    return DalgoLogger("ddp")

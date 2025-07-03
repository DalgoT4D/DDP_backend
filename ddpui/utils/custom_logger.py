import inspect
import logging
from opentelemetry import trace


class CustomLogger:
    """override the python logger to include the orgname associated with every request"""

    def __init__(self, name, level=logging.INFO):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(level)

    def get_slug(self):
        """retrieve the org.slug from the request"""
        try:
            stack = inspect.stack()
            for frame_info in stack:
                frame = frame_info.frame
                userorg = frame.f_locals.get("orguser")
                if userorg is not None and userorg.org is not None:
                    return userorg.org.slug
        except Exception as error:
            self.logger.error("An error occurred while getting slug: %s", str(error))
        return ""

    def get_trace_context(self):
        """Get current trace and span context for log correlation"""
        try:
            current_span = trace.get_current_span()
            if current_span and current_span.is_recording():
                span_context = current_span.get_span_context()
                return {
                    "trace_id": f"{span_context.trace_id:032x}",
                    "span_id": f"{span_context.span_id:016x}",
                    "trace_flags": span_context.trace_flags,
                }
        except Exception as error:
            # Don't log this error to avoid infinite recursion
            pass
        return {}

    def info(self, *args):
        """call logger.info with the caller_name and the orgname"""
        slug = self.get_slug()
        caller_name = inspect.stack()[1].function
        trace_context = self.get_trace_context()
        extra = {"caller_name": caller_name, "orgname": slug, **trace_context}
        self.logger.info(*args, extra=extra)

    def error(self, *args):
        """call logger.error with the caller_name and the orgname"""
        slug = self.get_slug()
        caller_name = inspect.stack()[1].function
        trace_context = self.get_trace_context()
        extra = {"caller_name": caller_name, "orgname": slug, **trace_context}
        self.logger.error(*args, extra=extra)

    def debug(self, *args):
        """call logger.debug with the caller_name and the orgname"""
        slug = self.get_slug()
        caller_name = inspect.stack()[1].function
        trace_context = self.get_trace_context()
        extra = {"caller_name": caller_name, "orgname": slug, **trace_context}
        self.logger.debug(*args, extra=extra)

    def exception(self, *args):
        """call logger.exception with the caller_name and the orgname"""
        slug = self.get_slug()
        caller_name = inspect.stack()[1].function
        trace_context = self.get_trace_context()
        extra = {"caller_name": caller_name, "orgname": slug, **trace_context}
        self.logger.exception(*args, extra=extra)

    def warning(self, *args):
        """call logger.warning with the caller_name and the orgname"""
        slug = self.get_slug()
        caller_name = inspect.stack()[1].function
        trace_context = self.get_trace_context()
        extra = {"caller_name": caller_name, "orgname": slug, **trace_context}
        self.logger.warning(*args, extra=extra)

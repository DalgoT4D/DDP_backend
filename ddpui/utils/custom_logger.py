import inspect
import logging


class CustomLogger:
    """custom logger to get org_slug from inspect.stack"""

    def __init__(self, name, level=logging.INFO):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(level)

    def get_slug(self):
        """custom logger for slug"""
        try:
            stack = inspect.stack()
            for frame_info in stack:
                frame = frame_info.frame
                userorg = frame.f_locals.get("orguser")
                if userorg is not None and userorg.org is not None:
                    return userorg.org.slug
        except Exception as e:
            self.logger.error(f"An error occurred while getting slug: {e}")
        return None

    def info(self, msg):
        """custom logger for info"""
        slug = self.get_slug()
        caller_name = inspect.stack()[1].function
        if slug:
            self.logger.info(msg, extra={"caller_name": caller_name, "orgname": slug})
        else:
            self.logger.info(msg, extra={"caller_name": caller_name})

    def error(self, msg):
        """custom logger for error"""
        slug = self.get_slug()
        caller_name = inspect.stack()[1].function
        if slug:
            self.logger.error(msg, extra={"caller_name": caller_name, "orgname": slug})
        else:
            self.logger.error(msg, extra={"caller_name": caller_name})

    def debug(self, msg):
        """custom logger for debug"""
        slug = self.get_slug()
        caller_name = inspect.stack()[1].function
        if slug:
            self.logger.debug(msg, extra={"caller_name": caller_name, "orgname": slug})
        else:
            self.logger.debug(msg, extra={"caller_name": caller_name})

    def exception(self, msg):
        """custom logger for exc"""
        slug = self.get_slug()
        caller_name = inspect.stack()[1].function
        if slug:
            self.logger.exception(
                msg, extra={"caller_name": caller_name, "orgname": slug}
            )
        else:
            self.logger.exception(msg, extra={"caller_name": caller_name})

    def warning(self, msg):
        """custom logger for warning"""
        slug = self.get_slug()
        caller_name = inspect.stack()[1].function
        if slug:
            self.logger.warning(
                msg, extra={"caller_name": caller_name, "orgname": slug}
            )
        else:
            self.logger.warning(msg, extra={"caller_name": caller_name})

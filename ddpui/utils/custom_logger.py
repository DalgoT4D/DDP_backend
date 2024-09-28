import inspect
import logging


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

    def info(self, *args):
        """call logger.info with the caller_name and the orgname"""
        slug = self.get_slug()
        caller_name = inspect.stack()[1].function
        self.logger.info(*args, extra={"caller_name": caller_name, "orgname": slug})

    def error(self, *args):
        """call logger.error with the caller_name and the orgname"""
        slug = self.get_slug()
        caller_name = inspect.stack()[1].function
        self.logger.error(*args, extra={"caller_name": caller_name, "orgname": slug})

    def debug(self, *args):
        """call logger.debug with the caller_name and the orgname"""
        slug = self.get_slug()
        caller_name = inspect.stack()[1].function
        self.logger.debug(*args, extra={"caller_name": caller_name, "orgname": slug})

    def exception(self, *args):
        """call logger.exception with the caller_name and the orgname"""
        slug = self.get_slug()
        caller_name = inspect.stack()[1].function
        self.logger.exception(*args, extra={"caller_name": caller_name, "orgname": slug})

    def warning(self, *args):
        """call logger.warning with the caller_name and the orgname"""
        slug = self.get_slug()
        caller_name = inspect.stack()[1].function
        self.logger.warning(*args, extra={"caller_name": caller_name, "orgname": slug})

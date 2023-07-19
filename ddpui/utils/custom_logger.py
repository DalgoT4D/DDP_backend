import inspect
import logging


class CustomLogger:
    def __init__(self, name, level=logging.INFO):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(level)

    def get_slug(self):
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
        slug = self.get_slug()
        caller_name = inspect.stack()[1].function
        if slug:
            self.logger.info(msg, extra={"caller_name": caller_name, "orgname": slug})
        else:
            self.logger.info(msg, extra={"caller_name": caller_name})

    def error(self, msg):
        slug = self.get_slug()
        caller_name = inspect.stack()[1].function
        if slug:
            self.logger.error(msg, extra={"caller_name": caller_name, "orgname": slug})
        else:
            self.logger.error(msg, extra={"caller_name": caller_name})

    def debug(self, msg):
        slug = self.get_slug()
        caller_name = inspect.stack()[1].function
        if slug:
            self.logger.debug(msg, extra={"caller_name": caller_name, "orgname": slug})
        else:
            self.logger.debug(msg, extra={"caller_name": caller_name})

    def exception(self, msg):
        slug = self.get_slug()
        caller_name = inspect.stack()[1].function
        if slug:
            self.logger.exception(
                msg, extra={"caller_name": caller_name, "orgname": slug}
            )
        else:
            self.logger.exception(msg, extra={"caller_name": caller_name})

    def warning(self, msg):
        slug = self.get_slug()
        caller_name = inspect.stack()[1].function
        if slug:
            self.logger.warning(
                msg, extra={"caller_name": caller_name, "orgname": slug}
            )
        else:
            self.logger.warning(msg, extra={"caller_name": caller_name})


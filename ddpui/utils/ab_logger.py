import logging
import sys
from logging.handlers import RotatingFileHandler
from ddpui import settings

logger = logging.getLogger("airbyte")


def setup_logger():
    """setup the airbyte api logger"""
    logfilename = settings.BASE_DIR / "ddpui/logs/airbyte.log"
    logger.setLevel(logging.INFO)

    # log to stdout
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        "%(levelname)s - %(asctime)s - %(name)s - %(message)s"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    # log to file
    handler = RotatingFileHandler(logfilename)
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter(
        "%(levelname)s - %(asctime)s - %(name)s - %(message)s"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

import logging
import sys
from logging.handlers import RotatingFileHandler
from ddpui import settings
from ddpui.utils.timezone import ist_time

logger = logging.getLogger("airbyte")


def setup_logger():
    """setup the airbyte api logger"""
    logfilename = settings.BASE_DIR / "ddpui/logs/airbyte.log"
    logger.setLevel(logging.INFO)

    # log to stdout
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)
    logging.Formatter.converter = ist_time
    formatter = logging.Formatter(
        "%(levelname)s - %(asctime)s - %(name)s - %(filename)s - %(caller_name)s - %(orgname)s: %(message)s"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    # log to file
    handler = RotatingFileHandler(logfilename, maxBytes=1048576, backupCount=5)
    handler.setLevel(logging.INFO)
    logging.Formatter.converter = ist_time
    formatter = logging.Formatter(
        "%(levelname)s - %(asctime)s - %(name)s - %(filename)s - %(caller_name)s - %(orgname)s: %(message)s"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

import logging
import sys
from logging.handlers import RotatingFileHandler
from ddpui import settings

logger = logging.getLogger("ddpui")


def setup_logger():
    """setup the ddpui logger"""
    logfilename = settings.BASE_DIR / "ddpui/logs/ddpui.log"
    logger.setLevel(logging.INFO)

    # log to stdout
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        "%(levelname)s - %(asctime)s - %(name)s - %(message)s"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    handler = RotatingFileHandler(logfilename, maxBytes=1048576, backupCount=5)
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter(
        "%(levelname)s - %(asctime)s - %(name)s - %(message)s"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

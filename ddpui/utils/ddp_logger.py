import logging
import sys
from logging.handlers import RotatingFileHandler
from ddpui import settings
import pytz
import datetime

logger = logging.getLogger("ddpui")
IST = pytz.timezone("Asia/Kolkata")


def ist_time(*args):
    utc_dt = pytz.utc.localize(datetime.datetime.utcnow())
    converted = utc_dt.astimezone(IST)
    return converted.timetuple()


def setup_logger():
    """setup the ddpui logger"""
    logfilename = settings.BASE_DIR / "ddpui/logs/ddpui.log"
    logger.setLevel(logging.INFO)

    # log to stdout
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)
    logging.Formatter.converter = ist_time
    formatter = logging.Formatter(
        "%(levelname)s - %(asctime)s - %(name)s - %(message)s"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    handler = RotatingFileHandler(logfilename, maxBytes=1048576, backupCount=5)
    handler.setLevel(logging.INFO)
    logging.Formatter.converter = ist_time
    formatter = logging.Formatter(
        "%(levelname)s - %(asctime)s - %(name)s - %(message)s"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

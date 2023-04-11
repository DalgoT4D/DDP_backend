import logging
import sys
from logging.handlers import RotatingFileHandler
from ddpui import settings

LOGFILE = settings.BASE_DIR / "ddpui/logs/airbyte.log"
logger = logging.getLogger("airbyte")
logger.setLevel(logging.INFO)

# log to stdout
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(levelname)s - %(asctime)s - %(name)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

# log to file
handler = RotatingFileHandler(LOGFILE)
handler.setLevel(logging.INFO)
formatter = logging.Formatter("%(levelname)s - %(asctime)s - %(name)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

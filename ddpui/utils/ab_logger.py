import logging
import sys
from logging.handlers import RotatingFileHandler

logger = logging.getLogger("airbyte")
logger.setLevel(logging.INFO)

# log to stdout
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(levelname)s - %(asctime)s - %(name)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

# log to file
handler = RotatingFileHandler("airbyte.log", maxBytes=1000000, backupCount=5)
handler.setLevel(logging.INFO)
formatter = logging.Formatter("%(levelname)s - %(asctime)s - %(name)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

import logging
import sys
from logging.handlers import RotatingFileHandler
from ddpui import settings

logger = logging.getLogger("ddpui")
logger.setLevel(logging.INFO)
# log to aws cw - requires aws credentials

# from cloudwatch import cloudwatch
# from datetime import datetime
# handler = cloudwatch.CloudwatchHandler(log_group=f"ddpui.{datetime.today().strftime('%Y-%m-%d')}")
# handler.setLevel(logging.INFO)
# formatter = logging.Formatter('%(asctime)s : %(levelname)s - %(message)s')
# handler.setFormatter(formatter)
# logger.addHandler(handler)

# log to stdout
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(levelname)s - %(asctime)s - %(name)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)


handler = RotatingFileHandler(settings.LOGFILE)
handler.setLevel(logging.INFO)
formatter = logging.Formatter("%(levelname)s - %(asctime)s - %(name)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

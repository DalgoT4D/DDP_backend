import logging
from ddpui.utils.unified_logger import DalgoLogger


class CustomLogger(DalgoLogger):
    """Backward compatible CustomLogger that now uses unified logging"""

    def __init__(self, name, level=logging.INFO):
        super().__init__(name, level)

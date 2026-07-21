import time
from contextlib import contextmanager

from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui.core.trial.timing")


@contextmanager
def step_timer(run, step_name: str):
    """time a clone step; record elapsed seconds into run.timings (even on error)

    `run` is any plain object exposing a `.timings` (dict) attribute — no DB writes here, it's
    an in-memory carrier for a single clone run.
    """
    start = time.perf_counter()
    try:
        yield
    finally:
        elapsed = round(time.perf_counter() - start, 3)
        run.timings[step_name] = elapsed
        logger.info(f"clone step '{step_name}' took {elapsed}s")

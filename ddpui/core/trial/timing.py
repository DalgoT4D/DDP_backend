import time
from contextlib import contextmanager

from ddpui.models.trial_clone import TrialClone
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui.core.trial.timing")


@contextmanager
def step_timer(trialclone: TrialClone, step_name: str):
    """time a clone step; record elapsed seconds into TrialClone.timings (even on error)"""
    trialclone.current_step = step_name
    trialclone.save(update_fields=["current_step", "updated_at"])
    start = time.perf_counter()
    try:
        yield
    finally:
        elapsed = round(time.perf_counter() - start, 3)
        trialclone.timings[step_name] = elapsed
        trialclone.save(update_fields=["timings", "updated_at"])
        logger.info(f"clone step '{step_name}' took {elapsed}s")

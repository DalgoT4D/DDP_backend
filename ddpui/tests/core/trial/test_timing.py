import pytest
from ddpui.core.trial.clone_service import CloneRun
from ddpui.core.trial.timing import step_timer


def _run():
    # `.template`/`.trial_email` aren't touched by step_timer — a bare CloneRun with
    # placeholder values is enough to exercise the timings carrier.
    return CloneRun(template=None, trial_email="a@b.org")


def test_step_timer_records_elapsed_and_step():
    run = _run()
    with step_timer(run, "mystep"):
        pass
    assert "mystep" in run.timings
    assert run.timings["mystep"] >= 0


def test_step_timer_records_even_on_exception():
    run = _run()
    with pytest.raises(ValueError):
        with step_timer(run, "boom"):
            raise ValueError("x")
    assert "boom" in run.timings

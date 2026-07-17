import pytest
from ddpui.models.org import Org
from ddpui.models.trial_clone import TrialClone
from ddpui.core.trial.timing import step_timer

pytestmark = pytest.mark.django_db


def test_step_timer_records_elapsed_and_step():
    org = Org.objects.create(name="t", slug="t")
    tc = TrialClone.objects.create(template_org=org, trial_email="a@b.org")
    with step_timer(tc, "mystep"):
        pass
    tc.refresh_from_db()
    assert "mystep" in tc.timings
    assert tc.timings["mystep"] >= 0
    assert tc.current_step == "mystep"


def test_step_timer_records_even_on_exception():
    org = Org.objects.create(name="t2", slug="t2")
    tc = TrialClone.objects.create(template_org=org, trial_email="a@b.org")
    with pytest.raises(ValueError):
        with step_timer(tc, "boom"):
            raise ValueError("x")
    tc.refresh_from_db()
    assert "boom" in tc.timings

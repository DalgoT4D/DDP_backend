import pytest
from ddpui.models.org import Org
from ddpui.models.trial_clone import TrialClone, TrialCloneStatus

pytestmark = pytest.mark.django_db


def test_trialclone_defaults():
    template = Org.objects.create(name="tmpl", slug="tmpl")
    tc = TrialClone.objects.create(template_org=template, trial_email="a@b.org")
    assert tc.status == TrialCloneStatus.PENDING.value
    assert tc.timings == {}
    assert tc.manifest == {}
    assert tc.trial_org is None
    assert tc.current_step is None


def test_trialclone_status_choices():
    values = [v for v, _ in TrialCloneStatus.choices()]
    assert values == ["pending", "running", "completed", "failed"]


def test_trialclone_records_timings_json():
    template = Org.objects.create(name="tmpl2", slug="tmpl2")
    tc = TrialClone.objects.create(template_org=template, trial_email="a@b.org")
    tc.timings["step1"] = 1.23
    tc.save()
    tc.refresh_from_db()
    assert tc.timings == {"step1": 1.23}

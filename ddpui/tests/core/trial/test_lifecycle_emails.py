import re
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest
import pytz

from django.contrib.auth.models import User
from django.test import override_settings
from django.utils import timezone

from ddpui.models.org import Org
from ddpui.models.org_plans import OrgPlans, OrgPlanType
from ddpui.models.org_user import OrgUser
from ddpui.models.userpreferences import UserPreferences
from ddpui.core.trial.lifecycle_emails import (
    decide_email,
    FLAGS_STAMPED_BY,
    EMAIL_DAY3,
    EMAIL_COMPLETION,
    EMAIL_MIDPOINT,
    EMAIL_PRE_END,
    TRACKED_FLOWS,
    completed_flows,
    trial_window,
    run_trial_lifecycle_sweep,
)
from ddpui.utils.email_templates import TRIAL_FLOW_COPY


UTC = pytz.UTC
START = datetime(2026, 8, 1, 9, 0, tzinfo=UTC)


def test_completed_flows_counts_only_completed_entries():
    """a flow counts only when completed is True"""
    assert completed_flows({"insights": {"completed": True, "skipped": False}}) == ["insights"]


def test_completed_flows_ignores_skipped():
    """skipping a walkthrough is not completing it"""
    assert completed_flows({"insights": {"completed": False, "skipped": True}}) == []


def test_completed_flows_ignores_product_tour():
    """product_tour is untracked and never counts, even when completed"""
    walkthrough = {
        "product_tour": {"completed": True, "skipped": False},
        "insights": {"completed": True, "skipped": False},
    }
    assert completed_flows(walkthrough) == ["insights"]


def test_completed_flows_returns_stable_order():
    """order follows TRACKED_FLOWS, not dict insertion order"""
    walkthrough = {
        "automate_pipeline": {"completed": True},
        "insights": {"completed": True},
    }
    assert completed_flows(walkthrough) == ["insights", "automate_pipeline"]


def test_completed_flows_handles_empty_and_malformed():
    """an empty dict, or a non-dict value, counts as nothing completed"""
    assert completed_flows({}) == []
    assert completed_flows({"insights": None}) == []
    assert completed_flows({"insights": "yes"}) == []


def test_trial_window_computes_elapsed_and_total_days():
    """day 3 means 72 hours elapsed; total comes from the plan's own dates"""
    day_number, total_days = trial_window(
        START, START + timedelta(days=14), START + timedelta(days=3)
    )
    assert day_number == 3
    assert total_days == 14


def test_trial_window_day_number_truncates():
    """71 hours in is still day 2 — .days floors"""
    day_number, _ = trial_window(START, START + timedelta(days=14), START + timedelta(hours=71))
    assert day_number == 2


def test_trial_window_respects_a_shorter_admin_set_window():
    """a 7-day window renders as 7, never rounded up to the 14-day default"""
    _, total_days = trial_window(START, START + timedelta(days=7), START + timedelta(days=1))
    assert total_days == 7


def test_trial_window_falls_back_when_window_is_zero():
    """identical dates would divide by zero in the progress bar — fall back to 14"""
    _, total_days = trial_window(START, START, START)
    assert total_days == 14


def test_trial_window_falls_back_when_window_is_inverted():
    """an end before the start is nonsense — fall back rather than render a negative bar"""
    _, total_days = trial_window(START, START - timedelta(days=3), START)
    assert total_days == 14


END = START + timedelta(days=14)


def _decide(day, completed, flags=None, now=None):
    """decide_email with the fixed 14-day window, so cases read as (day, completed, flags)"""
    at = now if now is not None else START + timedelta(days=day)
    return decide_email(day, completed, flags or {}, at, END)


@pytest.mark.parametrize(
    "day,completed,expected",
    [
        (0, 0, None),  # nothing before day 3
        (2, 0, None),
        (2, 2, None),  # C never fires before day 3, even when both are done
        (3, 0, EMAIL_DAY3),  # A
        (3, 1, EMAIL_DAY3),  # B
        (3, 2, EMAIL_COMPLETION),  # C outranks the day-3 email
        (5, 2, EMAIL_COMPLETION),  # C can fire later than day 3
    ],
)
def test_ladder_picks_the_right_email(day, completed, expected):
    assert _decide(day, completed) == expected


def test_completion_beats_day3_on_day_three():
    """with both walkthroughs done on day 3 the user gets C, never B"""
    assert _decide(3, 2) == EMAIL_COMPLETION


def test_day3_never_fires_after_completion():
    """once C has gone out, A and B are locked out forever"""
    flags = {EMAIL_COMPLETION: "2026-08-04T09:00:00+00:00"}
    assert _decide(3, 2, flags) is None
    assert _decide(4, 1, flags) is None


def test_day3_does_not_repeat():
    """the day-3 email is one-shot"""
    flags = {EMAIL_DAY3: "2026-08-04T09:00:00+00:00"}
    assert _decide(3, 0, flags) is None


def test_in_progress_email_is_day_three_only():
    """a user who finishes their first walkthrough on day 6 gets nothing then"""
    flags = {EMAIL_DAY3: "2026-08-04T09:00:00+00:00"}
    assert _decide(6, 1, flags) is None


def test_completion_still_fires_after_the_day3_email():
    """A on day 3 then C on day 9 is the expected two-email path"""
    flags = {EMAIL_DAY3: "2026-08-04T09:00:00+00:00"}
    assert _decide(9, 2, flags) == EMAIL_COMPLETION


def test_midpoint_fires_at_day_seven():
    flags = {EMAIL_DAY3: "x", EMAIL_COMPLETION: "y"}
    assert _decide(7, 2, flags) == EMAIL_MIDPOINT


def test_midpoint_fires_even_after_completion():
    """midpoint and pre-end are unconditional — C does not suppress them"""
    flags = {EMAIL_DAY3: "x", EMAIL_COMPLETION: "y"}
    assert _decide(7, 2, flags) == EMAIL_MIDPOINT
    flags[EMAIL_MIDPOINT] = "z"
    assert _decide(12, 2, flags) == EMAIL_PRE_END


def test_pre_end_fires_two_days_before_the_end():
    flags = {EMAIL_DAY3: "x", EMAIL_MIDPOINT: "y"}
    assert _decide(11, 0, flags) is None
    assert _decide(12, 0, flags) == EMAIL_PRE_END


def test_only_one_email_per_run_when_two_rules_match():
    """a day-7 trial with no day3 flag matches rules 2 and 3 — the earlier rule wins"""
    assert _decide(7, 0) == EMAIL_DAY3


def test_flags_stamped_by_completion_includes_day3():
    """C stamps day3 too, or the next run would fire B on top of the congratulations"""
    assert set(FLAGS_STAMPED_BY[EMAIL_COMPLETION]) == {EMAIL_COMPLETION, EMAIL_DAY3}


def test_flags_stamped_by_other_emails_are_self_only():
    assert FLAGS_STAMPED_BY[EMAIL_DAY3] == (EMAIL_DAY3,)
    assert FLAGS_STAMPED_BY[EMAIL_MIDPOINT] == (EMAIL_MIDPOINT,)
    assert FLAGS_STAMPED_BY[EMAIL_PRE_END] == (EMAIL_PRE_END,)


def test_pre_end_does_not_repeat():
    """rule 4 is the last rule, so nothing downstream masks a missing dedupe guard"""
    flags = {EMAIL_DAY3: "x", EMAIL_MIDPOINT: "y", EMAIL_PRE_END: "z"}
    assert _decide(13, 0, flags) is None


def test_tracked_flows_matches_flow_copy_contract():
    """TRACKED_FLOWS (lifecycle_emails) and TRIAL_FLOW_COPY (email_templates) must name the
    same set of flows — decide_email hands a flow name to send_decided_email, which looks it
    up in TRIAL_FLOW_COPY via TRIAL_FLOW_COPY[flow]; a flow present in one but not the other
    raises KeyError at send time, on a live day-3 email."""
    assert set(TRACKED_FLOWS) == set(TRIAL_FLOW_COPY)


def test_decide_email_normalizes_none_flags():
    """flags param can be None; the function normalizes it to {}"""
    # Call decide_email directly with None to exercise the normalization line.
    # Use day >= 3 so rule 2's `EMAIL_DAY3 not in flags` is actually evaluated with None.
    # Without the normalization, this would raise TypeError: argument of type 'NoneType' is not iterable.
    at = START + timedelta(days=3)
    result = decide_email(3, 0, None, at, END)
    # With no flags, day 3 without completion should return EMAIL_DAY3
    assert result == EMAIL_DAY3


pytestmark = pytest.mark.django_db


def _make_trial(slug, days_ago, completed=(), plan=OrgPlanType.FREE_TRIAL.value, duration=14):
    """a free-trial org whose plan started `days_ago` days ago, with the given flows completed"""
    now = timezone.now()
    org = Org.objects.create(slug=slug, name=slug, airbyte_workspace_id=None)
    OrgPlans.objects.create(
        org=org,
        base_plan=plan,
        start_date=now - timedelta(days=days_ago),
        end_date=now - timedelta(days=days_ago) + timedelta(days=duration),
    )
    user = User.objects.create(username=f"{slug}@x.org", email=f"{slug}@x.org")
    orguser = OrgUser.objects.create(user=user, org=org)
    UserPreferences.objects.create(
        orguser=orguser,
        trial_walkthrough={flow: {"completed": True} for flow in completed},
    )
    return org, orguser


def test_sweep_sends_not_started_email_on_day_three():
    """a day-3 trial with nothing completed gets email A and is flagged"""
    org, orguser = _make_trial("trial-a", days_ago=3)
    with patch("ddpui.core.trial.lifecycle_emails.send_trial_day3_not_started_email") as mock_send:
        assert run_trial_lifecycle_sweep() == 1
        mock_send.assert_called_once()
        assert mock_send.call_args[0][0] == "trial-a@x.org"

    prefs = UserPreferences.objects.get(orguser=orguser)
    assert EMAIL_DAY3 in prefs.trial_emails_sent


def test_sweep_sends_in_progress_email_with_the_completed_flow():
    """one completed flow routes to email B, and the flow name is passed through"""
    _make_trial("trial-b", days_ago=3, completed=("insights",))
    with patch("ddpui.core.trial.lifecycle_emails.send_trial_day3_in_progress_email") as mock_send:
        assert run_trial_lifecycle_sweep() == 1
        assert mock_send.call_args[0][1] == "insights"


def test_sweep_sends_completion_email_and_stamps_both_flags():
    """both flows complete sends C and locks out the day-3 email"""
    _, orguser = _make_trial("trial-c", days_ago=5, completed=("insights", "automate_pipeline"))
    with patch("ddpui.core.trial.lifecycle_emails.send_trial_completion_email") as mock_send:
        assert run_trial_lifecycle_sweep() == 1
        mock_send.assert_called_once()

    prefs = UserPreferences.objects.get(orguser=orguser)
    assert EMAIL_COMPLETION in prefs.trial_emails_sent
    assert EMAIL_DAY3 in prefs.trial_emails_sent


def test_sweep_is_idempotent():
    """a second sweep with unchanged state sends nothing"""
    _make_trial("trial-d", days_ago=3)
    with patch("ddpui.core.trial.lifecycle_emails.send_trial_day3_not_started_email"):
        assert run_trial_lifecycle_sweep() == 1
        assert run_trial_lifecycle_sweep() == 0


def test_sweep_sends_one_email_per_run():
    """a day-7 trial with no flags gets the day-3 email first, midpoint on the next run"""
    _make_trial("trial-e", days_ago=7)
    with patch(
        "ddpui.core.trial.lifecycle_emails.send_trial_day3_not_started_email"
    ) as mock_a, patch("ddpui.core.trial.lifecycle_emails.send_trial_midpoint_email") as mock_mid:
        assert run_trial_lifecycle_sweep() == 1
        assert mock_a.call_count == 1
        assert mock_mid.call_count == 0
        assert run_trial_lifecycle_sweep() == 1
        assert mock_mid.call_count == 1


def test_sweep_sends_pre_end_email_with_a_formatted_end_date():
    """the highest-stakes email — it warns the workspace and its data are about to be
    permanently deleted — has, until now, only been covered at the decision-ladder level.
    This exercises the full sweep: a day-12 trial on a 14-day window is 2 days from
    end_date, so rule 4 fires. The dispatcher's only real logic
    (`end_date.strftime(END_DATE_DISPLAY_FORMAT)`) must hand the sender a display STRING
    like "15 Aug 2026", never the raw datetime — `render_trial_pre_end_email` calls
    `html.escape` on it, which raises TypeError on a non-str.
    """
    org, orguser = _make_trial("trial-pre-end", days_ago=12)
    # simulate the day-3 and midpoint emails having already gone out on earlier sweeps —
    # otherwise the ladder's earlier rules (which this trial also matches, being past both
    # thresholds) would win and this run would send email A, not the pre-end warning.
    prefs = UserPreferences.objects.get(orguser=orguser)
    prefs.trial_emails_sent = {EMAIL_DAY3: "x", EMAIL_MIDPOINT: "y"}
    prefs.save()

    with patch("ddpui.core.trial.lifecycle_emails.send_trial_pre_end_email") as mock_send:
        assert run_trial_lifecycle_sweep() == 1
        mock_send.assert_called_once()

    args = mock_send.call_args[0]
    end_date_arg = args[3]
    assert isinstance(end_date_arg, str)
    # matches "%d %b %Y", e.g. "15 Aug 2026" — day is zero-padded, month is a 3-letter abbrev
    assert re.fullmatch(r"\d{2} [A-Za-z]{3} \d{4}", end_date_arg)

    expected = org.org_plans.end_date.strftime("%d %b %Y")
    assert end_date_arg == expected

    prefs = UserPreferences.objects.get(orguser=orguser)
    assert EMAIL_PRE_END in prefs.trial_emails_sent


@override_settings(FRONTEND_URL_V2="https://app", TRIAL_SCHEDULE_CALL_URL="https://cal")
def test_sweep_passes_workspace_and_schedule_call_urls_to_the_right_parameter():
    """send_decided_email passes FRONTEND_URL_V2 and SCHEDULE_CALL_URL positionally into
    send_trial_completion_email(to_email, workspace_url, schedule_call_url) — a transposition
    of the two settings would be invisible at sweep level while both are "" in every other
    test. Pin each value to its actual parameter position."""
    _make_trial("trial-urls", days_ago=5, completed=("insights", "automate_pipeline"))
    with patch("ddpui.core.trial.lifecycle_emails.send_trial_completion_email") as mock_send:
        assert run_trial_lifecycle_sweep() == 1
        mock_send.assert_called_once()

    _, workspace_url_arg, schedule_call_url_arg = mock_send.call_args[0]
    assert workspace_url_arg == "https://app"
    assert schedule_call_url_arg == "https://cal"


def test_sweep_skips_non_trial_plans():
    """only Free Trial plans are swept"""
    _make_trial("paid-org", days_ago=5, plan=OrgPlanType.DALGO.value)
    assert run_trial_lifecycle_sweep() == 0


def test_sweep_skips_expired_trials():
    """a trial past its end_date has dropped out of the query"""
    _make_trial("trial-old", days_ago=20)
    assert run_trial_lifecycle_sweep() == 0


def test_sweep_skips_plans_without_a_start_date():
    """a null start_date cannot produce a day number"""
    org = Org.objects.create(slug="trial-nostart", name="x", airbyte_workspace_id=None)
    OrgPlans.objects.create(
        org=org,
        base_plan=OrgPlanType.FREE_TRIAL.value,
        start_date=None,
        end_date=timezone.now() + timedelta(days=5),
    )
    assert run_trial_lifecycle_sweep() == 0


def test_sweep_leaves_flag_unset_when_the_send_fails():
    """an SES failure must not mark the email as sent — the next run retries"""
    _, orguser = _make_trial("trial-f", days_ago=3)
    with patch(
        "ddpui.core.trial.lifecycle_emails.send_trial_day3_not_started_email",
        side_effect=Exception("ses down"),
    ):
        assert run_trial_lifecycle_sweep() == 0

    prefs = UserPreferences.objects.get(orguser=orguser)
    assert prefs.trial_emails_sent == {}


def test_sweep_stamps_the_flag_only_after_the_send_returns():
    """the send must observably happen before the flag is stamped, not merely appear to.

    A mock with no side_effect can't tell "stamp then send" from "send then stamp" — both
    orderings return the same value. Instead, the mocked sender's side_effect reads the row from
    the DATABASE at the moment it is called and asserts trial_emails_sent is still {}. If the
    stamp were ever moved above the send, this assertion inside the mock would fail.
    """
    _, orguser = _make_trial("trial-i", days_ago=3)

    def _assert_not_yet_stamped(*args, **kwargs):
        prefs = UserPreferences.objects.get(orguser=orguser)
        assert prefs.trial_emails_sent == {}

    with patch(
        "ddpui.core.trial.lifecycle_emails.send_trial_day3_not_started_email",
        side_effect=_assert_not_yet_stamped,
    ):
        assert run_trial_lifecycle_sweep() == 1

    prefs = UserPreferences.objects.get(orguser=orguser)
    assert EMAIL_DAY3 in prefs.trial_emails_sent


def test_sweep_continues_after_one_trial_raises():
    """one bad row must not stop the run"""
    _make_trial("trial-g", days_ago=3)
    _make_trial("trial-h", days_ago=3)
    calls = {"n": 0}

    def _flaky(*args, **kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            raise Exception("ses down")

    with patch(
        "ddpui.core.trial.lifecycle_emails.send_trial_day3_not_started_email",
        side_effect=_flaky,
    ):
        assert run_trial_lifecycle_sweep() == 1
    assert calls["n"] == 2


def test_sweep_skips_a_trial_with_no_orguser():
    """a half-deleted trial has no recipient"""
    org = Org.objects.create(slug="trial-orphan", name="x", airbyte_workspace_id=None)
    OrgPlans.objects.create(
        org=org,
        base_plan=OrgPlanType.FREE_TRIAL.value,
        start_date=timezone.now() - timedelta(days=3),
        end_date=timezone.now() + timedelta(days=11),
    )
    assert run_trial_lifecycle_sweep() == 0


def test_sweep_creates_missing_preferences_rather_than_skipping():
    """a missing prefs row must not deny email A to the users it targets"""
    org = Org.objects.create(slug="trial-noprefs", name="x", airbyte_workspace_id=None)
    OrgPlans.objects.create(
        org=org,
        base_plan=OrgPlanType.FREE_TRIAL.value,
        start_date=timezone.now() - timedelta(days=3),
        end_date=timezone.now() + timedelta(days=11),
    )
    user = User.objects.create(username="noprefs@x.org", email="noprefs@x.org")
    OrgUser.objects.create(user=user, org=org)

    with patch("ddpui.core.trial.lifecycle_emails.send_trial_day3_not_started_email"):
        assert run_trial_lifecycle_sweep() == 1
    assert UserPreferences.objects.filter(orguser__org=org).exists()


def test_celery_task_delegates_to_the_sweep():
    """the task is a thin wrapper — all logic lives in the sweep"""
    from ddpui.celeryworkers.tasks import send_trial_lifecycle_emails

    with patch("ddpui.celeryworkers.tasks.run_trial_lifecycle_sweep", return_value=3) as mock_sweep:
        assert send_trial_lifecycle_emails() == 3
        mock_sweep.assert_called_once_with()


def test_superseded_expiry_task_is_gone():
    """check_org_plan_expiry_notify_people duplicated the midpoint and pre-end emails"""
    import ddpui.celeryworkers.tasks as tasks_module

    assert not hasattr(tasks_module, "check_org_plan_expiry_notify_people")


def test_trial_lifecycle_emails_is_registered_as_an_hourly_beat_task():
    """a wrong interval or a dropped add_periodic_task call for this task would pass the
    entire suite today — nothing else exercises setup_periodic_tasks. Call it with a mock
    sender and assert the specific call this task needs is present, regardless of how many
    other periodic tasks are registered alongside it."""
    from ddpui.celeryworkers.tasks import setup_periodic_tasks

    mock_sender = MagicMock()
    setup_periodic_tasks(mock_sender)

    matching_calls = [
        call
        for call in mock_sender.add_periodic_task.call_args_list
        if call.kwargs.get("name") == "trial lifecycle emails"
        or "trial lifecycle emails" in call.args
    ]
    assert len(matching_calls) == 1, "expected exactly one 'trial lifecycle emails' beat entry"

    call = matching_calls[0]
    interval = (
        call.args[0] if call.args else call.kwargs.get("run_every") or call.kwargs.get("schedule")
    )
    assert interval == 3600.0

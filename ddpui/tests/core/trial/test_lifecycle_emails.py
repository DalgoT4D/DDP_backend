from datetime import datetime, timedelta

import pytest
import pytz

from ddpui.core.trial.lifecycle_emails import (
    decide_email,
    FLAGS_STAMPED_BY,
    EMAIL_DAY3,
    EMAIL_COMPLETION,
    EMAIL_MIDPOINT,
    EMAIL_PRE_END,
    completed_flows,
    trial_window,
)


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


def test_decide_email_normalizes_none_flags():
    """flags param can be None; the function normalizes it to {}"""
    # Call decide_email directly with None instead of {} to exercise the normalization line
    at = START + timedelta(days=0)
    assert decide_email(0, 0, None, at, END) is None

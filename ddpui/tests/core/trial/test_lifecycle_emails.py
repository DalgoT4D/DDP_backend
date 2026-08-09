from datetime import datetime, timedelta

import pytz

from ddpui.core.trial.lifecycle_emails import completed_flows, trial_window


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

"""Schema tests for the UserPreferences trial JSONField contracts.

`TrialWalkthroughFlowState` and `TrialEmailsSentState` document what may live inside two JSON
columns. A schema that documents a shape is only worth having if it cannot silently fall behind
the code that writes that shape — so most of these tests assert the two stay in lockstep with
their writers rather than re-testing pydantic.
"""

from typing import get_args

import pytest

from ddpui.core.trial.lifecycle_emails import (
    FLAGS_STAMPED_BY,
    TRACKED_FLOWS,
    completed_flows,
)
from ddpui.schemas.userpreferences_schema import (
    TrialEmailsSentState,
    TrialWalkthroughFlow,
    TrialWalkthroughFlowState,
)


def test_tracked_flows_are_all_real_walkthrough_flows():
    """TRACKED_FLOWS drives the completion email; a typo there would silently never complete."""
    assert set(TRACKED_FLOWS) <= set(get_args(TrialWalkthroughFlow))


def test_emails_sent_state_covers_every_stamped_flag():
    """Every flag the sweep stamps must be a documented key, and vice versa.

    FLAGS_STAMPED_BY is what actually gets written to the column; if the two drift, the schema
    stops describing the data.
    """
    stamped = {flag for flags in FLAGS_STAMPED_BY.values() for flag in flags}
    assert stamped == set(TrialEmailsSentState.model_fields)


def test_flow_state_defaults_to_neither_skipped_nor_completed():
    """An absent flag means 'not done', not 'done' — the default decides who gets nudged."""
    state = TrialWalkthroughFlowState()

    assert state.model_dump() == {"skipped": False, "completed": False}


@pytest.mark.parametrize(
    "entry",
    [
        None,
        "completed",
        {"completed": None},
        {"completed": {"nested": True}},
    ],
)
def test_completed_flows_survives_a_malformed_entry(entry):
    """This JSON is frontend-written and read by an unattended sweep over every live trial.

    A malformed entry must read as 'not completed', never raise — one bad row cannot be allowed
    to stop everyone else's emails.
    """
    assert completed_flows({"insights": entry}) == []


def test_completed_flows_ignores_unknown_keys_and_extra_fields():
    """Forward compatibility: a newer frontend writing extra keys must not break the sweep."""
    walkthrough = {
        "insights": {"skipped": False, "completed": True, "step": 4},
        "some_future_flow": {"completed": True},
    }

    assert completed_flows(walkthrough) == ["insights"]


def test_completed_flows_treats_skipped_as_not_completed():
    """A dismissed walkthrough still earns the nudge — the whole point of the day-3 email."""
    assert completed_flows({"insights": {"skipped": True, "completed": False}}) == []

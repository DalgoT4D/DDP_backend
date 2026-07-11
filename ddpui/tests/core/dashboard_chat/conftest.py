"""Shared pytest fixtures for dashboard-chat backend tests."""

import sys

import pytest


@pytest.fixture(autouse=True)
def reset_dashboard_chat_runtime_state():
    """Release shared LangGraph runtime resources between dashboard-chat tests only.

    The dashboard-chat runtime holds a shared Postgres-backed LangGraph checkpointer open.
    Resetting it after each dashboard-chat test avoids leaking DB sessions into teardown
    without imposing that cleanup on the rest of the backend test suite.
    """
    yield
    if "ddpui.core.dashboard_chat.orchestration.orchestrator" in sys.modules:
        from ddpui.core.dashboard_chat.orchestration.orchestrator import (
            reset_dashboard_chat_runtime,
        )

        reset_dashboard_chat_runtime()
    if "ddpui.core.dashboard_chat.orchestration.checkpoints" in sys.modules:
        from ddpui.core.dashboard_chat.orchestration.checkpoints import (
            reset_dashboard_chat_checkpointer,
        )

        reset_dashboard_chat_checkpointer()

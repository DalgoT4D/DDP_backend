"""Progress and cancellation hooks for one dashboard-chat runtime invocation."""

from collections.abc import Callable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar

from ddpui.core.dashboard_chat.contracts.event_contracts import DashboardChatProgressStage


class DashboardChatRunCancelled(Exception):
    """Raised when a running dashboard-chat turn has been cancelled."""


_current_progress_publisher: ContextVar[
    Callable[[str, DashboardChatProgressStage | None], None] | None
] = ContextVar("dashboard_chat_progress_publisher", default=None)
_current_cancel_checker: ContextVar[Callable[[], bool] | None] = ContextVar(
    "dashboard_chat_cancel_checker",
    default=None,
)


@contextmanager
def dashboard_chat_runtime_hooks(
    *,
    progress_publisher: Callable[[str, DashboardChatProgressStage | None], None] | None = None,
    cancel_checker: Callable[[], bool] | None = None,
) -> Iterator[None]:
    """Install per-run progress and cancellation hooks for the current execution context."""

    progress_token = _current_progress_publisher.set(progress_publisher)
    cancel_token = _current_cancel_checker.set(cancel_checker)
    try:
        yield
    finally:
        _current_progress_publisher.reset(progress_token)
        _current_cancel_checker.reset(cancel_token)


def publish_runtime_progress(
    label: str,
    stage: DashboardChatProgressStage | None = None,
) -> None:
    """Publish one progress label if the current run has a registered publisher."""

    progress_publisher = _current_progress_publisher.get()
    if progress_publisher is not None:
        progress_publisher(label, stage)


def raise_if_runtime_cancelled() -> None:
    """Raise if the current run has been marked cancelled by its owner."""

    cancel_checker = _current_cancel_checker.get()
    if cancel_checker is not None and cancel_checker():
        raise DashboardChatRunCancelled()

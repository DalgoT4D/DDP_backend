"""Tests for ddpui/utils/thread.py

Covers:
  - get_current_request returns None by default
  - set_current_request / get_current_request round-trip
  - Thread isolation (different threads see different requests)
"""

import threading
from unittest.mock import MagicMock

from ddpui.utils.thread import get_current_request, set_current_request


class TestThreadLocalRequest:
    def test_get_current_request_default_none(self):
        """Without setting, get_current_request returns None."""
        # Clear any previous state
        from ddpui.utils.thread import _thread_locals

        if hasattr(_thread_locals, "request"):
            del _thread_locals.request

        assert get_current_request() is None

    def test_set_and_get_roundtrip(self):
        mock_request = MagicMock()
        set_current_request(mock_request)
        assert get_current_request() is mock_request

    def test_overwrite_request(self):
        req1 = MagicMock()
        req2 = MagicMock()
        set_current_request(req1)
        assert get_current_request() is req1
        set_current_request(req2)
        assert get_current_request() is req2

    def test_thread_isolation(self):
        """Different threads should have independent request objects."""
        main_request = MagicMock(name="main_request")
        set_current_request(main_request)

        child_results = {}

        def child_thread_fn():
            # Should not see parent thread's request
            child_results["before_set"] = get_current_request()
            child_req = MagicMock(name="child_request")
            set_current_request(child_req)
            child_results["after_set"] = get_current_request()

        t = threading.Thread(target=child_thread_fn)
        t.start()
        t.join()

        # Child thread should not have seen main thread's request
        assert child_results["before_set"] is None
        assert child_results["after_set"] is not main_request
        # Main thread's request should be unchanged
        assert get_current_request() is main_request

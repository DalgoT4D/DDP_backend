"""Tests for PdfExportService.generate_pdf — dashboard_filters URL encoding.

Playwright itself is fully mocked (no real browser); these tests only verify
the URL handed to page.goto() carries the caller's filter values correctly.
"""

import os
import django
from unittest.mock import patch, MagicMock

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

import pytest
from django.test import override_settings
from ddpui.core.reports.pdf_export_service import PdfExportService

pytestmark = pytest.mark.django_db


def _make_mock_playwright_cm(mock_page):
    """A mock for the `with sync_playwright() as p:` context manager."""
    mock_browser = MagicMock()
    mock_browser.new_page.return_value = mock_page
    mock_playwright_instance = MagicMock()
    mock_playwright_instance.chromium.launch.return_value = mock_browser
    mock_cm = MagicMock()
    mock_cm.__enter__.return_value = mock_playwright_instance
    mock_cm.__exit__.return_value = False
    return mock_cm


def _make_mock_page():
    mock_page = MagicMock()
    mock_page.pdf.return_value = b"%PDF-1.4 content"
    mock_page.evaluate.return_value = []  # no <img> elements to rasterize
    return mock_page


class TestGeneratePdfDashboardFilters:
    """dashboard_filters, when provided, must be encoded into the Playwright URL
    so the print-mode page can seed its filter state from it."""

    @override_settings(RENDER_SECRET="test-secret", FRONTEND_URL_V2="http://localhost:3001")
    @patch("ddpui.core.reports.pdf_export_service.sync_playwright")
    def test_appends_dashboard_filters_to_url(self, mock_sync_playwright):
        mock_page = _make_mock_page()
        mock_sync_playwright.return_value = _make_mock_playwright_cm(mock_page)

        result = PdfExportService.generate_pdf(1, "tok123", dashboard_filters={"1": "2025-01-15"})

        assert result == b"%PDF-1.4 content"
        goto_url = mock_page.goto.call_args[0][0]
        assert goto_url.startswith(
            "http://localhost:3001/share/report/tok123?print=true&dashboard_filters="
        )
        assert "2025-01-15" in goto_url

    @override_settings(RENDER_SECRET="test-secret", FRONTEND_URL_V2="http://localhost:3001")
    @patch("ddpui.core.reports.pdf_export_service.sync_playwright")
    def test_no_dashboard_filters_omits_query_param(self, mock_sync_playwright):
        mock_page = _make_mock_page()
        mock_sync_playwright.return_value = _make_mock_playwright_cm(mock_page)

        PdfExportService.generate_pdf(1, "tok123")

        goto_url = mock_page.goto.call_args[0][0]
        assert goto_url == "http://localhost:3001/share/report/tok123?print=true"

    @override_settings(RENDER_SECRET=None)
    def test_missing_render_secret_raises(self):
        with pytest.raises(ValueError, match="RENDER_SECRET"):
            PdfExportService.generate_pdf(1, "tok123")

"""PDF export service using Playwright for server-side report rendering"""

import os
from datetime import datetime

from django.conf import settings

from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui.core.reports.pdf_export")

EXPORTS_DIR = os.path.join(settings.BASE_DIR, "exports")
VIEWPORT_WIDTH = 1200
VIEWPORT_HEIGHT = 800
PDF_WAIT_TIMEOUT_MS = 2000
CANVAS_TIMEOUT_MS = 30000


class PdfExportService:
    """Generates PDFs from report snapshots using headless Playwright Chromium"""

    @staticmethod
    def generate_pdf(snapshot_id: int, share_token: str) -> str:
        """Generate a PDF of a report snapshot.

        Launches headless Chromium, navigates to the public report page
        in print mode, waits for charts to render, and generates a PDF.

        Args:
            snapshot_id: The snapshot ID
            share_token: Public share token for accessing the report

        Returns:
            File path to the generated PDF

        Raises:
            Exception: If PDF generation fails
        """
        from playwright.sync_api import sync_playwright

        frontend_url = getattr(settings, "FRONTEND_URL_V2", None) or getattr(
            settings, "FRONTEND_URL", "http://localhost:3001"
        )
        if not frontend_url or str(frontend_url).startswith("/"):
            frontend_url = "http://127.0.0.1:3001"
        url = f"{frontend_url.rstrip('/')}/share/report/{share_token}?print=true"

        os.makedirs(EXPORTS_DIR, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        pdf_path = os.path.join(EXPORTS_DIR, f"report_{snapshot_id}_{timestamp}.pdf")

        logger.info(f"Generating PDF for snapshot {snapshot_id} from {url}")

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            try:
                page = browser.new_page(
                    viewport={"width": VIEWPORT_WIDTH, "height": VIEWPORT_HEIGHT}
                )
                page.goto(url, wait_until="networkidle")
                page.wait_for_selector(
                    "[data-pdf-ready='true']", timeout=CANVAS_TIMEOUT_MS
                )
                try:
                    page.wait_for_selector("canvas", timeout=CANVAS_TIMEOUT_MS)
                except Exception:
                    logger.warning(
                        f"No canvas found for snapshot {snapshot_id}, proceeding without charts"
                    )
                page.wait_for_timeout(PDF_WAIT_TIMEOUT_MS)
                page.pdf(
                    path=pdf_path,
                    format="A4",
                    print_background=True,
                    scale=1,
                )
                logger.info(f"PDF generated at {pdf_path}")
                return pdf_path
            finally:
                browser.close()

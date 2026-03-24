"""PDF export service using Playwright for server-side report rendering"""

from django.conf import settings

from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui.core.reports.pdf_export")

VIEWPORT_WIDTH = 1200
VIEWPORT_HEIGHT = 800
PDF_WAIT_TIMEOUT_MS = 2000
CANVAS_TIMEOUT_MS = 30000


class PdfExportService:
    """Generates PDFs from report snapshots using headless Playwright Chromium"""

    @staticmethod
    def generate_pdf(snapshot_id: int, share_token: str) -> bytes:
        """Generate a PDF of a report snapshot.

        Launches headless Chromium, navigates to the public report page
        in print mode, and injects an X-Render-Secret header via route
        interception so the backend serves data without requiring
        is_public=True on the snapshot.

        Args:
            snapshot_id: The snapshot ID
            share_token: The snapshot's public_share_token (used in the URL)

        Returns:
            PDF contents as bytes

        Raises:
            ValueError: If RENDER_SECRET is not configured
            Exception: If PDF generation fails
        """
        from playwright.sync_api import sync_playwright

        render_secret = getattr(settings, "RENDER_SECRET", None)
        if not render_secret:
            raise ValueError(
                "RENDER_SECRET is not configured. "
                "Set the RENDER_SECRET environment variable."
            )

        frontend_url = getattr(settings, "FRONTEND_URL_V2", None) or getattr(
            settings, "FRONTEND_URL", "http://localhost:3001"
        )
        if not frontend_url or str(frontend_url).startswith("/"):
            frontend_url = "http://127.0.0.1:3001"
        url = f"{frontend_url.rstrip('/')}/share/report/{share_token}?print=true"

        logger.info(f"Generating PDF for snapshot {snapshot_id} from {url}")

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            try:
                page = browser.new_page(
                    viewport={"width": VIEWPORT_WIDTH, "height": VIEWPORT_HEIGHT}
                )

                # Intercept all requests and inject the render secret header.
                # This lets the backend's public report endpoints serve data
                # without the snapshot needing is_public=True.
                def _inject_render_secret(route):
                    headers = {**route.request.headers, "x-render-secret": render_secret}
                    route.continue_(headers=headers)

                page.route("**/*", _inject_render_secret)

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

                pdf_bytes = page.pdf(
                    format="A4",
                    print_background=True,
                    scale=1,
                )
                logger.info(
                    f"PDF generated for snapshot {snapshot_id} ({len(pdf_bytes)} bytes)"
                )
                return pdf_bytes
            finally:
                browser.close()

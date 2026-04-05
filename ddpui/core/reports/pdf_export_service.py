"""PDF export service using Playwright for server-side report rendering"""

import time

from django.conf import settings
from playwright.sync_api import sync_playwright

from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui.core.reports.pdf_export")

VIEWPORT_WIDTH = 1200
VIEWPORT_HEIGHT = 800
PDF_SCALE = 0.66
# Seconds to wait after network idle for ECharts to render canvases
POST_IDLE_WAIT_S = 1
NETWORK_IDLE_TIMEOUT_MS = 30000


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
        render_secret = getattr(settings, "RENDER_SECRET", None)
        if not render_secret:
            raise ValueError(
                "RENDER_SECRET is not configured. " "Set the RENDER_SECRET environment variable."
            )

        frontend_url = getattr(settings, "FRONTEND_URL_V2", None) or getattr(
            settings, "FRONTEND_URL", "http://localhost:3001"
        )
        if not frontend_url or str(frontend_url).startswith("/"):
            frontend_url = "http://localhost:3001"
        url = f"{frontend_url.rstrip('/')}/share/report/{share_token}?print=true"

        logger.info(f"Generating PDF for snapshot {snapshot_id} from {url}")

        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                args=[
                    "--disable-gpu",
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-extensions",
                    "--disable-background-networking",
                    "--disable-default-apps",
                ],
            )
            try:
                page = browser.new_page(
                    viewport={"width": VIEWPORT_WIDTH, "height": VIEWPORT_HEIGHT},
                    device_scale_factor=2,
                )

                def _inject_render_secret(route):
                    headers = {**route.request.headers, "x-render-secret": render_secret}
                    route.continue_(headers=headers)

                page.route("**/*", _inject_render_secret)

                console_messages = []
                failed_requests = []
                page.on("console", lambda msg: console_messages.append(f"[{msg.type}] {msg.text}"))
                page.on(
                    "requestfailed",
                    lambda req: failed_requests.append(f"{req.method} {req.url} -> {req.failure}"),
                )
                page.on(
                    "response",
                    lambda res: (
                        failed_requests.append(f"{res.request.method} {res.url} -> {res.status}")
                        if res.status >= 400
                        else None
                    ),
                )

                # Load page fast, then wait for all API calls to finish separately
                page.goto(url, wait_until="domcontentloaded")
                page.wait_for_load_state("networkidle", timeout=NETWORK_IDLE_TIMEOUT_MS)

                # Brief wait for ECharts to render canvases from received data
                time.sleep(POST_IDLE_WAIT_S)

                if failed_requests:
                    logger.warning(
                        f"Failed requests during PDF generation for snapshot {snapshot_id}: "
                        f"{failed_requests}"
                    )

                pdf_bytes = page.pdf(
                    format="A4",
                    print_background=True,
                    scale=PDF_SCALE,
                )
                logger.info(f"PDF generated for snapshot {snapshot_id} ({len(pdf_bytes)} bytes)")
                return pdf_bytes
            finally:
                browser.close()

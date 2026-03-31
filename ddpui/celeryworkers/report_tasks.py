"""Celery tasks for report email sharing"""

import re

from ddpui.celery import app
from ddpui.core.reports.pdf_export_service import PdfExportService
from ddpui.utils.awsses import send_email_with_attachment
from ddpui.utils.custom_logger import CustomLogger
from ddpui.utils.email_templates import render_share_report_email

logger = CustomLogger("ddpui.celeryworkers.report_tasks")


@app.task(bind=True)
def send_report_email_task(
    self,
    snapshot_id,
    share_token,
    recipient_emails,
    sender_name,
    report_title,
    report_url,
    message=None,
):
    """Generate a PDF of the report and email it to all recipients.

    Args:
        snapshot_id: The snapshot ID (for PDF generation)
        share_token: The snapshot's public_share_token
        recipient_emails: List of email addresses to send to
        sender_name: Display name of the person sharing
        report_title: Title of the report
        report_url: Public URL to view the report
        message: Optional personal message from the sender
    """
    # Generate PDF once for all recipients
    logger.info(
        f"Generating PDF for snapshot {snapshot_id} "
        f"to send to {len(recipient_emails)} recipients"
    )
    pdf_bytes = PdfExportService.generate_pdf(snapshot_id, share_token)

    # Sanitize title for filename
    safe_title = re.sub(r"[^\w\s\-]", "", report_title).strip() or "report"
    filename = f"{safe_title}.pdf"

    # Render email template
    plain_text, html_body = render_share_report_email(
        sender_name=sender_name,
        report_title=report_title,
        report_url=report_url,
        message=message,
    )

    subject = f"{report_title} \U0001f680"

    sent_count = 0
    for recipient in recipient_emails:
        try:
            send_email_with_attachment(
                to_email=recipient,
                subject=subject,
                text_body=plain_text,
                html_body=html_body,
                attachment_bytes=pdf_bytes,
                attachment_filename=filename,
            )
            sent_count += 1
            logger.info(f"Sent report email to {recipient}")
        except Exception as e:
            logger.error(
                f"Failed to send report email to {recipient}: {e}",
                exc_info=True,
            )

    logger.info(
        f"Sent report '{report_title}' to " f"{sent_count} of {len(recipient_emails)} recipients"
    )

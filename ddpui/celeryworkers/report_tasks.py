"""Celery tasks for report email sharing"""

import re

from ddpui.celery import app
from ddpui.core.reports.pdf_export_service import PdfExportService
from ddpui.core.reports.report_service import ReportService
from ddpui.models.org_user import OrgUser
from ddpui.models.report import ReportSnapshot
from ddpui.core.notifications.notifications_functions import create_notification
from ddpui.schemas.notifications_api_schemas import NotificationDataSchema
from ddpui.utils.awsses import send_email_with_attachment
from ddpui.utils.custom_logger import CustomLogger
from ddpui.utils.email_templates import render_share_report_email

logger = CustomLogger("ddpui.celeryworkers.report_tasks")


@app.task(bind=True)
def send_report_email_task(
    self,
    snapshot_id,
    orguser_id,
    recipient_emails,
    subject=None,
):
    """Generate a PDF of the report and email it to all recipients.

    Args:
        snapshot_id: The snapshot ID
        orguser_id: The ID of the OrgUser who triggered the share
        recipient_emails: List of email addresses to send to
        subject: Optional custom email subject line
    """
    snapshot = ReportSnapshot.objects.get(id=snapshot_id)
    orguser = OrgUser.objects.select_related("user").get(id=orguser_id)

    sender_name = orguser.user.email
    report_title = snapshot.title

    # Always include the private (authenticated) URL
    private_url = ReportService._build_private_url(snapshot_id)

    # Include the public URL only if the report is publicly shared
    public_url = None
    if snapshot.is_public and snapshot.public_share_token:
        public_url = ReportService._build_public_url(snapshot.public_share_token)

    # Start with all recipients as failed; remove as they succeed
    failed_recipients = list(recipient_emails)

    try:
        # Ensure a share token exists for PDF generation
        share_token = ReportService.ensure_share_token(snapshot)

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
            private_url=private_url,
            public_url=public_url,
        )

        email_subject = subject if subject else f"Report: {report_title}"

        for recipient in recipient_emails:
            try:
                send_email_with_attachment(
                    to_email=recipient,
                    subject=email_subject,
                    text_body=plain_text,
                    html_body=html_body,
                    attachment_bytes=pdf_bytes,
                    attachment_filename=filename,
                )
                failed_recipients.remove(recipient)
                logger.info(f"Sent report email to {recipient}")
            except Exception as e:
                logger.error(
                    f"Failed to send report email to {recipient}: {e}",
                    exc_info=True,
                )

        sent_count = len(recipient_emails) - len(failed_recipients)
        logger.info(
            f"Sent report '{report_title}' to "
            f"{sent_count} of {len(recipient_emails)} recipients"
        )

    except Exception as e:
        logger.error(
            f"Failed to send report '{report_title}': {e}",
            exc_info=True,
        )

    finally:
        if failed_recipients:
            failed_str = ", ".join(failed_recipients)
            create_notification(
                NotificationDataSchema(
                    author="System",
                    message=(
                        f'Report "{report_title}" could not be sent to these recipients: {failed_str}. '
                        f"Please try again."
                    ),
                    email_subject=f"Report sharing failed: {report_title}",
                    urgent=True,
                    recipients=[orguser_id],
                )
            )

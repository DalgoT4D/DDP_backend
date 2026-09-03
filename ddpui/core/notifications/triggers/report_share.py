"""Report-share notification trigger.

Fires from ``celeryworkers/report_tasks.py::send_report_email_task`` after the
PDF is generated. Owns:
- Email delivery with the PDF attached to every recipient (matched + external)
- In-app bell entry for recipients that resolve to an OrgUser in the sender's
  org — fires org-level Discord alongside if enabled (via ``create_notification``
  with ``skip_email=True`` so we don't fire a second generic email)

Returns the list of emails that failed delivery so the task can raise a
follow-up "sharing failed" notification back to the sharer.
"""

from __future__ import annotations

from ddpui.core.notifications.notifications_functions import create_notification
from ddpui.models.org_user import OrgUser
from ddpui.models.report import ReportSnapshot
from ddpui.schemas.notifications_api_schemas import NotificationDataSchema
from ddpui.utils.awsses import send_email_with_attachment
from ddpui.utils.custom_logger import CustomLogger


logger = CustomLogger("ddpui.notifications.triggers.report_share")


def notify_report_shared(
    *,
    sender: OrgUser,
    snapshot: ReportSnapshot,
    recipient_emails: list,
    email_subject: str,
    plain_body: str,
    html_body: str,
    pdf_bytes: bytes,
    filename: str,
    private_url: str,
) -> list:
    """Fan out the report share to recipients.

    For each address in ``recipient_emails``:
    - Send the PDF-attached email via SES.
    - If the address maps to an OrgUser in the sender's org, also create an
      in-app bell row (best-effort — failures logged, never raised).

    Returns:
        List of email addresses that failed to deliver. Callers use this to
        fire a follow-up notification back to the sender.
    """
    orgusers_by_email = {
        ou.user.email: ou
        for ou in OrgUser.objects.filter(
            org=sender.org, user__email__in=recipient_emails
        ).select_related("user")
    }

    sender_email = sender.user.email
    report_title = snapshot.title
    message = f"{sender_email} shared report '{report_title}' with you.\n{private_url}"
    in_app_subject = f"Report shared: {report_title}"

    failed: list = list(recipient_emails)

    for recipient_email in recipient_emails:
        orguser = orgusers_by_email.get(recipient_email)
        if orguser is not None and orguser.id != sender.id:
            try:
                create_notification(
                    NotificationDataSchema(
                        author=sender_email,
                        message=message,
                        email_subject=in_app_subject,
                        urgent=False,
                        scheduled_time=None,
                        recipients=[orguser.id],
                        skip_email=True,
                    )
                )
            except Exception as err:
                logger.error(
                    f"report-share in-app notification failed for orguser {orguser.id}: {err}"
                )

        try:
            send_email_with_attachment(
                to_email=recipient_email,
                subject=email_subject,
                text_body=plain_body,
                html_body=html_body,
                attachment_bytes=pdf_bytes,
                attachment_filename=filename,
            )
            failed.remove(recipient_email)
            logger.info(f"Sent report email to {recipient_email}")
        except Exception as e:
            logger.error(
                f"Failed to send report email to {recipient_email}: {e}",
                exc_info=True,
            )

    return failed

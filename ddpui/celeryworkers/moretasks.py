from datetime import datetime
from ddpui.celery import app
from ddpui.models.notifications import Notification
from ddpui.models.org_user import OrgUser
from ddpui.models.userpreferences import UserPreferences
from ddpui.utils.awsses import send_text_message
from ddpui.utils import timezone
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui")


@app.task(bind=True)
def schedule_notification_task(self, notification_id, recipient_id):  # skipcq: PYL-W0613
    """send scheduled notifications"""
    notification = Notification.objects.get(id=notification_id)
    recipient = OrgUser.objects.get(user_id=recipient_id)
    user_preference, _ = UserPreferences.objects.get_or_create(orguser=recipient)

    notification.sent_time = timezone.as_utc(datetime.now())
    notification.save()

    if user_preference.enable_email_notifications:
        try:
            send_text_message(
                user_preference.orguser.user.email, notification.email_subject, notification.message
            )
        except Exception as e:
            logger.error(f"Error sending discord notification: {str(e)}")

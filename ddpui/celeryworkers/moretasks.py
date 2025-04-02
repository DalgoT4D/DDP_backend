import os
from datetime import datetime
from ddpui.celery import app


from ddpui.core.orguserfunctions import create_orguser
from ddpui.models.notifications import Notification
from ddpui.models.org_user import OrgUser, OrgUserCreate
from ddpui.models.org import Org, CreateFreeTrailOrgSchema, OrgVizLoginType
from ddpui.models.userpreferences import UserPreferences

from ddpui.utils.awsses import send_text_message
from ddpui.utils import timezone, awsses
from ddpui.utils.custom_logger import CustomLogger
from ddpui.utils.helpers import generate_hash_id

from ddpui.core import infra_service

logger = CustomLogger("ddpui")


@app.task(bind=True)
def schedule_notification_task(self, notification_id, recipient_id):  # skipcq: PYL-W0613
    """send scheduled notifications"""
    notification = Notification.objects.get(id=notification_id)
    recipient = OrgUser.objects.get(id=recipient_id)
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


@app.task(bind=True)
def create_free_trail_org_account(self, payload: dict):
    """
    1. Create org
    2. Create the account manage orguser
    3. Provision a postgres warehouse for the org
    4. Create the corresponding postgres destination in the airbyte (this will also save in secrets manager)
    5. Email the warehouse creds to ADMIN_EMAIL
    5. Provision superset
    6. Email superset admin user creds to redis to ADMIN_EMAIL
    7. (Optional for now) Add the database connection of the above warehouse
    """
    from ddpui.core.orgfunctions import create_organization

    create_org_payload = CreateFreeTrailOrgSchema(**payload)

    # create org
    org, error = create_organization(create_org_payload)
    if error:
        raise Exception(error)
    logger.info(f"Created org {org.slug} with airbyte workspace {org.airbyte_workspace_id}")

    # create orguser
    password = generate_hash_id(10)
    payload = OrgUserCreate(
        email=create_org_payload.email,
        password=password,
        signupcode=os.getenv("SIGNUPCODE"),
        role=None,
    )
    orguser = create_orguser(payload, True)
    logger.info(f"Created the orguser with email {orguser.user.email}")

    # provision postgres warehouse
    creds = infra_service.create_warehouse_in_rds(org.slug)
    if not all(key in creds for key in ["dbname", "host", "port", "user", "password"]):
        raise KeyError("Warehouse creds generated is missing some key")

    logger.info(f"Created warehouse with dbname: {creds['dbname']}")

    awsses.send_text_message(
        os.getenv("ADMIN_EMAIL"),
        f"Warehouse creds for trial account of org {org.slug}",
        "\n".join([f"{key}: {value}" for key, value in creds]),
    )

    logger.info("Emailed warehouse creds to platform admin")

    # provision superset
    superset_res = infra_service.create_superset_instance()
    if not all(key in superset_res for key in ["url", "admin_user", "admin_password"]):
        raise KeyError("Superset creation didn't send back all the information needed")

    org.viz_url = superset_res["url"]
    org.viz_login_type = OrgVizLoginType.BASIC_AUTH
    org.save()

    logger.info(f"Provisioned superset at : {superset_res['url']}")

    awsses.send_text_message(
        os.getenv("ADMIN_EMAIL"),
        f"Superset admin user creds for trial account of org {org.slug}",
        "\n".join([f"{key}: {value}" for key, value in superset_res]),
    )

    logger.info("Emailed superset admin user creds to platform admin")

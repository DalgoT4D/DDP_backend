from django.contrib.auth.models import User

from ddpui.models.org import Org
from ddpui.models.trial_clone import TrialClone, TrialCloneStatus
from ddpui.core.trial.timing import step_timer
from ddpui.core.orgfunctions import create_organization, create_org_plan
from ddpui.schemas.org_schema import CreateOrgSchema
from ddpui.models.org_plans import OrgPlanType
from ddpui.models.org_user import OrgUser
from ddpui.models.userpreferences import UserPreferences
from ddpui.models.role_based_access import Role
from ddpui.auth import ACCOUNT_MANAGER_ROLE
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui.core.trial.clone_service")


def _step_org_and_user(template: Org, trialclone: TrialClone) -> None:
    """Step 1 — create the trial org (+ Airbyte workspace + plan) and an admin user."""
    trial_name = f"Trial {trialclone.id} {template.name}"
    org_payload = CreateOrgSchema(
        name=trial_name,
        base_plan=OrgPlanType.FREE_TRIAL.value,
        can_upgrade_plan=False,
        subscription_duration="trial",
        superset_included=False,
    )
    trial_org, err = create_organization(org_payload)
    if err:
        raise RuntimeError(f"create_organization failed: {err}")

    _, plan_err = create_org_plan(org_payload, trial_org)
    if plan_err:
        raise RuntimeError(f"create_org_plan failed: {plan_err}")

    # admin user — password is set later via the activation flow (Try Now), so unusable now.
    # NB: a brand-new Django User has password == "" which has_usable_password() treats as
    # "usable" (only None / the "!"-prefixed sentinel count as unusable), so we key off the
    # get_or_create `created` flag rather than has_usable_password() to decide when to reset it.
    user, created = User.objects.get_or_create(
        username=trialclone.trial_email,
        defaults={"email": trialclone.trial_email},
    )
    if created:
        user.set_unusable_password()
        user.save(update_fields=["password"])

    admin_role = Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first()
    if admin_role is None:
        raise RuntimeError(f"role {ACCOUNT_MANAGER_ROLE} not found (load role fixtures)")

    orguser = OrgUser.objects.create(
        user=user, org=trial_org, new_role=admin_role, email_verified=False
    )
    UserPreferences.objects.get_or_create(
        orguser=orguser, defaults={"enable_email_notifications": True}
    )

    trialclone.trial_org = trial_org
    trialclone.manifest["trial_org_slug"] = trial_org.slug
    trialclone.manifest["trial_workspace_id"] = trial_org.airbyte_workspace_id
    trialclone.manifest["trial_orguser_id"] = orguser.id
    trialclone.manifest["custom_connectors"] = "queued_async_not_awaited"
    trialclone.save(update_fields=["trial_org", "manifest", "updated_at"])


def _step_warehouse(template: Org, trialclone: TrialClone) -> None:
    """Step 2 — provision trial warehouse. Real body lands in Task 4."""
    return None


def _step_warehouse_data(template: Org, trialclone: TrialClone) -> None:
    """Step 3 — pg_dump/restore template warehouse → trial. Real body lands in Task 5."""
    return None


def clone_template_org(template_org_id: int, trial_email: str) -> TrialClone:
    """Deep-clone a template org into a new trial org (Steps 1–3), timing each step.

    Serial chain: org+user → warehouse → warehouse-data. On any failure the run is
    marked FAILED (with the error) and the exception is re-raised so the caller
    (management command / future Celery task) sees it.
    """
    template = Org.objects.get(id=template_org_id)
    trialclone = TrialClone.objects.create(
        template_org=template,
        trial_email=trial_email,
        status=TrialCloneStatus.RUNNING.value,
    )
    logger.info(f"starting clone {trialclone.id} from template {template.slug}")
    try:
        with step_timer(trialclone, "step1_org_user"):
            _step_org_and_user(template, trialclone)
        with step_timer(trialclone, "step2_warehouse"):
            _step_warehouse(template, trialclone)
        with step_timer(trialclone, "step3_warehouse_data"):
            _step_warehouse_data(template, trialclone)
    except Exception as err:
        trialclone.status = TrialCloneStatus.FAILED.value
        trialclone.error = str(err)
        trialclone.save(update_fields=["status", "error", "updated_at"])
        logger.error(f"clone {trialclone.id} failed: {err}")
        raise
    trialclone.status = TrialCloneStatus.COMPLETED.value
    trialclone.save(update_fields=["status", "updated_at"])
    logger.info(f"clone {trialclone.id} completed; timings={trialclone.timings}")
    return trialclone

from ddpui.models.org import Org
from ddpui.models.trial_clone import TrialClone, TrialCloneStatus
from ddpui.core.trial.timing import step_timer
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui.core.trial.clone_service")


def _step_org_and_user(template: Org, trialclone: TrialClone) -> None:
    """Step 1 — create trial org + admin user. Real body lands in Task 3."""
    return None


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

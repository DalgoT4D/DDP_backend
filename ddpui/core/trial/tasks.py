"""Celery task wrapping clone_template_org with Redis-backed progress reporting."""

import os

from ddpui.celery import app
from ddpui.utils.taskprogress import TaskProgress
from ddpui.core.trial.clone_service import clone_template_org
from ddpui.utils import awsses
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui.core.trial.tasks")


@app.task(bind=True)
def clone_trial_org_task(
    self, task_id, template_org_id, email, org_name, role
):  # pylint: disable=unused-argument
    """Run clone_template_org for a free-trial signup, reporting progress into Redis.

    `role` is the job-title captured on the public signup form — it is metadata ONLY (stored
    on the activation token for potential future use, e.g. analytics) and must NEVER be used
    as the RBAC role_slug for the cloned OrgUser: it is client-supplied and an attacker could
    submit role="super-admin" to self-grant elevated permissions. A trial user always gets
    clone_template_org's own default role (ACCOUNT_MANAGER_ROLE) by passing role_slug=None.
    """
    hashkey = f"trial-clone-{task_id}"
    progress = TaskProgress(task_id, hashkey)
    progress.add({"message": "queued", "status": "queued"})

    try:
        run = clone_template_org(
            template_org_id,
            email,
            org_name=org_name,
            role_slug=None,
            progress=lambda n, label: progress.add(
                {"step": n, "message": label, "status": "running"}
            ),
        )
    except Exception as err:  # skipcq PYL-W0703
        # log the real detail server-side only — the progress entry is polled by the public
        # /trial/status endpoint and must not leak internal exception text (M2).
        logger.error(f"trial clone failed for {email}: {err}")
        progress.add({"message": "clone failed", "status": "failed"})
        return

    progress.add({"message": "done", "status": "completed", "org_slug": run.trial_org.slug})

    # welcome email so the user gets in even if they closed the progress tab mid-clone
    try:
        login_url = os.getenv("FRONTEND_URL_V2") or ""
        awsses.send_trial_welcome_email(email, login_url)
    except Exception as err:  # skipcq PYL-W0703 — email failure must not fail the (done) clone
        logger.error(f"failed to send trial welcome email to {email}: {err}")

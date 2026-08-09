"""Celery task wrapping clone_template_org with Redis-backed progress reporting."""

from django.conf import settings

from ddpui.celery import app
from ddpui.utils.taskprogress import TaskProgress
from ddpui.core.trial.clone_service import clone_template_org
from ddpui.core.trial.activation import release_clone_lock
from ddpui.schemas.trial_schema import TrialCloneRequest
from ddpui.utils import awsses
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui.core.trial.tasks")

# soft_time_limit raises SoftTimeLimitExceeded (an Exception subclass) INSIDE the task at 300s —
# caught by clone_template_org's `except Exception`, so a hung clone tears down and reports
# "failed" like any other failure. The hard time_limit is 60s later so teardown has room to run
# before Celery SIGKILLs the worker. Keep in sync with CLONE_LOCK_TTL_SECONDS (the dead-worker
# lock backstop).
CLONE_SOFT_TIME_LIMIT = 300
CLONE_HARD_TIME_LIMIT = 360


@app.task(bind=True, soft_time_limit=CLONE_SOFT_TIME_LIMIT, time_limit=CLONE_HARD_TIME_LIMIT)
def clone_trial_org_task(
    self, task_id, template_org_id, email, org_name, role
):  # pylint: disable=unused-argument
    """Run clone_template_org for a free-trial signup, reporting progress into Redis.

    `role` is the job-title captured on the public signup form — it is metadata ONLY (persisted
    as `OrgUser.work_domain`, the same field the post-invitation signup writes) and must NEVER
    be used as the RBAC role_slug for the cloned OrgUser: it is client-supplied and an attacker
    could submit role="super-admin" to self-grant elevated permissions. A trial user always gets
    clone_template_org's own default role (ACCOUNT_MANAGER_ROLE) by passing role_slug=None.

    Releases the per-email running-clone lock in a `finally` so the email is freed the moment the
    clone ends (success or fail) — a subsequent POST /trial/retry can then re-run immediately
    rather than waiting out the lock's dead-worker TTL.
    """
    hashkey = f"trial-clone-{task_id}"
    # a fresh TaskProgress starts with an empty list and OVERWRITES the redis key on its first
    # add() — so a retry reusing this task_id gets a clean progress history, not the failed run's.
    # 24h TTL so the polled progress hash doesn't leak in redis forever after the clone ends.
    progress = TaskProgress(task_id, hashkey, 24 * 3600)
    progress.add({"message": "queued", "status": "queued"})

    try:
        run = clone_template_org(
            TrialCloneRequest(
                template_org_id=template_org_id,
                trial_email=email,
                org_name=org_name,
                role_slug=None,
                work_domain=role,
            ),
            progress=lambda n, label: progress.add(
                {"step": n, "message": label, "status": "running"}
            ),
        )
    except Exception as err:  # skipcq PYL-W0703
        # log the real detail server-side only — the progress entry is polled by the public
        # /trial/status endpoint and must not leak internal exception text (M2). Covers a
        # SoftTimeLimitExceeded too (the clone ran past CLONE_SOFT_TIME_LIMIT).
        logger.error(f"trial clone failed for {email}: {err}")
        progress.add({"message": "clone failed", "status": "failed"})
        return
    finally:
        release_clone_lock(email)

    progress.add({"message": "done", "status": "completed", "org_slug": run.trial_org.slug})

    # welcome email so the user gets in even if they closed the progress tab mid-clone
    try:
        login_url = settings.FRONTEND_URL_V2 or ""
        awsses.send_trial_welcome_email(email, login_url)
    except Exception as err:  # skipcq PYL-W0703 — email failure must not fail the (done) clone
        logger.error(f"failed to send trial welcome email to {email}: {err}")

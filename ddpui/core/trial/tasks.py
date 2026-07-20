"""Celery task wrapping clone_template_org with Redis-backed progress reporting."""

from ddpui.celery import app
from ddpui.utils.taskprogress import TaskProgress
from ddpui.core.trial.clone_service import clone_template_org
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui.core.trial.tasks")


@app.task(bind=True)
def clone_trial_org_task(self, task_id, template_org_id, email, org_name, role):
    """Run clone_template_org for a free-trial signup, reporting progress into Redis."""
    hashkey = f"trial-clone-{task_id}"
    progress = TaskProgress(task_id, hashkey)
    progress.add({"message": "queued", "status": "queued"})

    try:
        run = clone_template_org(
            template_org_id,
            email,
            org_name=org_name,
            role_slug=role,
            progress=lambda n, label: progress.add(
                {"step": n, "message": label, "status": "running"}
            ),
        )
    except Exception as err:  # skipcq PYL-W0703
        logger.error(f"clone_trial_org_task failed for {email}: {err}")
        progress.add({"message": str(err), "status": "failed"})
        return

    progress.add({"message": "done", "status": "completed", "org_slug": run.trial_org.slug})

from ninja import NinjaAPI
from ninja.errors import HttpError
from ddpui.utils.taskprogress import TaskProgress
from ddpui.utils.singletaskprogress import SingleTaskProgress

from ddpui.auth import has_permission
from ddpui import auth

taskapi = NinjaAPI(urls_namespace="tasks")


@taskapi.get("/{task_id}", auth=auth.CustomAuthMiddleware())
@has_permission(["can_view_task_progress"])
def get_task(
    request, task_id, hashkey: str = "taskprogress"
):  # pylint: disable=unused-argument
    """returns the progress for a celery task"""
    result = TaskProgress.fetch(task_id=task_id, hashkey=hashkey)
    if result:
        return {"progress": result}
    raise HttpError(400, "no such task id")


@taskapi.get("/stp/{task_key}", auth=auth.CustomAuthMiddleware())
@has_permission(["can_view_task_progress"])
def get_singletask(request, task_key):  # pylint: disable=unused-argument
    """returns the progress for a celery task"""
    result = SingleTaskProgress.fetch(task_key=task_key)
    if result is not None:
        return {"progress": result}
    raise HttpError(400, "no such task id")

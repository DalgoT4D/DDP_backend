from ninja import Router
from ninja.errors import HttpError
from ddpui.utils.taskprogress import TaskProgress
from ddpui.utils.singletaskprogress import SingleTaskProgress

from ddpui.auth import has_permission
from ddpui import auth
from celery.result import AsyncResult

task_router = Router(auth=auth.CustomAuthMiddleware())


@task_router.get("/{task_id}")
@has_permission(["can_view_task_progress"])
def get_task(request, task_id, hashkey: str = "taskprogress"):  # pylint: disable=unused-argument
    """returns the progress for a celery task"""
    result = TaskProgress.fetch(task_id=task_id, hashkey=hashkey)
    if result:
        return {"progress": result}
    raise HttpError(400, "no such task id")


@task_router.get("/stp/{task_key}")
@has_permission(["can_view_task_progress"])
def get_singletask(request, task_key):  # pylint: disable=unused-argument
    """returns the progress for a celery task"""
    result = SingleTaskProgress.fetch(task_key=task_key)
    if result is not None:
        return {"progress": result}
    raise HttpError(400, "no such task id")


@task_router.get("/celery/{task_id}")
@has_permission(["can_view_task_progress"])
def get_celerytask(request, task_id):  # pylint: disable=unused-argument
    """Get the celery task progress and not the one we create in redis"""
    task_result = AsyncResult(task_id)
    result = {
        "id": task_id,
        "status": task_result.status,
        "result": task_result.result,
        "error": str(task_result.info) if task_result.info else None,
    }
    return result

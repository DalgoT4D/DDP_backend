from ninja import Router
from ninja.errors import HttpError
from ddpui.utils.taskprogress import TaskProgress
from ddpui.utils.singletaskprogress import SingleTaskProgress

from ddpui.auth import has_permission
from ddpui import auth
from celery.result import AsyncResult

task_router = Router()


@task_router.get("/{task_id}")
@has_permission(["can_view_task_progress"])
def get_task(request, task_id, hashkey: str = "taskprogress"):  # pylint: disable=unused-argument
    """
    Retrieve progress information for a task stored in Redis.

    Fetches the current progress status for a task using the custom TaskProgress
    system that stores progress information in Redis with a specific hash key.

    Args:
        request: HTTP request object containing orguser authentication data
        task_id (str): Unique identifier of the task
        hashkey (str, optional): Redis hash key for task progress. Defaults to "taskprogress"

    Returns:
        dict: Task progress information including status and progress details

    Raises:
        HttpError: 400 if no task found with the specified ID
        HttpError: 403 if user lacks permission to view task progress
    """
    result = TaskProgress.fetch(task_id=task_id, hashkey=hashkey)
    if result:
        return {"progress": result}
    raise HttpError(400, "no such task id")


@task_router.get("/stp/{task_key}")
@has_permission(["can_view_task_progress"])
def get_singletask(request, task_key):  # pylint: disable=unused-argument
    """
    Retrieve progress information for a single task using a task key.

    Fetches progress status for tasks managed by the SingleTaskProgress system,
    which handles individual task tracking with unique task keys.

    Args:
        request: HTTP request object containing orguser authentication data
        task_key (str): Unique key identifier for the single task

    Returns:
        dict: Task progress information including current status and progress data

    Raises:
        HttpError: 400 if no task found with the specified key
        HttpError: 403 if user lacks permission to view task progress
    """
    result = SingleTaskProgress.fetch(task_key=task_key)
    if result is not None:
        return {"progress": result}
    raise HttpError(400, "no such task id")


@task_router.get("/celery/{task_id}")
@has_permission(["can_view_task_progress"])
def get_celerytask(request, task_id):  # pylint: disable=unused-argument
    """
    Get the native Celery task progress and status information.

    Retrieves task status directly from Celery's result backend rather than
    the custom Redis-based progress tracking system. Provides access to
    Celery's built-in task state management.

    Args:
        request: HTTP request object containing orguser authentication data
        task_id (str): Celery task ID to query

    Returns:
        dict: Celery task information including:
            - id: Task ID
            - status: Current task status (PENDING, SUCCESS, FAILURE, etc.)
            - result: Task result if completed
            - error: Error information if task failed

    Raises:
        HttpError: 403 if user lacks permission to view task progress
    """
    task_result = AsyncResult(task_id)
    result = {
        "id": task_id,
        "status": task_result.status,
        "result": task_result.result,
        "error": str(task_result.info) if task_result.info else None,
    }
    return result

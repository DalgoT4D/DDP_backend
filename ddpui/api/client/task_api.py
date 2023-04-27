from ninja import NinjaAPI
from ninja.errors import HttpError
from ddpui.utils.taskprogress import TaskProgress

taskapi = NinjaAPI(urls_namespace="tasks")

@taskapi.get('/{task_id}')
def get_task(request, task_id): # pylint: disable=unused-argument
    """returns the progress for a celery task"""
    result = TaskProgress.fetch(task_id)
    if result:
        return {"progress": result}
    raise HttpError(400, "no such task id")

from ddpui.celery import app
from ddpui.models.org import Org
from ddpui.models.tasks import (
    TaskProgressStatus,
)
from ddpui.utils.singletaskprogress import SingleTaskProgress
from ddpui.ddpairbyte.deleteconnection import delete_org_connection
from ddpui.utils.redis_client import RedisClient


@app.task(bind=False)
def delete_airbyte_connections(task_key: str, org_id, connection_ids: list[str]):
    """
    Deletes a set of Airbyte connections and related Dalgo artifacts asynchronously.

    Args:
        task_key (str): Unique key for progress tracking
        org_id: Organization ID to which connections belong
        connection_ids (list[str]): List of Airbyte connection IDs to delete

    Raises:
        Org.DoesNotExist: If organization with given ID doesn't exist
    """
    if not connection_ids:
        return

    if not isinstance(connection_ids, list):
        raise ValueError("connection_ids must be a list")

    org = Org.objects.get(id=org_id)
    # the task_key is keyed by org only
    taskprogress = SingleTaskProgress(task_key, 180)
    taskprogress.add({"message": "started", "status": TaskProgressStatus.RUNNING, "result": None})
    redisclient = RedisClient.get_instance()
    for connection_id in connection_ids:
        try:
            delete_org_connection(org, connection_id)
            redisclient.delete(f"deleting-{connection_id}")
            taskprogress.add(
                {
                    "message": f"connection {connection_id} deleted",
                    "status": TaskProgressStatus.RUNNING,
                    "result": {},
                }
            )
        except Exception as err:
            taskprogress.add(
                {
                    "message": f"Failed to delete connection {connection_id}: {str(err)}",
                    "status": TaskProgressStatus.RUNNING,
                    "result": {"error": str(err)},
                }
            )
    taskprogress.add(
        {"message": "all connections deleted", "status": TaskProgressStatus.COMPLETED, "result": {}}
    )

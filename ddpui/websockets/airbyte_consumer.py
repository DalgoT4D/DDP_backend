import json

from ddpui.celeryworkers.tasks import get_schema_catalog_task
from ddpui.models.org_user import OrgUser
from ddpui.models.tasks import TaskProgressHashPrefix, TaskProgressStatus
from ddpui.utils.custom_logger import CustomLogger
from ddpui.utils.singletaskprogress import SingleTaskProgress
from ddpui.websockets.schemas import WebsocketResponse, WebsocketResponseStatus
from ddpui.ddpairbyte import airbyte_service, airbytehelpers
from ddpui.models.org import OrgType
from ddpui.ddpairbyte.schema import (
    AirbyteSourceCreate,
    AirbyteSourceUpdateCheckConnection,
    AirbyteDestinationCreate,
    AirbyteDestinationUpdateCheckConnection,
)
from ddpui.websockets import BaseConsumer

import time

logger = CustomLogger("ddpui")


class SourceCheckConnectionConsumer(BaseConsumer):
    """websocket for checking source connection"""

    def websocket_receive(self, message):
        logger.info("Recieved the message from client, inside check connection for source consumer")
        payload = json.loads(message["text"])
        source_id = payload.get("sourceId", None)
        if "sourceId" in payload:
            del payload["sourceId"]
        if source_id:
            payload = AirbyteSourceUpdateCheckConnection(**payload)
        else:
            payload = AirbyteSourceCreate(**payload)

        if self.orguser.org.base_plan() == OrgType.DEMO:
            logger.info("Demo account user")
            source_def = airbyte_service.get_source_definition(
                self.orguser.org.airbyte_workspace_id, payload.sourceDefId
            )
            # replace the payload config with the correct whitelisted source config
            whitelisted_config, error = airbytehelpers.get_demo_whitelisted_source_config(
                source_def["name"]
            )
            if error:
                self.respond(
                    WebsocketResponse(
                        data={},
                        message="Error in getting whitelisted source config",
                        status=WebsocketResponseStatus.ERROR,
                    )
                )
                return

            payload.config = whitelisted_config
            logger.info("whitelisted the source config")

        try:
            if source_id:
                response = airbyte_service.check_source_connection_for_update(source_id, payload)
            else:
                response = airbyte_service.check_source_connection(
                    self.orguser.org.airbyte_workspace_id, payload
                )
        except Exception:
            self.respond(
                WebsocketResponse(
                    data={},
                    message="Invalid credentials",
                    status=WebsocketResponseStatus.ERROR,
                )
            )
            return
        self.respond(
            WebsocketResponse(
                data={
                    "status": ("succeeded" if response["jobInfo"]["succeeded"] else "failed"),
                    "logs": response["jobInfo"]["logs"]["logLines"],
                },
                message="Source connection check completed",
                status=WebsocketResponseStatus.SUCCESS,
            )
        )


class DestinationCheckConnectionConsumer(BaseConsumer):
    """websocket for checking destination connection"""

    def websocket_receive(self, message):
        logger.info(
            "Recieved the message from client, inside check connection for destination consumer"
        )
        payload = json.loads(message["text"])
        destination_id = payload.get("destinationId", None)
        if "destinationId" in payload:
            del payload["destinationId"]
        if destination_id:
            payload = AirbyteDestinationUpdateCheckConnection(**payload)
        else:
            payload = AirbyteDestinationCreate(**payload)

        try:
            if destination_id:
                response = airbyte_service.check_destination_connection_for_update(
                    destination_id, payload
                )
            else:
                response = airbyte_service.check_destination_connection(
                    self.orguser.org.airbyte_workspace_id, payload
                )
        except Exception:
            self.respond(
                WebsocketResponse(
                    data={},
                    message="Invalid credentials",
                    status=WebsocketResponseStatus.ERROR,
                )
            )
            return
        self.respond(
            WebsocketResponse(
                data={
                    "status": ("succeeded" if response["jobInfo"]["succeeded"] else "failed"),
                    "logs": response["jobInfo"]["logs"]["logLines"],
                },
                message="Destination connection check completed",
                status=WebsocketResponseStatus.SUCCESS,
            )
        )


class SchemaCatalogConsumer(BaseConsumer):
    """websocket for checking source schema_catalog"""

    def websocket_receive(self, message):
        logger.info("Recieved the message from client, get schema_catalog inside the connection")
        payload = json.loads(message["text"])

        orguser: OrgUser = self.orguser
        if orguser.org.airbyte_workspace_id is None:
            self.respond(
                WebsocketResponse(
                    data={},
                    message="Create an airbyte workspace first.",
                    status=WebsocketResponseStatus.ERROR,
                )
            )
            return

        if "sourceId" not in payload:
            self.respond(
                WebsocketResponse(
                    data={},
                    message="SourceId is required in the payload",
                    status=WebsocketResponseStatus.ERROR,
                )
            )
            return
        source_id = payload.get("sourceId", None)

        # creating a key and checking if it exists or not
        task_key = f"{TaskProgressHashPrefix.SOURCE_SCHEMA_CATALOG}-{orguser.org.slug}-{source_id}"  # change this schema change to catalog
        print(task_key, "taskkey ")
        task_progress = SingleTaskProgress.fetch(task_key)

        if task_progress is not None:
            polling_celery(self, task_key)
            return

        # creating a celery task now #.delay gives this task to celery
        get_schema_catalog_task.delay(task_key, str(orguser.org.airbyte_workspace_id), source_id)
        time.sleep(2)
        polling_celery(self, task_key)


def polling_celery(self, task_key):
    task_progress = SingleTaskProgress.fetch(task_key)
    if task_progress is None:
        self.respond(
            WebsocketResponse(
                data={},
                message="No Task of this task_key found",
                status=WebsocketResponseStatus.ERROR,
            )
        )
        return

    last_status = task_progress[-1]["status"]

    while last_status == TaskProgressStatus.RUNNING:
        task_progress = SingleTaskProgress.fetch(task_key)
        last_status = None if task_progress is None else task_progress[-1]["status"]
        time.sleep(2)

    if last_status == TaskProgressStatus.FAILED:
        self.respond(
            WebsocketResponse(
                data={},
                message="Invalid credentials",
                status=WebsocketResponseStatus.ERROR,
            )
        )
    elif last_status == TaskProgressStatus.COMPLETED:
        self.respond(
            WebsocketResponse(
                data={
                    "status": last_status,
                    "result": task_progress[-1]["result"],
                },
                message="Destination connection check completed",
                status=WebsocketResponseStatus.SUCCESS,
            )
        )
    else:
        self.respond(
            WebsocketResponse(
                data={},
                message="No task found",
                status=WebsocketResponseStatus.ERROR,
            )
        )
    return

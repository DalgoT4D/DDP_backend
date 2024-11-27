import json

from ddpui.utils.custom_logger import CustomLogger
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

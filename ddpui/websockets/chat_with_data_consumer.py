import json
import uuid

from django.forms.models import model_to_dict
from ddpui.utils.custom_logger import CustomLogger
from ddpui.websockets import BaseConsumer
from ddpui.websockets.schemas import WebsocketResponse, WebsocketResponseStatus
from ddpui.schemas.chat_with_data_schemas import (
    FetchChatMessagesRequest,
    StartThreadRequest,
    AskChatWithDataBotRequest,
    CloseThreadRequest,
    GenerateSqlAndStartThreadRequest,
)
from ddpui.core.warehousefunctions import parse_sql_query_with_limit
from ddpui.datainsights.warehouse.warehouse_factory import WarehouseFactory
from ddpui.models.org import OrgWarehouse
from ddpui.models.chat_with_data import Thread, Message, MessageType
from ddpui.models.llm import LlmAssistantType, AssistantPrompt
from ddpui.utils import secretsmanager
from ddpui.core import llm_service
from ddpui.core.sqlgeneration_service import SqlGeneration
from ddpui.utils.helpers import (
    convert_sqlalchemy_rows_to_csv_string,
)


logger = CustomLogger("ddpui")


class ChatWithDataBotConsumer(BaseConsumer):
    """chat with data consumer"""

    def websocket_receive(self, message):
        logger.info("Recieved the message from client, inside chat with data consumer")
        payload: dict = json.loads(message["text"])
        logger.info(f"Payload: {payload}")

        action = payload.get("action", None)
        params = payload.get("params", {})
        if action:
            method = getattr(self, action, None)
            if method:
                try:
                    if action == "get_messages":
                        params = FetchChatMessagesRequest(**params)
                    elif action == "start_thread":
                        params = StartThreadRequest(**params)
                    elif action == "ask_bot":
                        params = AskChatWithDataBotRequest(**params)
                    elif action == "close_thread":
                        params = CloseThreadRequest(**params)
                    elif action == "generate_sql_and_start_thread":
                        params = GenerateSqlAndStartThreadRequest(**params)
                    method(params)
                except Exception as e:
                    logger.error(f"Error in calling the method: {e}")
                    self.respond(
                        WebsocketResponse(
                            data={},
                            message=f"Error in calling the method: {e}",
                            status=WebsocketResponseStatus.ERROR,
                        )
                    )
            else:
                self.respond(
                    WebsocketResponse(
                        data={},
                        message="Invalid action",
                        status=WebsocketResponseStatus.ERROR,
                    )
                )
        else:
            self.respond(
                WebsocketResponse(
                    data={},
                    message="No action specified in the payload",
                    status=WebsocketResponseStatus.ERROR,
                )
            )

    def get_messages(self, payload: FetchChatMessagesRequest):
        """Get messages for a thread (paginated)"""
        messages = Message.objects.filter(thread__uuid=payload.thread_uuid).order_by("-created_at")[
            payload.offset : payload.limit
        ]
        self.respond(
            WebsocketResponse(
                data={
                    "messages": [
                        {
                            **model_to_dict(
                                message, exclude=["thread_id", "id", "created_at", "updated_at"]
                            ),
                            "created_at": str(message.created_at),
                            "updated_at": str(message.updated_at),
                        }
                        for message in messages
                    ]
                },
                message="Messages fetched successfully",
                status=WebsocketResponseStatus.SUCCESS,
            )
        )

    @classmethod
    def start_thread(self, payload: StartThreadRequest):
        """
        - Filters the data from a sql query
        - Starts a session by sending the results of sql query to llm service
        - Creates a thread with the session_id
        You can pass any meta information to save to the thread being created
        """
        org_warehouse = OrgWarehouse.objects.filter(org=self.orguser.org).first()
        credentials = secretsmanager.retrieve_warehouse_credentials(org_warehouse)

        try:
            wclient = WarehouseFactory.connect(credentials, wtype=org_warehouse.wtype)
        except Exception as err:
            logger.error("Failed to connect to the warehouse - %s", err)
            self.respond(
                WebsocketResponse(
                    data={},
                    message=f"Failed to connect to the warehouse {err}",
                    status=WebsocketResponseStatus.ERROR,
                )
            )

        # fetch the results of the query
        sql = parse_sql_query_with_limit(payload.sql)
        logger.info(f"Submitting query to warehouse for execution \n '''{sql}'''")
        rows = []
        try:
            rows = wclient.execute(sql)
        except Exception as err:
            logger.error(err)
            return

        fpath, session_id = llm_service.upload_text_as_file(
            convert_sqlalchemy_rows_to_csv_string(rows), "warehouse_results"
        )
        logger.info("Uploaded file successfully to LLM service at " + str(fpath))
        logger.info("Session ID: " + session_id)

        # create the thread
        thread = Thread.objects.create(
            uuid=uuid.uuid4(),
            orguser=self.orguser,
            session_id=session_id,
            meta={"sql": payload.sql, **payload.meta},
        )

        self.respond(
            WebsocketResponse(
                data={"thread_uuid": str(thread.uuid)},
                message="Thread created successfully",
                status=WebsocketResponseStatus.SUCCESS,
            )
        )

    def ask_bot(self, payload: AskChatWithDataBotRequest):
        """
        - Create a message for the user sending/asking the bot
        - Generate the response via llm service using the session_id in thread
        - Create a message for the bot sending the response
        """

        # fetch the thread
        thread: Thread = Thread.objects.filter(uuid=payload.thread_uuid).first()
        if not thread:
            self.respond(
                WebsocketResponse(
                    data={},
                    message="Thread not found",
                    status=WebsocketResponseStatus.ERROR,
                )
            )
            return

        # create user message
        user_message = Message.objects.create(
            content=payload.message,
            sender=self.orguser,
            thread=thread,
            type=MessageType.HUMAN.value,
        )

        # generate response
        assistant_prompt = AssistantPrompt.objects.filter(
            type=LlmAssistantType.CHAT_WITH_DATA_ASSISTANT
        ).first()
        if not assistant_prompt:
            raise Exception("Assistant/System prompt not found for chat with data bot")

        ai_result = llm_service.file_search_query_and_poll(
            assistant_prompt=assistant_prompt.prompt,
            queries=[user_message.content],
            session_id=thread.session_id,
        )

        if len(ai_result["result"]) == 0:
            raise Exception("No response from llm service")

        # create ai message
        ai_message = Message.objects.create(
            content=ai_result["result"][0],
            thread=thread,
            type=MessageType.AI.value,
        )

        self.respond(
            WebsocketResponse(
                data={"response": ai_message.content},
                message="Response generated by bot successfully",
                status=WebsocketResponseStatus.SUCCESS,
            )
        )

    def get_threads(self, payload: dict):
        """
        Fetch all the threads started by the authenticated orguser
        """
        threads = Thread.objects.filter(orguser=self.orguser).all()
        self.respond(
            WebsocketResponse(
                data={
                    "threads": [
                        {
                            **model_to_dict(
                                thread, exclude=["id", "created_at", "updated_at", "orguser"]
                            ),
                            "uuid": str(thread.uuid),
                            "created_at": str(thread.created_at),
                            "updated_at": str(thread.updated_at),
                        }
                        for thread in threads
                    ]
                },
                message="Threads fetched successfully",
                status=WebsocketResponseStatus.SUCCESS,
            )
        )

    def close_thread(self, payload: CloseThreadRequest):
        """
        Close the thread
        """
        thread = Thread.objects.filter(uuid=payload.thread_uuid).first()
        if thread:
            llm_service.close_file_search_session(thread.session_id)
            thread.delete()
            self.respond(
                WebsocketResponse(
                    data={},
                    message="Thread closed successfully",
                    status=WebsocketResponseStatus.SUCCESS,
                )
            )
        else:
            self.respond(
                WebsocketResponse(
                    data={},
                    message="Thread not found",
                    status=WebsocketResponseStatus.ERROR,
                )
            )

    def generate_sql_and_start_thread(self, payload: GenerateSqlAndStartThreadRequest):
        """
        - Generate sql query from the user input/prompt
        - If the successful, create/start a thread.
        This assumes the model for sql generation is trained before hand
        """
        warehouse = OrgWarehouse.objects.filter(org=self.orguser.org).first()

        if not warehouse:
            self.respond(
                WebsocketResponse(
                    data={},
                    message="No warehouse found for the org",
                    status=WebsocketResponseStatus.ERROR,
                )
            )
            return

        sqlgeneration_client = SqlGeneration(warehouse)

        sql = sqlgeneration_client.generate_sql(payload.user_prompt)
        if not sql:
            self.respond(
                WebsocketResponse(
                    data={},
                    message="Failed to generate sql query",
                    status=WebsocketResponseStatus.ERROR,
                )
            )
            return

        logger.info(f"Generated SQL: {sql}")
        self.start_thread(StartThreadRequest(sql=sql, meta={"user_prompt": payload.user_prompt}))

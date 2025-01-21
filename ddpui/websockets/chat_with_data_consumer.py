import json
import uuid

from django.forms.models import model_to_dict
from ddpui.utils.custom_logger import CustomLogger
from ddpui.models.org_user import OrgUser
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
from ddpui.models.chat_with_data import Thread, Message, MessageType, ThreadStatus
from ddpui.models.llm import LlmAssistantType, AssistantPrompt
from ddpui.utils import secretsmanager
from ddpui.core import llm_service
from ddpui.core.chatwithdata.sqlgeneration_service import SqlGeneration
from ddpui.utils.helpers import (
    convert_sqlalchemy_rows_to_csv_string,
)
from sqlalchemy.sql import text


logger = CustomLogger("ddpui")


class ChatWithDataBotConsumer(BaseConsumer):
    """chat with data consumer"""

    @staticmethod
    def get_all_threads(orguser: OrgUser):
        """
        Fetch all(open & close) the threads started by the authenticated orguser
        """
        threads = Thread.objects.filter(orguser=orguser).order_by("-created_at").all()

        return {
            "threads": [
                {
                    **model_to_dict(thread, exclude=["id", "created_at", "updated_at", "orguser"]),
                    "uuid": str(thread.uuid),
                    "created_at": str(thread.created_at),
                    "updated_at": str(thread.updated_at),
                }
                for thread in threads
            ]
        }

    @staticmethod
    def create_thread(orguser: OrgUser, payload: StartThreadRequest, return_threads=False):
        """
        - Filters the data from a sql query
        - Starts a session by sending the results of sql query to llm service
        - Creates a thread with the session_id
        You can pass any meta information to save to the thread being created
        """
        org_warehouse = OrgWarehouse.objects.filter(org=orguser.org).first()
        credentials = secretsmanager.retrieve_warehouse_credentials(org_warehouse)

        try:
            wclient = WarehouseFactory.connect(credentials, wtype=org_warehouse.wtype)
        except Exception as err:
            logger.error("Failed to connect to the warehouse - %s", err)
            raise Exception(f"Failed to connect to the warehouse {err}")

        # fetch the results of the query
        sql = parse_sql_query_with_limit(payload.sql)
        logger.info(f"Submitting query to warehouse for execution \n '''{sql}'''")
        rows = []
        try:
            rows = wclient.execute(text(sql))
            if len(rows) == 0:
                logger.error("No results found for the query")
                raise Exception("No results found for the query")
        except Exception as err:
            logger.error(f"Failed to execute the query: {err}")
            raise Exception(f"Failed to execute the query: {err}")

        fpath, session_id = llm_service.upload_text_as_file(
            convert_sqlalchemy_rows_to_csv_string(rows), "warehouse_results"
        )
        logger.info("Uploaded file successfully to LLM service at " + str(fpath))
        logger.info("Session ID: " + session_id)

        # create the thread
        thread = Thread.objects.create(
            uuid=uuid.uuid4(),
            orguser=orguser,
            session_id=session_id,
            meta={"sql": payload.sql, **payload.meta},
        )

        res = {"thread_uuid": str(thread.uuid)}

        if return_threads:
            res = ChatWithDataBotConsumer.get_all_threads(orguser)

        return res

    @staticmethod
    def ask_bot_prompt(orguser: OrgUser, payload: AskChatWithDataBotRequest):
        """
        - Create a message for the user sending/asking the bot
        - Generate the response via llm service using the session_id in thread
        - Create a message for the bot sending the response
        """

        # fetch the thread
        thread: Thread = Thread.objects.filter(uuid=payload.thread_uuid).first()
        if not thread:
            raise Exception("Thread not found")

        # create user message
        user_message = Message.objects.create(
            content=payload.message,
            sender=orguser,
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

        return {"response": ai_message.content}

    def websocket_receive(self, message):
        logger.info("Recieved the message from client, inside chat with data consumer")
        payload: dict = json.loads(message["text"])
        logger.info(f"Payload: {payload}")

        action = payload.get("action", None)
        params = payload.get("params", {})
        if action:
            method = getattr(self, action, None)
            logger.info(f"Action found {method}")
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
                            **model_to_dict(message, exclude=["thread_id", "updated_at"]),
                            "created_at": str(message.created_at),
                            "updated_at": str(message.updated_at),
                        }
                        for message in messages
                    ][::-1]
                },
                message="Messages fetched successfully",
                status=WebsocketResponseStatus.SUCCESS,
            )
        )

    def start_thread(self, payload: StartThreadRequest, return_threads=False):
        """
        - Filters the data from a sql query
        - Starts a session by sending the results of sql query to llm service
        - Creates a thread with the session_id
        You can pass any meta information to save to the thread being created
        """
        try:
            res = self.create_thread(self.orguser, payload, return_threads)
            self.respond(
                WebsocketResponse(
                    data=res,
                    message="Thread created successfully",
                    status=WebsocketResponseStatus.SUCCESS,
                )
            )
        except Exception as e:
            self.respond(
                WebsocketResponse(
                    data={},
                    message=f"Failed to create thread: {e}",
                    status=WebsocketResponseStatus.ERROR,
                )
            )

    def ask_bot(self, payload: AskChatWithDataBotRequest):
        """
        - Create a message for the user sending/asking the bot
        - Generate the response via llm service using the session_id in thread
        - Create a message for the bot sending the response
        """

        try:
            res = self.ask_bot_prompt(self.orguser, payload)
            self.respond(
                WebsocketResponse(
                    data=res,
                    message="Response generated by bot successfully",
                    status=WebsocketResponseStatus.SUCCESS,
                )
            )
        except Exception as e:
            self.respond(
                WebsocketResponse(
                    data={},
                    message=f"Failed to generate response: {e}",
                    status=WebsocketResponseStatus.ERROR,
                )
            )

    def get_threads(self, payload: dict):
        """
        Fetch all the threads started by the authenticated orguser
        """
        self.respond(
            WebsocketResponse(
                data=self.get_all_threads(self.orguser),
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
            thread.status = ThreadStatus.CLOSE.value
            thread.save()
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
        logger.info(f"Generated SQL: {sql}")

        if not sql:
            self.respond(
                WebsocketResponse(
                    data={},
                    message="Failed to generate sql query",
                    status=WebsocketResponseStatus.ERROR,
                )
            )
            return

        cleaned_sql = sql.replace("\n", " ").replace(";", " ")
        logger.info(f"Cleaned sql: {cleaned_sql}")

        # create the thread
        thread_res = ChatWithDataBotConsumer.create_thread(
            self.orguser,
            StartThreadRequest(sql=cleaned_sql, meta={"user_prompt": payload.user_prompt}),
            return_threads=False,
        )

        # run the thread with that current prompt
        ChatWithDataBotConsumer.ask_bot_prompt(
            self.orguser,
            AskChatWithDataBotRequest(
                message=payload.user_prompt, thread_uuid=thread_res["thread_uuid"]
            ),
        )

        # respond with new list of threads
        threads = ChatWithDataBotConsumer.get_all_threads(self.orguser)
        self.respond(
            WebsocketResponse(
                data=threads,
                message="Thread started successfully",
                status=WebsocketResponseStatus.SUCCESS,
            )
        )

import json
from channels.generic.websocket import WebsocketConsumer
from rest_framework.authtoken.models import Token
from urllib.parse import parse_qs

from ddpui.websockets.schemas import WebsocketResponse
from ddpui.models.org_user import OrgUser
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui")


class BaseConsumer(WebsocketConsumer):
    def authenticate_user(self, token: str, orgslug: str):
        self.orguser = None
        self.user = None
        tokenrecord = Token.objects.filter(key=token).first()
        if tokenrecord and tokenrecord.user:
            self.user = tokenrecord.user
            q_orguser = OrgUser.objects.filter(user=self.user)
            if orgslug:
                q_orguser = q_orguser.filter(org__slug=orgslug)
            orguser = q_orguser.first()
            if orguser is not None:
                self.orguser = orguser
                return True
        return False

    def respond(self, message: WebsocketResponse):
        self.send(text_data=json.dumps(message.dict()))

    def connect(self):
        query_string = parse_qs(self.scope["query_string"].decode())
        token = query_string.get("token", [None])[0]
        orgslug = query_string.get("orgslug", [None])[0]

        if self.authenticate_user(token, orgslug):
            logger.info("User authenticated, establishing connection")
            self.accept()
        else:
            logger.info("Authentication failed, closing connection")
            self.close()

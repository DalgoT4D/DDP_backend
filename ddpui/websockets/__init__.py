import json
from channels.generic.websocket import WebsocketConsumer
from rest_framework_simplejwt.tokens import AccessToken
from urllib.parse import parse_qs
from django.contrib.auth.models import User

from ddpui.websockets.schemas import WebsocketResponse
from ddpui.models.org_user import OrgUser
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui")


class BaseConsumer(WebsocketConsumer):
    def authenticate_user(self, token: str, orgslug: str):
        """Authenticate user using JWT token"""
        self.orguser = None
        self.user = None

        try:
            # Validate and decode JWT using SimpleJWT's AccessToken
            access_token = AccessToken(token)
            token_payload = access_token.payload

            user_id = token_payload.get("user_id")
            if not user_id:
                return False

            # Get the user from the token payload
            user = User.objects.filter(id=user_id).first()
            if not user:
                return False

            self.user = user

            # Find the orguser
            q_orguser = OrgUser.objects.filter(user=self.user)
            if orgslug:
                q_orguser = q_orguser.filter(org__slug=orgslug)

            orguser = q_orguser.first()
            if orguser is not None:
                self.orguser = orguser
                return True
            else:
                return False

        except Exception as err:
            return False

    def respond(self, message: WebsocketResponse):
        self.send(text_data=json.dumps(message.dict()))

    def connect(self):
        query_string = parse_qs(self.scope["query_string"].decode())
        token = query_string.get("token", [None])[0]
        orgslug = query_string.get("orgslug", [None])[0]

        if self.authenticate_user(token, orgslug):
            self.accept()
        else:
            self.close()

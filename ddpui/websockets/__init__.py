import json
from http.cookies import SimpleCookie
from channels.generic.websocket import WebsocketConsumer
from rest_framework_simplejwt.tokens import AccessToken
from urllib.parse import parse_qs
from django.contrib.auth.models import User

from ddpui.websockets.schemas import WebsocketResponse, WebsocketCloseCodes
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
                logger.error("No user_id found in JWT token payload")
                return False

            # Get the user from the token payload
            user = User.objects.filter(id=user_id).first()
            if not user:
                logger.error(f"User with id {user_id} not found")
                return False

            self.user = user

            # Find the orguser
            q_orguser = OrgUser.objects.filter(user=self.user)
            if orgslug:
                q_orguser = q_orguser.filter(org__slug=orgslug)

            orguser = q_orguser.first()
            if orguser is not None:
                self.orguser = orguser
                logger.info(f"JWT authentication successful for user {user.email}")
                return True
            else:
                logger.error(f"No orguser found for user {user.email} with orgslug {orgslug}")
                return False

        except Exception as err:
            logger.error(f"JWT authentication failed: {err}")
            return False

    def respond(self, message: WebsocketResponse):
        self.send(text_data=json.dumps(message.model_dump()))

    def _get_cookie(self, name: str) -> str | None:
        """Extract a cookie value from the WebSocket scope headers."""
        for header_name, header_value in self.scope.get("headers", []):
            if header_name == b"cookie":
                cookie = SimpleCookie(header_value.decode())
                if name in cookie:
                    return cookie[name].value
        return None

    def connect(self):
        query_string = parse_qs(self.scope["query_string"].decode())
        orgslug = query_string.get("orgslug", [None])[0]

        # Read JWT from the access_token httpOnly cookie
        token = self._get_cookie("access_token")

        if not token:
            logger.info("No access_token cookie found, closing connection")
            self.accept()
            self.close(code=WebsocketCloseCodes.NO_TOKEN)
        elif not self.authenticate_user(token, orgslug):
            logger.info("Authentication failed (invalid/expired token), closing connection")
            self.accept()
            self.close(code=WebsocketCloseCodes.INVALID_TOKEN)
        else:
            logger.info("User authenticated via cookie, establishing connection")
            self.accept()

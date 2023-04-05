from ninja.security import HttpBearer

from rest_framework.authentication import TokenAuthentication
from rest_framework.authtoken.models import Token


class AuthBearer(HttpBearer):
    """ninja middleware to look up a user from an auth token"""

    def authenticate(self, request, token):
        tokenrecord = Token.objects.filter(key=token).first()
        if tokenrecord:
            request.user = tokenrecord.user
            return request.user
        return None


class BearerAuthentication(TokenAuthentication):
    """This allows us to send the Authorization header "Bearer <token>" instead of "Token <token>" """

    keyword = "Bearer"

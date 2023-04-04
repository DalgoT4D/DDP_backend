from ninja import Schema
from ninja.security import HttpBearer
from ninja.errors import HttpError

from ddpui.models.org_user import OrgUser
from ddpui.models.admin_user import AdminUser


class LoginData(Schema):
    """Docstring"""

    email: str
    password: str


def stub_lookup_org_user_by_token(token):
    """Docstring"""
    user = None
    if token.find("fake-auth-token:") == 0:
        token = token[len("fake-auth-token:") :]
        try:
            userid = int(token)
        except ValueError:
            raise HttpError(400, "invalid token")
        user = OrgUser.objects.filter(id=userid).first()
    if user is None:
        raise HttpError(400, "invalid token")
    return user


def stub_lookup_admin_user_by_token(token):
    """Docstring"""
    user = None
    if token.find("fake-admin-auth-token:") == 0:
        token = token[len("fake-admin-auth-token:") :]
        try:
            userid = int(token)
        except ValueError:
            raise HttpError(400, "invalid token")
        user = AdminUser.objects.filter(id=userid).first()
    if user is None:
        raise HttpError(400, "invalid token")
    return user


class UserAuthBearer(HttpBearer):
    """Docstring"""

    def authenticate(self, request, token):
        user = stub_lookup_org_user_by_token(token)
        return user


class AdminAuthBearer(HttpBearer):
    """Docstring"""

    def authenticate(self, request, token):
        user = stub_lookup_admin_user_by_token(token)
        return user

from unittest.mock import Mock
from faker import Faker
import pytest
from django.contrib.auth.models import User
from ddpui.auth import (
    Token,
    OrgUser,
)
from ddpui.models.org import Org
from ddpui.models.org_user import OrgUserRole

pytestmark = pytest.mark.django_db


def create_user():
    faker = Faker()
    return User.objects.create(
        username=faker.user_name(), email=faker.email(), password=faker.password()
    )


@pytest.fixture
def user():
    temp_user = create_user()
    yield temp_user
    temp_user.delete()


def token(user: User):
    temp_token = Token.objects.create(key="ttt", user=user)
    return temp_token


@pytest.fixture
def org():
    temp_org = Org.objects.create(name="temp-org", slug="temp-org")
    yield temp_org
    temp_org.delete()


def create_accountmanager(user: User, org: Org):
    return OrgUser.objects.create(user=user, org=org, role=OrgUserRole.ACCOUNT_MANAGER)


@pytest.fixture
def org_user_accountmanager(user: User, org: Org):
    temp_org_user = create_accountmanager(user, org)
    yield temp_org_user
    temp_org_user.delete()


@pytest.fixture
def org_user_pipelinemanager(user: User, org: Org):
    temp_org_user = OrgUser.objects.create(user=user, org=org, role=OrgUserRole.PIPELINE_MANAGER)
    yield temp_org_user
    temp_org_user.delete()


@pytest.fixture
def org_user_reportviewer(user: User, org: Org):
    temp_org_user = OrgUser.objects.create(user=user, org=org, role=OrgUserRole.REPORT_VIEWER)
    yield temp_org_user
    temp_org_user.delete()


@pytest.fixture
def anotherorg():
    temp_org = Org.objects.create(name="another-temp-org", slug="another-temp-org")
    yield temp_org
    temp_org.delete()

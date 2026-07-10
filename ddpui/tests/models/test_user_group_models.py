"""Task 7 Part A: the UserGroup/UserGroupMember model constraints —
unique(org, name), unique(group, orguser), unique(group, pending_email),
and the "exactly one of orguser/pending_email" CheckConstraint.
"""

import os

import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

import pytest
from django.contrib.auth.models import User
from django.db import IntegrityError, transaction

from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser
from ddpui.models.role_based_access import Role
from ddpui.models.user_group import UserGroup, UserGroupMember

pytestmark = pytest.mark.django_db


@pytest.fixture
def org():
    org = Org.objects.create(name="Group Model Org", slug="grp-model-org")
    yield org
    org.delete()


@pytest.fixture
def orguser(org):
    user = User.objects.create(username="groupmodel-user", email="groupmodel-user@test.com")
    role = Role.objects.filter(slug="analyst").first()
    ou = OrgUser.objects.create(user=user, org=org, new_role=role)
    yield ou
    ou.delete()


@pytest.fixture
def orguser2(org):
    user = User.objects.create(username="groupmodel-user2", email="groupmodel-user2@test.com")
    role = Role.objects.filter(slug="analyst").first()
    ou = OrgUser.objects.create(user=user, org=org, new_role=role)
    yield ou
    ou.delete()


def test_unique_org_name(org, orguser):
    UserGroup.objects.create(org=org, name="Funders", created_by=orguser)
    with pytest.raises(IntegrityError):
        with transaction.atomic():
            UserGroup.objects.create(org=org, name="Funders", created_by=orguser)


def test_same_name_allowed_in_different_org(org, orguser):
    other_org = Org.objects.create(name="Other Group Model Org", slug="other-grp-model-org")
    try:
        UserGroup.objects.create(org=org, name="Funders", created_by=orguser)
        UserGroup.objects.create(org=other_org, name="Funders", created_by=None)
    finally:
        other_org.delete()


def test_member_requires_exactly_one_of_orguser_pending_email(org, orguser):
    group = UserGroup.objects.create(org=org, name="G1", created_by=orguser)

    # neither set -> violates check constraint
    with pytest.raises(IntegrityError):
        with transaction.atomic():
            UserGroupMember.objects.create(group=group, orguser=None, pending_email=None)

    # both set -> violates check constraint
    with pytest.raises(IntegrityError):
        with transaction.atomic():
            UserGroupMember.objects.create(group=group, orguser=orguser, pending_email="a@b.com")


def test_member_orguser_only_is_valid(org, orguser):
    group = UserGroup.objects.create(org=org, name="G2", created_by=orguser)
    member = UserGroupMember.objects.create(group=group, orguser=orguser)
    assert member.status == "active"
    assert member.pending_email is None


def test_member_pending_email_only_is_valid(org, orguser):
    group = UserGroup.objects.create(org=org, name="G3", created_by=orguser)
    member = UserGroupMember.objects.create(
        group=group, pending_email="invitee@test.com", status="pending"
    )
    assert member.orguser is None


def test_unique_group_orguser(org, orguser):
    group = UserGroup.objects.create(org=org, name="G4", created_by=orguser)
    UserGroupMember.objects.create(group=group, orguser=orguser)
    with pytest.raises(IntegrityError):
        with transaction.atomic():
            UserGroupMember.objects.create(group=group, orguser=orguser)


def test_unique_group_pending_email(org, orguser):
    group = UserGroup.objects.create(org=org, name="G5", created_by=orguser)
    UserGroupMember.objects.create(group=group, pending_email="dup@test.com")
    with pytest.raises(IntegrityError):
        with transaction.atomic():
            UserGroupMember.objects.create(group=group, pending_email="dup@test.com")


def test_group_deletion_cascades_members(org, orguser):
    group = UserGroup.objects.create(org=org, name="G6", created_by=orguser)
    member = UserGroupMember.objects.create(group=group, orguser=orguser)
    group.delete()
    assert not UserGroupMember.objects.filter(id=member.id).exists()


def test_created_by_set_null_on_orguser_delete(org, orguser2):
    user = User.objects.create(username="groupmodel-creator", email="groupmodel-creator@test.com")
    role = Role.objects.filter(slug="analyst").first()
    creator = OrgUser.objects.create(user=user, org=org, new_role=role)
    group = UserGroup.objects.create(org=org, name="G7", created_by=creator)
    creator.delete()
    group.refresh_from_db()
    assert group.created_by_id is None

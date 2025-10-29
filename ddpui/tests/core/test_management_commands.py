import os
from io import StringIO
from unittest.mock import Mock, patch
import pytest
from django.contrib.auth.models import User
from django.core.management import call_command
from django.core.management.base import CommandError
from django.test import TestCase
from django.db import transaction

from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser, UserAttributes
from ddpui.models.role_based_access import Role

pytestmark = pytest.mark.django_db


@pytest.fixture(scope="session")
def seed_db(django_db_setup, django_db_blocker):
    """Seed the database with roles and permissions"""
    with django_db_blocker.unblock():
        call_command("loaddata", "001_roles.json")
        call_command("loaddata", "002_permissions.json")
        call_command("loaddata", "003_role_permissions.json")


class TestCreateOrgAndUserCommand:
    """Test cases for the createorganduser management command"""

    @patch("ddpui.core.orgfunctions.create_organization")
    def test_create_new_org_and_user_success(self, mock_create_org, seed_db):
        """Test creating a new organization and user successfully"""
        org_name = "Test Organization"
        user_email = "test@example.com"
        role_slug = "account-manager"

        # Mock organization creation to avoid external dependencies
        def create_org_side_effect(org_schema):
            org = Org.objects.create(name=org_schema.name, slug=org_schema.slug)
            return org, None  # Return tuple (org, error) as expected

        mock_create_org.side_effect = create_org_side_effect

        # Ensure the org and user don't exist
        assert not Org.objects.filter(name=org_name).exists()
        assert not User.objects.filter(email=user_email).exists()

        with patch("getpass.getpass", return_value="testpassword123"):
            call_command("createorganduser", org_name, user_email, role=role_slug)

        # Verify org was created
        org = Org.objects.get(name=org_name)
        assert org.slug == "test-organization"

        # Verify user was created
        user = User.objects.get(email=user_email)
        assert user.username == user_email

        # Verify OrgUser was created with correct role
        orguser = OrgUser.objects.get(org=org, user=user)
        assert orguser.new_role.slug == role_slug
        assert orguser.email_verified is True

        # Verify UserAttributes was created
        user_attrs = UserAttributes.objects.get(user=user)
        assert user_attrs.email_verified is True
        assert user_attrs.can_create_orgs is True

    def test_create_with_existing_org_and_user(self, seed_db):
        """Test command with existing organization and user"""
        # Create existing org and user
        org = Org.objects.create(name="Existing Org", slug="existing-org")
        user = User.objects.create_user(
            username="existing@example.com", email="existing@example.com", password="password"
        )

        # Ensure no OrgUser exists yet
        assert not OrgUser.objects.filter(org=org, user=user).exists()

        call_command("createorganduser", org.name, user.email, role="account-manager")

        # Verify OrgUser was created
        orguser = OrgUser.objects.get(org=org, user=user)
        assert orguser.new_role.slug == "account-manager"
        assert orguser.email_verified is True

    def test_create_with_existing_orguser(self, seed_db):
        """Test command when OrgUser already exists"""
        # Create existing org, user, and orguser
        org = Org.objects.create(name="Existing Org", slug="existing-org")
        user = User.objects.create_user(
            username="existing@example.com", email="existing@example.com", password="password"
        )
        role = Role.objects.get(slug="account-manager")
        OrgUser.objects.create(org=org, user=user, new_role=role)

        # Command should succeed without error
        call_command("createorganduser", org.name, user.email, role="account-manager")

        # Should still only have one OrgUser
        assert OrgUser.objects.filter(org=org, user=user).count() == 1

    def test_invalid_role_fails(self, seed_db):
        """Test command fails with invalid role"""
        with pytest.raises(SystemExit):
            call_command("createorganduser", "Test Org", "test@example.com", role="invalid-role")

    @patch("ddpui.core.orgfunctions.create_organization")
    def test_no_role_parameter_error_regression(self, mock_create_org, seed_db):
        """
        Regression test to ensure we don't get TypeError about 'role' parameter.
        This tests the specific bug we fixed where OrgUser.objects.create()
        was called with an invalid 'role' parameter.
        """
        org_name = "Regression Test Org"
        user_email = "regression@example.com"

        # Mock organization creation to avoid external dependencies
        def create_org_side_effect(org_schema):
            org = Org.objects.create(name=org_schema.name, slug=org_schema.slug)
            return org, None  # Return tuple (org, error) as expected

        mock_create_org.side_effect = create_org_side_effect

        # This should not raise TypeError: OrgUser() got unexpected keyword arguments: 'role'
        with patch("getpass.getpass", return_value="testpassword123"):
            call_command("createorganduser", org_name, user_email, role="account-manager")

        # Verify the OrgUser was created successfully
        org = Org.objects.get(name=org_name)
        user = User.objects.get(email=user_email)
        orguser = OrgUser.objects.get(org=org, user=user)

        # Verify the role was set correctly using new_role field (not old role field)
        assert orguser.new_role is not None
        assert orguser.new_role.slug == "account-manager"

        # Verify no 'role' field exists (this would cause the original error)
        # If this fails, it means the model has a 'role' field which would be unexpected
        assert not hasattr(orguser, "role") or orguser.role is None

    @patch("ddpui.core.orgfunctions.create_organization")
    def test_org_creation_with_mocked_dependencies(self, mock_create_org, seed_db):
        """Test that org creation calls the right function even if external dependencies fail"""
        # Mock the create_organization function to avoid Airbyte dependencies
        mock_create_org.return_value = None

        org_name = "Mock Test Org"
        user_email = "mock@example.com"

        # Create the org manually since we're mocking the creation function
        org = Org.objects.create(name=org_name, slug="mock-test-org")

        with patch("getpass.getpass", return_value="testpassword123"):
            call_command("createorganduser", org_name, user_email, role="account-manager")

        # Verify user and orguser were created
        user = User.objects.get(email=user_email)
        orguser = OrgUser.objects.get(org=org, user=user)
        assert orguser.new_role.slug == "account-manager"

    @patch("ddpui.core.orgfunctions.create_organization")
    def test_password_via_environment_variable(self, mock_create_org, seed_db):
        """Test providing password via PASSWORD environment variable"""
        org_name = "Env Password Org"
        user_email = "env@example.com"

        # Mock organization creation to avoid external dependencies
        def create_org_side_effect(org_schema):
            org = Org.objects.create(name=org_schema.name, slug=org_schema.slug)
            return org, None  # Return tuple (org, error) as expected

        mock_create_org.side_effect = create_org_side_effect
        # test password via environment variable
        with patch.dict(os.environ, {"PASSWORD": "env_password123"}):
            call_command("createorganduser", org_name, user_email, role="account-manager")

        # Verify user was created with correct password
        user = User.objects.get(email=user_email)
        assert user.check_password("env_password123")

    @patch("ddpui.core.orgfunctions.create_organization")
    def test_super_admin_role_creation(self, mock_create_org, seed_db):
        """Test creating user with super-admin role"""
        org_name = "Super Admin Org"
        user_email = "superadmin@example.com"

        # Mock organization creation to avoid external dependencies
        def create_org_side_effect(org_schema):
            org = Org.objects.create(name=org_schema.name, slug=org_schema.slug)
            return org, None  # Return tuple (org, error) as expected

        mock_create_org.side_effect = create_org_side_effect

        with patch("getpass.getpass", return_value="testpassword123"):
            call_command("createorganduser", org_name, user_email, role="super-admin")

        # Verify orguser has super-admin role
        org = Org.objects.get(name=org_name)
        user = User.objects.get(email=user_email)
        orguser = OrgUser.objects.get(org=org, user=user)
        assert orguser.new_role.slug == "super-admin"

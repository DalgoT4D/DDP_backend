"""Tests for org_preferences_api.py endpoints"""

import os
import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from unittest.mock import patch, MagicMock
import pytest
from ninja.errors import HttpError

from ddpui.models.org_preferences import OrgPreferences
from ddpui.models.org_plans import OrgPlans
from ddpui.models.org_supersets import OrgSupersets
from ddpui.models.userpreferences import UserPreferences
from ddpui.models.role_based_access import Role
from ddpui.schemas.org_preferences_schema import (
    CreateOrgPreferencesSchema,
    UpdateLLMOptinSchema,
    UpdateDiscordNotificationsSchema,
)
from ddpui.api.org_preferences_api import (
    create_org_preferences,
    update_org_preferences,
    update_discord_notifications,
    get_org_preferences,
    get_tools_versions,
    get_org_plans,
    initiate_upgrade_dalgo_plan,
)
from ddpui.tests.api_tests.test_user_org_api import mock_request

pytestmark = pytest.mark.django_db


# ================================================================================
# Fixtures
# ================================================================================


# ================================================================================
# Tests for create_org_preferences
# ================================================================================


class TestCreateOrgPreferences:
    @patch("ddpui.api.org_preferences_api.OrgPreferences.objects")
    def test_create_success(self, mock_prefs_manager, orguser, seed_db):
        """Test successful creation via mocked OrgPreferences.objects"""
        mock_prefs_manager.filter.return_value.exists.return_value = False
        mock_pref_instance = MagicMock()
        mock_pref_instance.to_json.return_value = {
            "org": {"name": orguser.org.name, "slug": orguser.org.slug},
            "llm_optin": False,
            "llm_optin_approved_by": None,
            "llm_optin_date": None,
            "enable_discord_notifications": False,
            "discord_webhook": None,
        }
        mock_prefs_manager.create.return_value = mock_pref_instance

        request = mock_request(orguser)
        payload = CreateOrgPreferencesSchema(llm_optin=False)
        result = create_org_preferences(request, payload)
        assert result["success"] is True
        assert result["res"]["llm_optin"] is False

    def test_create_already_exists(self, orguser, seed_db):
        OrgPreferences.objects.create(org=orguser.org)
        request = mock_request(orguser)
        payload = CreateOrgPreferencesSchema(llm_optin=True)
        with pytest.raises(HttpError) as exc:
            create_org_preferences(request, payload)
        assert exc.value.status_code == 400
        assert "already exist" in str(exc.value)
        # Cleanup
        OrgPreferences.objects.filter(org=orguser.org).delete()


# ================================================================================
# Tests for update_org_preferences (LLM optin)
# ================================================================================


class TestUpdateOrgPreferences:
    @patch("ddpui.api.org_preferences_api.create_notification")
    def test_llm_optin_true(self, mock_notif, orguser, seed_db):
        mock_notif.return_value = (None, {"success": True})
        request = mock_request(orguser)
        payload = UpdateLLMOptinSchema(llm_optin=True)
        result = update_org_preferences(request, payload)
        assert result["success"] is True
        assert result["res"]["llm_optin"] is True
        # Cleanup
        OrgPreferences.objects.filter(org=orguser.org).delete()
        UserPreferences.objects.filter(orguser=orguser).delete()

    def test_llm_optin_false(self, orguser, seed_db):
        request = mock_request(orguser)
        payload = UpdateLLMOptinSchema(llm_optin=False)
        result = update_org_preferences(request, payload)
        assert result["success"] is True
        assert result["res"]["llm_optin"] is False
        # Cleanup
        OrgPreferences.objects.filter(org=orguser.org).delete()
        UserPreferences.objects.filter(orguser=orguser).delete()

    @patch("ddpui.api.org_preferences_api.create_notification")
    def test_llm_optin_creates_prefs_if_not_exist(self, mock_notif, orguser, seed_db):
        mock_notif.return_value = (None, {"success": True})
        assert not OrgPreferences.objects.filter(org=orguser.org).exists()
        assert not UserPreferences.objects.filter(orguser=orguser).exists()

        request = mock_request(orguser)
        payload = UpdateLLMOptinSchema(llm_optin=True)
        result = update_org_preferences(request, payload)
        assert result["success"] is True
        assert OrgPreferences.objects.filter(org=orguser.org).exists()
        assert UserPreferences.objects.filter(orguser=orguser).exists()
        # Cleanup
        OrgPreferences.objects.filter(org=orguser.org).delete()
        UserPreferences.objects.filter(orguser=orguser).delete()

    @patch("ddpui.api.org_preferences_api.create_notification")
    def test_llm_optin_notification_error(self, mock_notif, orguser, seed_db):
        mock_notif.return_value = (None, {"errors": ["some error"]})
        request = mock_request(orguser)
        payload = UpdateLLMOptinSchema(llm_optin=True)
        with pytest.raises(HttpError) as exc:
            update_org_preferences(request, payload)
        assert exc.value.status_code == 400
        # Cleanup
        OrgPreferences.objects.filter(org=orguser.org).delete()
        UserPreferences.objects.filter(orguser=orguser).delete()


# ================================================================================
# Tests for update_discord_notifications
# ================================================================================


class TestUpdateDiscordNotifications:
    def test_enable_with_webhook(self, orguser, seed_db):
        request = mock_request(orguser)
        payload = UpdateDiscordNotificationsSchema(
            enable_discord_notifications=True,
            discord_webhook="https://discord.com/api/webhooks/test",
        )
        result = update_discord_notifications(request, payload)
        assert result["success"] is True
        assert result["res"]["enable_discord_notifications"] is True
        assert result["res"]["discord_webhook"] == "https://discord.com/api/webhooks/test"
        # Cleanup
        OrgPreferences.objects.filter(org=orguser.org).delete()

    def test_enable_without_webhook_no_existing(self, orguser, seed_db):
        request = mock_request(orguser)
        payload = UpdateDiscordNotificationsSchema(
            enable_discord_notifications=True,
        )
        with pytest.raises(HttpError) as exc:
            update_discord_notifications(request, payload)
        assert exc.value.status_code == 400
        assert "Discord webhook is required" in str(exc.value)
        # Cleanup
        OrgPreferences.objects.filter(org=orguser.org).delete()

    def test_disable(self, orguser, seed_db):
        OrgPreferences.objects.create(
            org=orguser.org,
            enable_discord_notifications=True,
            discord_webhook="https://discord.com/api/webhooks/test",
        )
        request = mock_request(orguser)
        payload = UpdateDiscordNotificationsSchema(enable_discord_notifications=False)
        result = update_discord_notifications(request, payload)
        assert result["success"] is True
        assert result["res"]["enable_discord_notifications"] is False
        assert result["res"]["discord_webhook"] is None
        # Cleanup
        OrgPreferences.objects.filter(org=orguser.org).delete()


# ================================================================================
# Tests for get_org_preferences
# ================================================================================


class TestGetOrgPreferences:
    def test_get_existing(self, orguser, seed_db):
        OrgPreferences.objects.create(org=orguser.org, llm_optin=True)
        request = mock_request(orguser)
        result = get_org_preferences(request)
        assert result["success"] is True
        assert result["res"]["llm_optin"] is True
        # Cleanup
        OrgPreferences.objects.filter(org=orguser.org).delete()

    def test_get_creates_if_not_exist(self, orguser, seed_db):
        assert not OrgPreferences.objects.filter(org=orguser.org).exists()
        request = mock_request(orguser)
        result = get_org_preferences(request)
        assert result["success"] is True
        assert result["res"]["llm_optin"] is False  # default
        # Cleanup
        OrgPreferences.objects.filter(org=orguser.org).delete()


# ================================================================================
# Tests for get_tools_versions
# ================================================================================


class TestGetToolsVersions:
    @patch("ddpui.api.org_preferences_api.RedisClient")
    @patch("ddpui.api.org_preferences_api.elementary_service")
    @patch("ddpui.api.org_preferences_api.prefect_service")
    @patch("ddpui.api.org_preferences_api.airbyte_service")
    def test_get_versions_no_cache(
        self, mock_airbyte, mock_prefect, mock_elementary, mock_redis_cls, orguser, seed_db
    ):
        mock_redis = MagicMock()
        mock_redis.get.return_value = None
        mock_redis_cls.get_instance.return_value = mock_redis

        mock_airbyte.get_current_airbyte_version.return_value = "0.50.1"
        mock_prefect.get_prefect_version.return_value = "2.14.0"
        mock_elementary.get_dbt_version.return_value = "1.7.0"
        mock_elementary.get_edr_version.return_value = "0.13.0"

        request = mock_request(orguser)
        result = get_tools_versions(request)
        assert result["success"] is True
        versions = result["res"]
        assert len(versions) == 5
        assert versions[0]["Airbyte"]["version"] == "0.50.1"
        assert versions[1]["Prefect"]["version"] == "2.14.0"

    @patch("ddpui.api.org_preferences_api.RedisClient")
    def test_get_versions_from_cache(self, mock_redis_cls, orguser, seed_db):
        import json

        cached = json.dumps([{"Airbyte": {"version": "cached"}}])
        mock_redis = MagicMock()
        mock_redis.get.return_value = cached
        mock_redis_cls.get_instance.return_value = mock_redis

        request = mock_request(orguser)
        result = get_tools_versions(request)
        assert result["success"] is True
        assert result["res"][0]["Airbyte"]["version"] == "cached"


# ================================================================================
# Tests for get_org_plans
# ================================================================================


class TestGetOrgPlans:
    def test_plan_not_found(self, orguser, seed_db):
        request = mock_request(orguser)
        with pytest.raises(HttpError) as exc:
            get_org_plans(request)
        assert exc.value.status_code == 400
        assert "Org's Plan not found" in str(exc.value)

    def test_plan_found(self, orguser, seed_db):
        plan = OrgPlans.objects.create(
            org=orguser.org,
            base_plan="Dalgo",
            superset_included=True,
        )
        request = mock_request(orguser)
        result = get_org_plans(request)
        assert result["success"] is True
        assert result["res"]["base_plan"] == "Dalgo"
        plan.delete()


# ================================================================================
# Tests for initiate_upgrade_dalgo_plan
# ================================================================================


class TestInitiateUpgradeDalgoPlan:
    def test_no_plan(self, orguser, seed_db):
        request = mock_request(orguser)
        with pytest.raises(HttpError) as exc:
            initiate_upgrade_dalgo_plan(request)
        assert exc.value.status_code == 400

    @patch("ddpui.api.org_preferences_api.send_text_message")
    @patch.dict(os.environ, {"BIZ_DEV_EMAILS": "dev@test.com,dev2@test.com"})
    def test_success(self, mock_send, orguser, seed_db):
        plan = OrgPlans.objects.create(
            org=orguser.org,
            base_plan="Dalgo",
            features={"max_users": 5},
        )
        request = mock_request(orguser)
        result = initiate_upgrade_dalgo_plan(request)
        assert result["success"] is True
        assert mock_send.call_count == 2
        plan.refresh_from_db()
        assert plan.upgrade_requested is True
        plan.delete()

    @patch("ddpui.api.org_preferences_api.send_text_message")
    @patch.dict(os.environ, {"BIZ_DEV_EMAILS": "dev@test.com"})
    def test_already_requested(self, mock_send, orguser, seed_db):
        plan = OrgPlans.objects.create(
            org=orguser.org,
            base_plan="Dalgo",
            upgrade_requested=True,
        )
        request = mock_request(orguser)
        result = initiate_upgrade_dalgo_plan(request)
        assert result["success"] is True
        assert result["res"] == "Upgrade request already sent"
        mock_send.assert_not_called()
        plan.delete()


def test_seed_data(seed_db):
    """Test seed data loaded"""
    assert Role.objects.count() == 5

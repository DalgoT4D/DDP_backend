import pytest
from django.test import TestCase
from django.db import IntegrityError
from ddpui.models.org import Org, OrgFeatureFlag
from ddpui.utils.feature_flags import (
    enable_feature_flag,
    disable_feature_flag,
    is_feature_flag_enabled,
    get_all_feature_flags_for_org,
    clear_org_flag,
    bulk_set_feature_flag,
)

pytestmark = pytest.mark.django_db


class TestFeatureFlags(TestCase):
    """Minimal test cases for the feature flag system"""

    def setUp(self):
        """Set up test data"""
        # Create test org
        self.org = Org.objects.create(name="Test Org", slug="test-org")

    def tearDown(self):
        """Clean up after each test"""
        OrgFeatureFlag.objects.all().delete()
        Org.objects.all().delete()

    def test_global_flag_enable_disable(self):
        """Test enabling and disabling a global flag without any org"""
        # Enable global flag
        enable_feature_flag("DATA_QUALITY")
        self.assertTrue(is_feature_flag_enabled("DATA_QUALITY"))

        # Disable global flag
        disable_feature_flag("DATA_QUALITY")
        self.assertFalse(is_feature_flag_enabled("DATA_QUALITY"))

    def test_org_specific_flag_enable_disable(self):
        """Test enabling and disabling an org-specific flag"""
        # Enable org-specific flag
        enable_feature_flag("USAGE_DASHBOARD", org=self.org)
        self.assertTrue(is_feature_flag_enabled("USAGE_DASHBOARD", org=self.org))

        # Disable org-specific flag
        disable_feature_flag("USAGE_DASHBOARD", org=self.org)
        self.assertFalse(is_feature_flag_enabled("USAGE_DASHBOARD", org=self.org))

    def test_org_specific_overrides_global(self):
        """Test that org-specific flags take precedence over global flags"""
        # Enable global flags
        enable_feature_flag("DATA_QUALITY")  # Global: True
        enable_feature_flag("USAGE_DASHBOARD")  # Global: True

        # Override with org-specific flags
        disable_feature_flag("DATA_QUALITY", org=self.org)  # Org: False
        enable_feature_flag("USAGE_DASHBOARD", org=self.org)  # Org: True (same as global)

        # Check that org-specific overrides global
        self.assertFalse(
            is_feature_flag_enabled("DATA_QUALITY", org=self.org)
        )  # Overridden to False
        self.assertTrue(
            is_feature_flag_enabled("USAGE_DASHBOARD", org=self.org)
        )  # Org-specific True

        # Check that global flags are still intact
        self.assertTrue(is_feature_flag_enabled("DATA_QUALITY"))  # Global still True
        self.assertTrue(is_feature_flag_enabled("USAGE_DASHBOARD"))  # Global still True

        # Verify get_all_feature_flags_for_org reflects the correct override behavior
        all_flags = get_all_feature_flags_for_org(self.org)
        print(all_flags)
        self.assertFalse(all_flags["DATA_QUALITY"])  # Should be False due to org override
        self.assertTrue(all_flags["USAGE_DASHBOARD"])  # Should be True from org-specific setting

    def test_invalid_flag_no_db_entry(self):
        """Test that invalid flag names don't create DB entries for both enable and disable"""
        # Try to enable invalid flag
        result = enable_feature_flag("INVALID_FLAG")
        self.assertIsNone(result)

        # Try to enable invalid flag with org
        result = enable_feature_flag("ANOTHER_INVALID_FLAG", org=self.org)
        self.assertIsNone(result)

        # Try to disable invalid flag
        result = disable_feature_flag("INVALID_FLAG")
        self.assertIsNone(result)

        # Try to disable invalid flag with org
        result = disable_feature_flag("ANOTHER_INVALID_FLAG", org=self.org)
        self.assertIsNone(result)

        # Verify no DB entries were created
        self.assertEqual(OrgFeatureFlag.objects.filter(flag_name="INVALID_FLAG").count(), 0)
        self.assertEqual(OrgFeatureFlag.objects.filter(flag_name="ANOTHER_INVALID_FLAG").count(), 0)

    def test_uniqueness_constraint(self):
        """Test that the database uniqueness constraint prevents duplicate (org, flag_name) pairs"""
        from django.db import transaction

        # Create a global flag
        OrgFeatureFlag.objects.create(org=None, flag_name="DATA_QUALITY", flag_value=True)

        # Try to create duplicate global flag - should raise IntegrityError
        with self.assertRaises(IntegrityError):
            with transaction.atomic():
                OrgFeatureFlag.objects.create(org=None, flag_name="DATA_QUALITY", flag_value=False)

        # Create an org-specific flag
        OrgFeatureFlag.objects.create(org=self.org, flag_name="DATA_QUALITY", flag_value=False)

        # Try to create duplicate org-specific flag - should raise IntegrityError
        with self.assertRaises(IntegrityError):
            with transaction.atomic():
                OrgFeatureFlag.objects.create(
                    org=self.org, flag_name="DATA_QUALITY", flag_value=True
                )

        # Verify we can create same flag for different org
        org2 = Org.objects.create(name="Test Org 2", slug="test-org-2")
        flag = OrgFeatureFlag.objects.create(org=org2, flag_name="DATA_QUALITY", flag_value=True)
        self.assertIsNotNone(flag)

        # Clean up
        org2.delete()

    def test_clear_org_flag_removes_the_override(self):
        """clear_org_flag deletes the org's row entirely, leaving no override at all"""
        enable_feature_flag("DATA_QUALITY", org=self.org)
        self.assertTrue(
            OrgFeatureFlag.objects.filter(org=self.org, flag_name="DATA_QUALITY").exists()
        )

        result = clear_org_flag("DATA_QUALITY", org=self.org)

        self.assertTrue(result)
        self.assertFalse(
            OrgFeatureFlag.objects.filter(org=self.org, flag_name="DATA_QUALITY").exists()
        )

    def test_clear_org_flag_invalid_name_returns_none(self):
        """an unknown flag_name is rejected, same as enable/disable, and touches no rows"""
        result = clear_org_flag("NOT_A_REAL_FLAG", org=self.org)
        self.assertIsNone(result)

    def test_bulk_set_feature_flag_mixed_success_and_failure(self):
        """valid orgs succeed independently of a nonexistent org_id in the same request —
        best-effort, not all-or-nothing (plan.md §4.3)"""
        org2 = Org.objects.create(name="Test Org 2", slug="test-org-2")
        nonexistent_id = 999999

        results = bulk_set_feature_flag(
            "DATA_QUALITY", [self.org.id, org2.id, nonexistent_id], True
        )

        results_by_org = {r["org_id"]: r["success"] for r in results}
        self.assertEqual(results_by_org, {self.org.id: True, org2.id: True, nonexistent_id: False})
        self.assertTrue(is_feature_flag_enabled("DATA_QUALITY", org=self.org))
        self.assertTrue(is_feature_flag_enabled("DATA_QUALITY", org=org2))

        org2.delete()

    def test_bulk_set_feature_flag_failure_shape_is_generic(self):
        """every result is exactly {org_id, success} -- no message field, so a failed
        org_id can never be told apart from a different failure cause (plan.md §5)"""
        results = bulk_set_feature_flag("DATA_QUALITY", [999999], True)
        self.assertEqual(results, [{"org_id": 999999, "success": False}])
        self.assertEqual(set(results[0].keys()), {"org_id", "success"})

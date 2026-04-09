import os
from unittest.mock import Mock, patch, MagicMock
import pytest

import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from ddpui.core.orgfunctions import create_organization, create_org_plan
from ddpui.models.org import Org
from ddpui.models.org_plans import OrgPlans, OrgPlanType
from ddpui.utils.constants import DALGO_WITH_SUPERSET, DALGO, FREE_TRIAL

pytestmark = pytest.mark.django_db


# =========================================================
# Tests for create_organization
# =========================================================
class TestCreateOrganization:
    @patch("ddpui.core.orgfunctions.add_custom_connectors_to_workspace")
    @patch("ddpui.core.orgfunctions.airbytehelpers.setup_airbyte_workspace_v1")
    def test_create_organization_success(self, mock_setup_workspace, mock_add_connectors):
        """Test successful organization creation."""
        mock_workspace = Mock()
        mock_workspace.workspaceId = "ws-123"
        mock_setup_workspace.return_value = mock_workspace
        mock_add_connectors.delay = Mock()

        payload = Mock()
        payload.name = "TestOrg"
        payload.viz_url = None
        payload.website = None

        org, error = create_organization(payload)

        assert org is not None
        assert error is None
        assert org.name == "TestOrg"
        assert org.slug is not None
        mock_setup_workspace.assert_called_once()

        # Cleanup
        org.delete()

    def test_create_organization_duplicate_name(self):
        """Test creating org with existing name returns error."""
        existing_org = Org.objects.create(name="DuplicateOrg", slug="duplicateorg")

        payload = Mock()
        payload.name = "DuplicateOrg"

        org, error = create_organization(payload)

        assert org is None
        assert "already exists" in error

        existing_org.delete()

    @patch("ddpui.core.orgfunctions.airbytehelpers.setup_airbyte_workspace_v1")
    def test_create_organization_airbyte_failure(self, mock_setup_workspace):
        """Test that org is deleted if airbyte workspace creation fails."""
        mock_setup_workspace.side_effect = Exception("Airbyte down")

        payload = Mock()
        payload.name = "FailOrg"
        payload.viz_url = None
        payload.website = None

        org, error = create_organization(payload)

        assert org is None
        assert "could not create airbyte workspace" in error
        # Verify org was cleaned up
        assert Org.objects.filter(name="FailOrg").count() == 0


# =========================================================
# Tests for create_org_plan
# =========================================================
class TestCreateOrgPlan:
    @pytest.fixture
    def org(self):
        org = Org.objects.create(name="PlanTestOrg", slug="plan-test-org")
        yield org
        org.delete()

    def test_create_org_plan_free_trial(self, org):
        """Test creating a Free Trial plan."""
        payload = Mock()
        payload.base_plan = OrgPlanType.FREE_TRIAL
        payload.can_upgrade_plan = True
        payload.subscription_duration = "monthly"
        payload.superset_included = False
        payload.start_date = "2024-01-01"
        payload.end_date = "2024-02-01"

        plan, error = create_org_plan(payload, org)

        assert plan is not None
        assert error is None
        assert plan.features == FREE_TRIAL
        assert plan.base_plan == OrgPlanType.FREE_TRIAL

        plan.delete()

    def test_create_org_plan_internal(self, org):
        """Test creating an Internal plan."""
        payload = Mock()
        payload.base_plan = OrgPlanType.INTERNAL
        payload.can_upgrade_plan = False
        payload.subscription_duration = "yearly"
        payload.superset_included = True
        payload.start_date = None
        payload.end_date = None

        plan, error = create_org_plan(payload, org)

        assert plan is not None
        assert error is None
        assert plan.features == DALGO_WITH_SUPERSET

        plan.delete()

    def test_create_org_plan_dalgo_with_superset(self, org):
        """Test creating a Dalgo plan with superset included."""
        payload = Mock()
        payload.base_plan = OrgPlanType.DALGO
        payload.can_upgrade_plan = True
        payload.subscription_duration = "monthly"
        payload.superset_included = True
        payload.start_date = None
        payload.end_date = None

        plan, error = create_org_plan(payload, org)

        assert plan is not None
        assert error is None
        assert plan.features == DALGO_WITH_SUPERSET

        plan.delete()

    def test_create_org_plan_dalgo_without_superset(self, org):
        """Test creating a Dalgo plan without superset."""
        payload = Mock()
        payload.base_plan = OrgPlanType.DALGO
        payload.can_upgrade_plan = True
        payload.subscription_duration = "monthly"
        payload.superset_included = False
        payload.start_date = None
        payload.end_date = None

        plan, error = create_org_plan(payload, org)

        assert plan is not None
        assert error is None
        assert plan.features == DALGO

        plan.delete()

    def test_create_org_plan_duplicate(self, org):
        """Test that creating a plan when one already exists returns error."""
        # Create first plan
        OrgPlans.objects.create(
            org=org,
            base_plan=OrgPlanType.DALGO,
            can_upgrade_plan=False,
            subscription_duration="monthly",
            superset_included=False,
        )

        payload = Mock()
        payload.base_plan = OrgPlanType.DALGO
        payload.can_upgrade_plan = True
        payload.subscription_duration = "yearly"
        payload.superset_included = True
        payload.start_date = None
        payload.end_date = None

        plan, error = create_org_plan(payload, org)

        assert plan is None
        assert "already exists" in error

        OrgPlans.objects.filter(org=org).delete()

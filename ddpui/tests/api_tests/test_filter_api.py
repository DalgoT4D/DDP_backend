"""API Tests for filter_api.get_filter_preview

Tests the `constraints` narrowing shared by dependent-filter narrowing -- a JSON list of
{column, operator, value} entries, AND-combined.
"""

import os
import django
from unittest.mock import patch, MagicMock
import pytest
from ninja.errors import HttpError

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from django.contrib.auth.models import User
from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser
from ddpui.models.role_based_access import Role
from ddpui.auth import ACCOUNT_MANAGER_ROLE
from ddpui.api.filter_api import get_filter_preview
from ddpui.tests.api_tests.test_user_org_api import seed_db, mock_request

pytestmark = pytest.mark.django_db


# ================================================================================
# Fixtures
# ================================================================================


@pytest.fixture
def authuser():
    """A django User object"""
    user = User.objects.create(
        username="filterapiuser", email="filterapiuser@test.com", password="testpassword"
    )
    yield user
    user.delete()


@pytest.fixture
def org():
    """An Org object"""
    org = Org.objects.create(
        name="Filter API Test Org",
        slug="filter-api-test-org",  # max_length=20
        airbyte_workspace_id="workspace-id",
    )
    yield org
    org.delete()


@pytest.fixture
def orguser(authuser, org):
    """An OrgUser with account manager role"""
    orguser = OrgUser.objects.create(
        user=authuser,
        org=org,
        new_role=Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first(),
    )
    yield orguser
    orguser.delete()


# ================================================================================
# Test get_filter_preview narrowing by active constraints
# ================================================================================


class TestGetFilterPreviewNarrowing:
    """Tests for dependent-filter narrowing on GET /api/filters/preview/"""

    def test_value_filter_without_constraints_is_unnarrowed(self, orguser, seed_db):
        """No constraints -> only the existing NOT NULL where clause (no regression)"""
        mock_results = [{"value": "Kerala", "count": 12}]

        with patch("ddpui.api.filter_api.OrgWarehouse.objects") as mock_ow, patch(
            "ddpui.services.dashboard_service.execute_query"
        ) as mock_exec, patch("ddpui.api.filter_api.get_warehouse_client") as mock_wc:
            mock_ow.filter.return_value.first.return_value = MagicMock(wtype="postgres")
            mock_wc.return_value = MagicMock()
            mock_exec.return_value = mock_results

            request = mock_request(orguser)
            response = get_filter_preview(
                request,
                schema_name="public",
                table_name="schools",
                column_name="state",
                filter_type="value",
            )

            assert len(response.options) == 1
            query_builder = mock_exec.call_args[0][1]
            assert len(query_builder.where_clauses) == 1

    def test_value_filter_narrowed_by_one_constraint(self, orguser, seed_db):
        """One entry in `constraints` adds a second WHERE clause narrowing the filter"""
        mock_results = [{"value": "Ernakulam", "count": 4}]

        with patch("ddpui.api.filter_api.OrgWarehouse.objects") as mock_ow, patch(
            "ddpui.services.dashboard_service.execute_query"
        ) as mock_exec, patch("ddpui.api.filter_api.get_warehouse_client") as mock_wc:
            mock_ow.filter.return_value.first.return_value = MagicMock(wtype="postgres")
            mock_wc.return_value = MagicMock()
            mock_exec.return_value = mock_results

            request = mock_request(orguser)
            response = get_filter_preview(
                request,
                schema_name="public",
                table_name="schools",
                column_name="district",
                filter_type="value",
                constraints='[{"column": "state", "operator": "in", "value": ["Kerala"]}]',
            )

            assert len(response.options) == 1
            query_builder = mock_exec.call_args[0][1]
            assert len(query_builder.where_clauses) == 2
            assert "state" in str(query_builder.where_clauses[1])

    def test_value_filter_narrowed_by_a_numerical_range_constraint(self, orguser, seed_db):
        """A numerical/datetime constraint sends two operator entries (>= and <=), which
        apply_chart_filters turns into a between-style range, not an exact match"""
        mock_results = [{"value": "Ernakulam", "count": 4}]

        with patch("ddpui.api.filter_api.OrgWarehouse.objects") as mock_ow, patch(
            "ddpui.services.dashboard_service.execute_query"
        ) as mock_exec, patch("ddpui.api.filter_api.get_warehouse_client") as mock_wc:
            mock_ow.filter.return_value.first.return_value = MagicMock(wtype="postgres")
            mock_wc.return_value = MagicMock()
            mock_exec.return_value = mock_results

            request = mock_request(orguser)
            response = get_filter_preview(
                request,
                schema_name="public",
                table_name="schools",
                column_name="district",
                filter_type="value",
                constraints=(
                    '[{"column": "population", "operator": "greater_than_equal", "value": 10}, '
                    '{"column": "population", "operator": "less_than_equal", "value": 50}]'
                ),
            )

            assert len(response.options) == 1
            query_builder = mock_exec.call_args[0][1]
            # NOT NULL + one clause per operator entry
            assert len(query_builder.where_clauses) == 3
            assert "population" in str(query_builder.where_clauses[1])
            assert "population" in str(query_builder.where_clauses[2])

    def test_value_filter_narrowed_by_multiple_constraints_and_combined(self, orguser, seed_db):
        """Two entries in `constraints` add two WHERE clauses -- AND-combined, per the spec"""
        mock_results = [{"value": "Ernakulam", "count": 4}]

        with patch("ddpui.api.filter_api.OrgWarehouse.objects") as mock_ow, patch(
            "ddpui.services.dashboard_service.execute_query"
        ) as mock_exec, patch("ddpui.api.filter_api.get_warehouse_client") as mock_wc:
            mock_ow.filter.return_value.first.return_value = MagicMock(wtype="postgres")
            mock_wc.return_value = MagicMock()
            mock_exec.return_value = mock_results

            request = mock_request(orguser)
            response = get_filter_preview(
                request,
                schema_name="public",
                table_name="schools",
                column_name="city",
                filter_type="value",
                constraints=(
                    '[{"column": "state", "operator": "in", "value": ["Kerala"]}, '
                    '{"column": "country", "operator": "in", "value": ["India"]}]'
                ),
            )

            assert len(response.options) == 1
            query_builder = mock_exec.call_args[0][1]
            # NOT NULL + one clause per constraint
            assert len(query_builder.where_clauses) == 3
            assert "state" in str(query_builder.where_clauses[1])
            assert "country" in str(query_builder.where_clauses[2])

    def test_multi_constraint_narrowing_keeps_the_same_cap_as_unnarrowed(self, orguser, seed_db):
        """The 100-item (default) cap applies identically whether narrowed by several
        constraints or not -- narrowing must never bypass it."""
        mock_results = [{"value": "Ernakulam", "count": 4}]

        with patch("ddpui.api.filter_api.OrgWarehouse.objects") as mock_ow, patch(
            "ddpui.services.dashboard_service.execute_query"
        ) as mock_exec, patch("ddpui.api.filter_api.get_warehouse_client") as mock_wc:
            mock_ow.filter.return_value.first.return_value = MagicMock(wtype="postgres")
            mock_wc.return_value = MagicMock()
            mock_exec.return_value = mock_results

            request = mock_request(orguser)
            get_filter_preview(
                request,
                schema_name="public",
                table_name="schools",
                column_name="city",
                filter_type="value",
                constraints=(
                    '[{"column": "state", "operator": "in", "value": ["Kerala"]}, '
                    '{"column": "country", "operator": "in", "value": ["India"]}]'
                ),
            )

            query_builder = mock_exec.call_args[0][1]
            assert query_builder.limit_records == 100

    def test_invalid_constraints_json_returns_400(self, orguser, seed_db):
        """Malformed JSON in `constraints` is a client error, not swallowed or 500'd"""
        request = mock_request(orguser)
        with pytest.raises(HttpError) as excinfo:
            get_filter_preview(
                request,
                schema_name="public",
                table_name="schools",
                column_name="district",
                filter_type="value",
                constraints="not json",
            )
        assert excinfo.value.status_code == 400

    def test_narrowed_query_falls_back_to_full_list_on_error(self, orguser, seed_db):
        """If a constraint's column was renamed/removed, the narrowed query errors --
        retry unnarrowed rather than breaking the child (spec: dependent-filters)."""
        mock_results = [{"value": "Ernakulam", "count": 4}, {"value": "Pune", "count": 2}]

        with patch("ddpui.api.filter_api.OrgWarehouse.objects") as mock_ow, patch(
            "ddpui.services.dashboard_service.execute_query"
        ) as mock_exec, patch("ddpui.api.filter_api.get_warehouse_client") as mock_wc:
            mock_ow.filter.return_value.first.return_value = MagicMock(wtype="postgres")
            mock_wc.return_value = MagicMock()
            mock_exec.side_effect = [Exception("column state does not exist"), mock_results]

            request = mock_request(orguser)
            response = get_filter_preview(
                request,
                schema_name="public",
                table_name="schools",
                column_name="district",
                filter_type="value",
                constraints='[{"column": "state", "operator": "in", "value": ["Kerala"]}]',
            )

            # Full, unnarrowed list came back -- the child didn't break.
            assert len(response.options) == 2
            assert mock_exec.call_count == 2
            # First attempt was narrowed (2 clauses); the retry has only the NOT NULL clause.
            first_query_builder = mock_exec.call_args_list[0][0][1]
            retry_query_builder = mock_exec.call_args_list[1][0][1]
            assert len(first_query_builder.where_clauses) == 2
            assert len(retry_query_builder.where_clauses) == 1

    def test_error_without_constraints_still_raises(self, orguser, seed_db):
        """An independent filter's query failure is a real error -- no fallback to swallow it"""
        with patch("ddpui.api.filter_api.OrgWarehouse.objects") as mock_ow, patch(
            "ddpui.services.dashboard_service.execute_query"
        ) as mock_exec, patch("ddpui.api.filter_api.get_warehouse_client") as mock_wc:
            mock_ow.filter.return_value.first.return_value = MagicMock(wtype="postgres")
            mock_wc.return_value = MagicMock()
            mock_exec.side_effect = Exception("table schools does not exist")

            request = mock_request(orguser)
            with pytest.raises(Exception):
                get_filter_preview(
                    request,
                    schema_name="public",
                    table_name="schools",
                    column_name="state",
                    filter_type="value",
                )

            assert mock_exec.call_count == 1

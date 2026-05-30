import os
import django

import pytest
from ninja.errors import HttpError
from django.contrib.auth.models import User

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from ddpui.api.alert_api import create_alert, list_alerts, list_fired_alerts
from ddpui.api.metrics_api import update_metric, delete_metric
from ddpui.core.alerts.alert_service import AlertService
from ddpui.models.alert import Alert, AlertEvaluation, AlertQueryConfig
from ddpui.models.metrics import MetricDefinition
from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser
from ddpui.models.role_based_access import Role
from ddpui.schemas.alert_schema import AlertCreate
from ddpui.schemas.metric_schema import MetricUpdate
from ddpui.tests.api_tests.test_user_org_api import mock_request, seed_db

pytestmark = pytest.mark.django_db


@pytest.fixture
def org():
    return Org.objects.create(name="Alert Org", slug="alert-org")


@pytest.fixture
def orguser(org, seed_db):
    user = User.objects.create(username="alerts-user", email="alerts@example.com")
    role = Role.objects.first()
    return OrgUser.objects.create(org=org, user=user, new_role=role)


@pytest.fixture
def api_request(orguser):
    return mock_request(orguser)


def create_metric(orguser: OrgUser, **overrides) -> MetricDefinition:
    defaults = {
        "name": "Attendance metric",
        "schema_name": "analytics",
        "table_name": "attendance_fact",
        "column": "attendance",
        "aggregation": "sum",
        "direction": "increase",
        "time_column": None,
        "time_grain": "month",
        "target_value": 100.0,
        "amber_threshold_pct": 80,
        "green_threshold_pct": 100,
        "program_tag": "Health",
        "metric_type_tag": "Output",
        "trend_periods": 12,
        "display_order": 0,
        "created_by": orguser,
        "org": orguser.org,
    }
    defaults.update(overrides)
    return MetricDefinition.objects.create(**defaults)


def build_alert_payload(metric_id: int | None = None, metric_rag_level: str | None = None) -> AlertCreate:
    return AlertCreate(
        name="Attendance threshold alert",
        metric_id=metric_id,
        metric_rag_level=metric_rag_level,
        query_config={
            "schema_name": "wrong_schema",
            "table_name": "wrong_table",
            "filters": [],
            "filter_connector": "AND",
            "aggregation": "COUNT",
            "measure_column": None,
            "group_by_column": "district_name",
            "condition_operator": "<",
            "condition_value": 10,
        },
        recipients=["ops@example.com"],
        message="Alert body",
        group_message="",
    )


def create_metric_backed_alert(orguser: OrgUser, metric: MetricDefinition) -> Alert:
    return Alert.objects.create(
        name="Linked alert",
        org=orguser.org,
        created_by=orguser,
        metric=metric,
        metric_rag_level="amber",
        query_config=AlertService._build_metric_backed_query_config(metric).to_dict(),
        recipients=["ops@example.com"],
        message="Alert body",
        group_message="",
    )


def test_create_and_list_metric_backed_alert_uses_metric_owned_query_fields(api_request, orguser):
    metric = create_metric(orguser)

    response = create_alert.__wrapped__(
        api_request,
        build_alert_payload(metric_id=metric.id, metric_rag_level="red"),
    )
    alert = Alert.objects.get(id=response["data"]["id"])
    config = alert.get_query_config()

    assert response["success"] is True
    assert response["data"]["metric_id"] == metric.id
    assert response["data"]["metric_name"] == metric.name
    assert response["data"]["metric_rag_level"] == "red"
    assert config.schema_name == metric.schema_name
    assert config.table_name == metric.table_name
    assert config.aggregation == "SUM"
    assert config.measure_column == metric.column
    assert config.group_by_column is None
    assert config.condition_operator == "="
    assert config.condition_value == 0

    list_response = list_alerts.__wrapped__(api_request, metric_id=metric.id)
    assert list_response["data"]["total"] == 1
    listed = list_response["data"]["data"][0]
    assert listed["name"] == alert.name
    assert listed["metric_name"] == metric.name
    assert listed["metric_rag_level"] == "red"
    assert listed["query_config"]["schema_name"] == metric.schema_name


def test_updating_metric_syncs_linked_alert_query_config_and_delete_is_blocked(
    api_request, orguser
):
    metric = create_metric(orguser)
    alert = create_metric_backed_alert(orguser, metric)

    update_metric.__wrapped__(
        api_request,
        metric.id,
        MetricUpdate(
            schema_name="marts",
            table_name="attendance_rollup",
            column="present_students",
            aggregation="max",
        ),
    )

    alert.refresh_from_db()
    updated_config = alert.get_query_config()
    assert updated_config.schema_name == "marts"
    assert updated_config.table_name == "attendance_rollup"
    assert updated_config.measure_column == "present_students"
    assert updated_config.aggregation == "MAX"

    with pytest.raises(HttpError) as excinfo:
        delete_metric.__wrapped__(api_request, metric.id)

    assert excinfo.value.status_code == 400
    assert "linked" in str(excinfo.value).lower()


def test_fired_alert_history_returns_rendered_message_and_trigger_flow_context(
    api_request, orguser
):
    metric = create_metric(orguser)
    alert = create_metric_backed_alert(orguser, metric)

    AlertEvaluation.objects.create(
        alert=alert,
        query_config=alert.query_config,
        query_executed="Metric RAG evaluation",
        recipients=alert.recipients,
        message=alert.message,
        fired=True,
        rows_returned=1,
        result_preview=[
            {
                "alert_value": 72.0,
                "rag_status": "amber",
                "achievement_pct": 72.0,
                "selected_rag_level": "amber",
            }
        ],
        rendered_message="Attendance metric is amber at 72.0",
        trigger_flow_run_id="flow-run-123",
    )

    response = list_fired_alerts.__wrapped__(api_request, metric_id=metric.id)

    assert response["success"] is True
    assert response["data"]["total"] == 1
    event = response["data"]["data"][0]
    assert event["alert_id"] == alert.id
    assert event["metric_name"] == metric.name
    assert event["metric_rag_level"] == "amber"
    assert event["rendered_message"] == "Attendance metric is amber at 72.0"
    assert event["trigger_flow_run_id"] == "flow-run-123"

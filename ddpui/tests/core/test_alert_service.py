import os
import django
from decimal import Decimal

import pytest
from django.contrib.auth.models import User
from sqlalchemy import create_engine
from unittest.mock import patch

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from ddpui.core.alerts.alert_service import AlertService
from ddpui.models.alert import Alert, AlertEvaluation, AlertQueryConfig
from ddpui.models.metrics import MetricDefinition
from ddpui.models.org import Org, OrgDataFlowv1
from ddpui.models.org_user import OrgUser
from ddpui.models.role_based_access import Role
from ddpui.models.tasks import DataflowOrgTask, OrgTask, Task, TaskType
from ddpui.tests.api_tests.test_user_org_api import seed_db

pytestmark = pytest.mark.django_db


@pytest.fixture
def org():
    return Org.objects.create(name="Service Org", slug="service-org")


@pytest.fixture
def orguser(org, seed_db):
    user = User.objects.create(username="service-user", email="service@example.com")
    role = Role.objects.first()
    return OrgUser.objects.create(org=org, user=user, new_role=role)


@pytest.fixture
def metric(orguser):
    return MetricDefinition.objects.create(
        name="Attendance metric",
        schema_name="analytics",
        table_name="attendance_fact",
        column="attendance",
        aggregation="avg",
        direction="increase",
        time_column=None,
        time_grain="month",
        target_value=1.0,
        amber_threshold_pct=80,
        green_threshold_pct=100,
        program_tag="Health",
        metric_type_tag="Output",
        trend_periods=12,
        display_order=0,
        created_by=orguser,
        org=orguser.org,
    )


@pytest.fixture
def active_alert(orguser):
    return Alert.objects.create(
        name="Attendance alert",
        org=orguser.org,
        created_by=orguser,
        query_config=AlertQueryConfig(
            schema_name="analytics",
            table_name="attendance_fact",
            aggregation="SUM",
            measure_column="attendance",
            condition_operator="<",
            condition_value=10,
            filters=[],
        ).to_dict(),
        recipients=["ops@example.com"],
        message="Alert body",
        group_message="",
        is_active=True,
    )


def attach_task(org: Org, deployment_id: str, task_type: str) -> OrgDataFlowv1:
    dataflow = OrgDataFlowv1.objects.create(
        org=org,
        name=f"{task_type}-flow",
        deployment_id=deployment_id,
        dataflow_type="manual",
    )
    task = Task.objects.create(
        type=task_type,
        slug=f"{task_type}-task",
        label=f"{task_type} task",
        is_system=False,
    )
    orgtask = OrgTask.objects.create(org=org, task=task)
    DataflowOrgTask.objects.create(dataflow=dataflow, orgtask=orgtask)
    return dataflow


@pytest.mark.parametrize("task_type", [TaskType.DBT.value, TaskType.DBTCLOUD.value])
def test_completed_transform_flows_evaluate_and_send_alerts(org, active_alert, task_type):
    dataflow = attach_task(org, f"{task_type}-deployment", task_type)

    with patch(
        "ddpui.core.alerts.alert_service.AlertService.evaluate_alert",
        return_value=(True, 2, "Rendered alert body"),
    ) as mock_evaluate, patch(
        "ddpui.core.alerts.alert_service.send_alert_emails"
    ) as mock_send_emails:
        summary = AlertService.evaluate_alerts_for_completed_flow(
            org=org,
            deployment_id=dataflow.deployment_id,
            trigger_flow_run_id=f"{task_type}-flow-run",
        )

    assert summary == {"evaluated": 1, "fired": 1}
    mock_evaluate.assert_called_once_with(
        active_alert,
        trigger_flow_run_id=f"{task_type}-flow-run",
    )
    mock_send_emails.assert_called_once_with(active_alert, "Rendered alert body")


@pytest.mark.parametrize("task_type", [TaskType.AIRBYTE.value, TaskType.GIT.value])
def test_non_transform_flows_do_not_evaluate_alerts(org, active_alert, task_type):
    dataflow = attach_task(org, f"{task_type}-deployment", task_type)

    with patch(
        "ddpui.core.alerts.alert_service.AlertService.evaluate_alert"
    ) as mock_evaluate, patch(
        "ddpui.core.alerts.alert_service.send_alert_emails"
    ) as mock_send_emails:
        summary = AlertService.evaluate_alerts_for_completed_flow(
            org=org,
            deployment_id=dataflow.deployment_id,
            trigger_flow_run_id=f"{task_type}-flow-run",
        )

    assert summary == {"evaluated": 0, "fired": 0}
    mock_evaluate.assert_not_called()
    mock_send_emails.assert_not_called()


def test_evaluate_alert_serializes_decimal_result_preview(orguser, active_alert):
    class FakeWarehouseClient:
        engine = create_engine("sqlite://")

        def execute(self, _sql):
            return [
                {
                    "district_name": "North",
                    "alert_value": Decimal("0.6666666667"),
                }
            ]

    active_alert.set_query_config(
        AlertQueryConfig(
            schema_name="analytics",
            table_name="attendance_fact",
            aggregation="AVG",
            measure_column="attendance",
            group_by_column="district_name",
            condition_operator="<",
            condition_value=0.9,
            filters=[],
        )
    )
    active_alert.message = "The following {{group_by_column}} values failed {{alert_name}}."
    active_alert.group_message = "District: {{group_by_value}}\nAttendance: {{alert_value}}"
    active_alert.save()

    with patch.object(AlertService, "_get_warehouse_client", return_value=FakeWarehouseClient()):
        fired, rows_count, rendered_message = AlertService.evaluate_alert(
            active_alert,
            trigger_flow_run_id="decimal-flow-run",
        )

    evaluation = AlertEvaluation.objects.get(
        alert=active_alert,
        trigger_flow_run_id="decimal-flow-run",
    )

    assert fired is True
    assert rows_count == 1
    assert "District: North" in rendered_message
    assert evaluation.result_preview == [
        {
            "district_name": "North",
            "alert_value": "0.6666666667",
        }
    ]


def test_metric_rag_alert_fires_when_metric_matches_selected_level(orguser, metric):
    alert = Alert.objects.create(
        name="Attendance metric is red",
        org=orguser.org,
        created_by=orguser,
        metric=metric,
        metric_rag_level="red",
        query_config=AlertService._build_metric_backed_query_config(metric).to_dict(),
        recipients=["ops@example.com"],
        message="{{alert_name}} at {{alert_value}}.",
        group_message="",
        is_active=True,
    )

    with patch.object(
        AlertService,
        "_get_metric_rag_state",
        return_value={
            "alert_value": 0.7,
            "rag_status": "red",
            "achievement_pct": 70.0,
            "target_value": 1.0,
            "selected_rag_level": None,
        },
    ):
        fired, rows_count, rendered_message = AlertService.evaluate_alert(
            alert,
            trigger_flow_run_id="metric-rag-run",
        )

    evaluation = AlertEvaluation.objects.get(
        alert=alert,
        trigger_flow_run_id="metric-rag-run",
    )

    assert fired is True
    assert rows_count == 1
    assert "Attendance metric is red at 0.7." in rendered_message
    assert evaluation.rows_returned == 1
    assert evaluation.result_preview == [
        {
            "alert_value": 0.7,
            "rag_status": "red",
            "achievement_pct": 70.0,
            "target_value": 1.0,
            "selected_rag_level": "red",
        }
    ]

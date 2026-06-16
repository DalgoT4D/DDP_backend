"""API tests for Alert endpoints — CRUD + listing + permissions."""

import os
import django
import pytest
from ninja.errors import HttpError

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from django.contrib.auth.models import User

from ddpui.api.alert_api import (
    create_alert,
    delete_alert,
    get_alert,
    get_alert_logs,
    list_alerts,
    toggle_alert,
    update_alert,
)
from ddpui.api.alert_api import test_alert as run_dry_run
from ddpui.api.alert_api import test_slack_webhook as run_slack_webhook_test
from ddpui.auth import ACCOUNT_MANAGER_ROLE
from ddpui.models.alert import Alert
from ddpui.models.metric import KPI, Metric
from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser
from ddpui.models.role_based_access import Role
from ddpui.schemas.alert_schema import (
    AlertCreate,
    AlertTestRequest,
    AlertToggle,
    AlertUpdate,
    RecipientIn,
    SlackTestRequest,
    StandaloneConfig,
    ThresholdCondition,
    RagCondition,
)
from ddpui.tests.api_tests.test_user_org_api import seed_db, mock_request


pytestmark = pytest.mark.django_db


# ── Fixtures ────────────────────────────────────────────────────────────────


@pytest.fixture
def authuser():
    user = User.objects.create(
        username="alertapiuser", email="alertapiuser@test.com", password="testpassword"
    )
    yield user
    user.delete()


@pytest.fixture
def org():
    org = Org.objects.create(
        name="Alert API Test Org",
        slug="alert-api-test",
        airbyte_workspace_id="workspace-id",
    )
    yield org
    org.delete()


@pytest.fixture
def orguser(authuser, org):
    orguser = OrgUser.objects.create(
        user=authuser,
        org=org,
        new_role=Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first(),
    )
    yield orguser
    orguser.delete()


@pytest.fixture
def sample_metric(orguser, org):
    metric = Metric.objects.create(
        name="Alert API Metric",
        schema_name="public",
        table_name="students",
        column="id",
        aggregation="count",
        org=org,
        created_by=orguser,
    )
    yield metric
    try:
        metric.refresh_from_db()
        metric.delete()
    except Metric.DoesNotExist:
        pass


@pytest.fixture
def sample_kpi(orguser, org, sample_metric):
    kpi = KPI.objects.create(
        name="Alert API KPI",
        metric=sample_metric,
        target_value=1000.0,
        direction="increase",
        time_grain="monthly",
        org=org,
        created_by=orguser,
    )
    yield kpi
    try:
        kpi.refresh_from_db()
        kpi.delete()
    except KPI.DoesNotExist:
        pass


def _base_payload(orguser, **overrides):
    payload = dict(
        name="Test alert",
        alert_type="metric_threshold",
        condition=ThresholdCondition(operator="lt", value=50),
        schedule_cron="0 9 * * *",
        delivery_channels=["email"],
        slack_webhook_url=None,
        message_template="Alert: {{alert_name}} - value {{current_value}}",
        recipients=[RecipientIn(type="orguser", orguser_id=orguser.id)],
    )
    payload.update(overrides)
    return AlertCreate(**payload)


# ── Create ──────────────────────────────────────────────────────────────────


def test_create_metric_threshold_alert(seed_db, orguser, sample_metric):
    request = mock_request(orguser)
    payload = _base_payload(orguser, metric_id=sample_metric.id)

    response = create_alert(request, payload)

    assert response.id is not None
    assert response.name == "Test alert"
    assert response.alert_type == "metric_threshold"
    assert response.metric_id == sample_metric.id
    assert response.metric_name == "Alert API Metric"
    assert response.condition.operator == "lt"
    assert response.condition.value == 50.0
    assert response.is_active is True
    assert len(response.recipients) == 1
    assert response.recipients[0].type == "orguser"
    assert response.recipients[0].orguser_id == orguser.id


def test_create_kpi_rag_alert(seed_db, orguser, sample_kpi):
    request = mock_request(orguser)
    payload = _base_payload(
        orguser,
        name="KPI alert",
        alert_type="kpi_rag",
        kpi_id=sample_kpi.id,
        condition=RagCondition(rag_states=["red", "amber"]),
    )

    response = create_alert(request, payload)

    assert response.kpi_id == sample_kpi.id
    assert response.kpi_name == "Alert API KPI"
    assert response.condition.rag_states == ["red", "amber"]


def test_create_standalone_alert(seed_db, orguser):
    request = mock_request(orguser)
    payload = _base_payload(
        orguser,
        name="Standalone alert",
        alert_type="standalone",
        standalone_config=StandaloneConfig(
            schema_name="public",
            table_name="students",
            column="id",
            aggregation="count",
        ),
    )

    response = create_alert(request, payload)

    assert response.alert_type == "standalone"
    assert response.metric_id is None
    assert response.kpi_id is None
    assert response.standalone_config.schema_name == "public"
    assert response.standalone_config.table_name == "students"


def test_create_alert_with_external_recipient(seed_db, orguser, sample_metric):
    request = mock_request(orguser)
    payload = _base_payload(
        orguser,
        metric_id=sample_metric.id,
        recipients=[
            RecipientIn(type="orguser", orguser_id=orguser.id),
            RecipientIn(type="external", email="funder@example.org"),
        ],
    )

    response = create_alert(request, payload)

    assert len(response.recipients) == 2
    external = [r for r in response.recipients if r.type == "external"]
    assert len(external) == 1
    assert external[0].email == "funder@example.org"


def test_create_alert_with_slack_requires_webhook_url(seed_db, orguser, sample_metric):
    request = mock_request(orguser)
    payload = _base_payload(
        orguser,
        metric_id=sample_metric.id,
        delivery_channels=["email", "slack"],
        slack_webhook_url=None,
    )

    with pytest.raises(HttpError, match="slack_webhook_url is required"):
        create_alert(request, payload)


def test_create_alert_with_slack_url_masked_on_response(seed_db, orguser, sample_metric):
    request = mock_request(orguser)
    payload = _base_payload(
        orguser,
        metric_id=sample_metric.id,
        delivery_channels=["email", "slack"],
        slack_webhook_url="https://hooks.slack.com/services/TXXX/BYYY/secrettoken",
    )

    response = create_alert(request, payload)

    assert "secrettoken" not in (response.slack_webhook_url_masked or "")
    assert response.slack_webhook_url_masked.endswith("/services/****")


def test_create_alert_rejects_invalid_cron(seed_db, orguser, sample_metric):
    request = mock_request(orguser)
    payload = _base_payload(orguser, metric_id=sample_metric.id, schedule_cron="not a cron")

    with pytest.raises(HttpError, match="Invalid cron"):
        create_alert(request, payload)


def test_create_alert_rejects_duplicate_name(seed_db, orguser, sample_metric):
    request = mock_request(orguser)
    payload = _base_payload(orguser, metric_id=sample_metric.id)

    create_alert(request, payload)

    with pytest.raises(HttpError, match="already exists"):
        create_alert(request, payload)


def test_create_metric_alert_missing_metric_id(seed_db, orguser):
    request = mock_request(orguser)
    payload = _base_payload(orguser)  # no metric_id

    with pytest.raises(HttpError, match="metric_id is required"):
        create_alert(request, payload)


def test_create_kpi_alert_with_three_rag_states_blocked(seed_db, orguser, sample_kpi):
    request = mock_request(orguser)
    # Pydantic Literal already accepts these, but our service-layer rule blocks >2
    payload = _base_payload(
        orguser,
        name="Bad KPI alert",
        alert_type="kpi_rag",
        kpi_id=sample_kpi.id,
        condition=RagCondition(rag_states=["red", "amber", "green"]),
    )

    with pytest.raises(HttpError, match="1 or 2 RAG states"):
        create_alert(request, payload)


# ── Read / List ─────────────────────────────────────────────────────────────


def test_get_alert(seed_db, orguser, sample_metric):
    request = mock_request(orguser)
    created = create_alert(request, _base_payload(orguser, metric_id=sample_metric.id))

    fetched = get_alert(request, created.id)

    assert fetched.id == created.id
    assert fetched.name == "Test alert"


def test_get_alert_not_found(seed_db, orguser):
    request = mock_request(orguser)
    with pytest.raises(HttpError, match="not found"):
        get_alert(request, 99999)


def test_list_alerts(seed_db, orguser, sample_metric):
    request = mock_request(orguser)
    create_alert(request, _base_payload(orguser, metric_id=sample_metric.id, name="A"))
    create_alert(request, _base_payload(orguser, metric_id=sample_metric.id, name="B"))
    create_alert(request, _base_payload(orguser, metric_id=sample_metric.id, name="C"))

    listing = list_alerts(request)

    assert listing.total == 3
    names = {item.name for item in listing.data}
    assert names == {"A", "B", "C"}


def test_list_alerts_filter_by_active(seed_db, orguser, sample_metric):
    request = mock_request(orguser)
    a1 = create_alert(request, _base_payload(orguser, metric_id=sample_metric.id, name="A1"))
    create_alert(request, _base_payload(orguser, metric_id=sample_metric.id, name="A2"))
    toggle_alert(request, a1.id, AlertToggle(is_active=False))

    enabled = list_alerts(request, is_active=True)
    disabled = list_alerts(request, is_active=False)

    assert enabled.total == 1
    assert disabled.total == 1
    assert disabled.data[0].name == "A1"


# ── Update / Toggle / Delete ────────────────────────────────────────────────


def test_update_alert(seed_db, orguser, sample_metric):
    request = mock_request(orguser)
    created = create_alert(request, _base_payload(orguser, metric_id=sample_metric.id))

    updated = update_alert(
        request,
        created.id,
        AlertUpdate(name="Renamed", condition=ThresholdCondition(operator="gt", value=100)),
    )

    assert updated.name == "Renamed"
    assert updated.condition.operator == "gt"
    assert updated.condition.value == 100.0


def test_toggle_alert(seed_db, orguser, sample_metric):
    request = mock_request(orguser)
    created = create_alert(request, _base_payload(orguser, metric_id=sample_metric.id))

    off = toggle_alert(request, created.id, AlertToggle(is_active=False))
    assert off.is_active is False

    on = toggle_alert(request, created.id, AlertToggle(is_active=True))
    assert on.is_active is True


def test_delete_alert(seed_db, orguser, sample_metric):
    request = mock_request(orguser)
    created = create_alert(request, _base_payload(orguser, metric_id=sample_metric.id))

    delete_alert(request, created.id)

    assert not Alert.objects.filter(id=created.id).exists()


def test_metric_delete_cascades_to_alert(seed_db, orguser, sample_metric):
    """Deleting a Metric silently deletes its Alert (on_delete=CASCADE)."""
    request = mock_request(orguser)
    created = create_alert(request, _base_payload(orguser, metric_id=sample_metric.id))

    Metric.objects.filter(id=sample_metric.id).delete()

    assert not Alert.objects.filter(id=created.id).exists()


# ── Logs ────────────────────────────────────────────────────────────────────


def test_get_alert_logs_empty(seed_db, orguser, sample_metric):
    request = mock_request(orguser)
    created = create_alert(request, _base_payload(orguser, metric_id=sample_metric.id))

    logs = get_alert_logs(request, created.id)

    assert logs.total == 0
    assert logs.data == []


# ── Slack webhook test ──────────────────────────────────────────────────────


def test_slack_webhook_endpoint_success(seed_db, orguser, monkeypatch):
    """Mocked 200 response returns success=True."""

    class _MockResp:
        status_code = 200
        text = "ok"

    def fake_post(url, json, timeout):  # noqa: ANN001
        assert url == "https://hooks.slack.com/services/T/B/X"
        assert json == {"text": "This is a test message from Dalgo platform"}
        assert timeout == 10
        return _MockResp()

    import requests

    monkeypatch.setattr(requests, "post", fake_post)

    request = mock_request(orguser)
    result = run_slack_webhook_test(
        request,
        SlackTestRequest(webhook_url="https://hooks.slack.com/services/T/B/X"),
    )

    assert result.success is True
    assert result.http_status == 200
    assert result.response_body == "ok"


def test_slack_webhook_endpoint_failure_status(seed_db, orguser, monkeypatch):
    """Non-2xx response returns success=False with HTTP status + body."""

    class _MockResp:
        status_code = 404
        text = "no_service"

    import requests

    monkeypatch.setattr(requests, "post", lambda *a, **kw: _MockResp())

    request = mock_request(orguser)
    result = run_slack_webhook_test(
        request,
        SlackTestRequest(webhook_url="https://hooks.slack.com/services/bad"),
    )

    assert result.success is False
    assert result.http_status == 404
    assert "no_service" in result.response_body


def test_slack_webhook_endpoint_network_error(seed_db, orguser, monkeypatch):
    """RequestException → success=False, http_status=0, error captured in body."""
    import requests

    def boom(*a, **kw):  # noqa: ANN001
        raise requests.ConnectionError("DNS resolution failed")

    monkeypatch.setattr(requests, "post", boom)

    request = mock_request(orguser)
    result = run_slack_webhook_test(
        request,
        SlackTestRequest(webhook_url="https://hooks.slack.com/services/nope"),
    )

    assert result.success is False
    assert result.http_status == 0
    assert "DNS resolution failed" in result.response_body


def test_slack_webhook_endpoint_rejects_empty_url(seed_db, orguser):
    request = mock_request(orguser)
    with pytest.raises(HttpError) as exc:
        run_slack_webhook_test(request, SlackTestRequest(webhook_url=""))
    assert "webhook_url" in str(exc.value)


# ── Dry-run (POST /api/alerts/test/) ────────────────────────────────────────


def _patch_query_for_dry_run(monkeypatch, value, rag_status=None, sql="SELECT 1"):
    from ddpui.core.alerts import alert_service as alert_svc

    monkeypatch.setattr(
        alert_svc.alert_query,
        "compute_from_config",
        lambda alert_type, ow, **kw: (value, sql, rag_status),
    )


def _ensure_warehouse(orguser):
    from ddpui.models.org import OrgWarehouse

    OrgWarehouse.objects.get_or_create(org=orguser.org, defaults={"wtype": "postgres"})


def test_dry_run_returns_would_fire_true_when_condition_met(
    seed_db, orguser, sample_metric, monkeypatch
):
    _ensure_warehouse(orguser)
    _patch_query_for_dry_run(monkeypatch, value=10.0)
    request = mock_request(orguser)

    payload = AlertTestRequest(
        name="Test",
        alert_type="metric_threshold",
        metric_id=sample_metric.id,
        condition=ThresholdCondition(operator="lt", value=50),
        delivery_channels=["email"],
        message_template="{{alert_name}}: {{current_value}}",
    )

    out = run_dry_run(request, payload)
    assert out.would_fire is True
    assert out.current_value == 10.0
    assert out.sql_executed == "SELECT 1"
    assert "Test" in out.message
    assert "10" in out.message
    assert out.error is None


def test_dry_run_returns_would_fire_false_when_condition_not_met(
    seed_db, orguser, sample_metric, monkeypatch
):
    _ensure_warehouse(orguser)
    _patch_query_for_dry_run(monkeypatch, value=999.0)
    request = mock_request(orguser)

    payload = AlertTestRequest(
        name="Test",
        alert_type="metric_threshold",
        metric_id=sample_metric.id,
        condition=ThresholdCondition(operator="lt", value=50),
        delivery_channels=["email"],
        message_template="{{current_value}} vs {{target_value}}",
    )

    out = run_dry_run(request, payload)
    assert out.would_fire is False
    assert out.current_value == 999.0


def test_dry_run_returns_error_when_no_warehouse(seed_db, orguser, sample_metric):
    """No warehouse → error message, no crash."""
    request = mock_request(orguser)

    payload = AlertTestRequest(
        name="Test",
        alert_type="metric_threshold",
        metric_id=sample_metric.id,
        condition=ThresholdCondition(operator="lt", value=50),
        delivery_channels=["email"],
        message_template="X",
    )

    out = run_dry_run(request, payload)
    assert out.would_fire is False
    assert "warehouse" in (out.error or "").lower()


def test_dry_run_kpi_rag_uses_rag_status_for_evaluation(seed_db, orguser, sample_kpi, monkeypatch):
    """For kpi_rag alerts the condition is checked against the RAG status, not the raw value."""
    _ensure_warehouse(orguser)
    _patch_query_for_dry_run(monkeypatch, value=500.0, rag_status="red")
    request = mock_request(orguser)

    payload = AlertTestRequest(
        name="KPI watch",
        alert_type="kpi_rag",
        kpi_id=sample_kpi.id,
        condition=RagCondition(rag_states=["red", "amber"]),
        delivery_channels=["email"],
        message_template="{{kpi_name}} is {{rag_status}}",
    )

    out = run_dry_run(request, payload)
    assert out.would_fire is True
    assert "red" in out.message

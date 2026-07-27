"""Alert API endpoints."""

from typing import Optional

from ninja import Router
from ninja.errors import HttpError

from ddpui.auth import has_permission
from ddpui.core.alerts import condition as condition_helpers
from ddpui.core.alerts import scheduling
from ddpui.core.alerts.alert_service import AlertService
from ddpui.core.alerts.exceptions import (
    AlertNotFoundError,
    AlertPermissionError,
    AlertValidationError,
)
from ddpui.core.audit_log_service import create_audit_log
from ddpui.models.alert import Alert, AlertLog, AlertType
from ddpui.models.audit_log import AuditLogAction, AuditLogResourceType
from ddpui.models.metric import KPI, Metric
from ddpui.models.org_user import OrgUser
from ddpui.schemas.alert_schema import (
    AlertCreate,
    AlertListItem,
    AlertListResponse,
    AlertLogOut,
    AlertResponse,
    AlertTestRequest,
    AlertTestResponse,
    AlertToggle,
    AlertUpdate,
    KpiRagContext,
    LogDeliveryOut,
    LogListResponse,
    RecipientOut,
    SlackTestRequest,
    SlackTestResponse,
    mask_webhook_url,
)
from ddpui.utils.custom_logger import CustomLogger
from ddpui.utils.response_wrapper import api_response


logger = CustomLogger("ddpui")

alert_router = Router()


# ── Response builders ──────────────────────────────────────────────────────


def _build_recipient_out(recipients: list, org_id: int) -> list[RecipientOut]:
    """Render stored JSON recipients with OrgUser names resolved."""
    out: list[RecipientOut] = []
    orguser_ids = [r["orguser_id"] for r in recipients if r.get("type") == "orguser"]
    name_by_id: dict[int, str] = {}
    if orguser_ids:
        for ou in OrgUser.objects.filter(id__in=orguser_ids, org_id=org_id).select_related("user"):
            full = (ou.user.first_name + " " + ou.user.last_name).strip()
            name_by_id[ou.id] = full or ou.user.email

    for r in recipients:
        if r.get("type") == "orguser":
            out.append(
                RecipientOut(
                    type="orguser",
                    orguser_id=r.get("orguser_id"),
                    orguser_name=name_by_id.get(r.get("orguser_id")),
                )
            )
        else:
            out.append(RecipientOut(type="external", email=r.get("email")))
    return out


def _resolve_alert_source_label(
    org_id: int,
    alert_type: str,
    metric_id: Optional[int],
    kpi_id: Optional[int],
    standalone_config,
) -> Optional[str]:
    """Human-readable label for what an alert is monitoring, for audit logging
    (a bare metric/kpi id isn't useful to a human reading the log later)."""
    if alert_type == AlertType.METRIC_THRESHOLD and metric_id:
        metric = Metric.objects.filter(id=metric_id, org_id=org_id).first()
        return metric.name if metric else str(metric_id)
    if alert_type == AlertType.KPI_RAG and kpi_id:
        kpi = KPI.objects.filter(id=kpi_id, org_id=org_id).first()
        return kpi.name if kpi else str(kpi_id)
    if alert_type == AlertType.STANDALONE and standalone_config:
        return f"{standalone_config.schema_name}.{standalone_config.table_name}"
    return None


def _summarize_recipients(recipients: list, org_id: int) -> list[str]:
    """Human-readable recipient targets (names/emails) for audit logging, not raw ids."""
    return [
        r.orguser_name if r.type == "orguser" else r.email
        for r in _build_recipient_out(recipients, org_id)
    ]


def _build_alert_response(alert: Alert) -> AlertResponse:
    return AlertResponse(
        id=alert.id,
        name=alert.name,
        alert_type=alert.alert_type,
        metric_id=alert.metric_id,
        metric_name=alert.metric.name if alert.metric else None,
        kpi_id=alert.kpi_id,
        kpi_name=alert.kpi.name if alert.kpi else None,
        standalone_config=alert.standalone_config,
        condition=alert.condition,
        schedule_cron=alert.schedule_cron,
        delivery_channels=alert.delivery_channels,
        slack_webhook_url_masked=mask_webhook_url(alert.slack_webhook_url),
        message_template=alert.message_template,
        is_active=alert.is_active,
        last_evaluated_at=alert.last_evaluated_at,
        recipients=_build_recipient_out(alert.recipients or [], alert.org_id),
        created_at=alert.created_at,
        updated_at=alert.updated_at,
    )


def _build_list_item(alert: Alert) -> AlertListItem:
    if alert.alert_type == AlertType.METRIC_THRESHOLD:
        source_kind = "metric"
        source_id = alert.metric_id
        source_name = alert.metric.name if alert.metric else None
    elif alert.alert_type == AlertType.KPI_RAG:
        source_kind = "kpi"
        source_id = alert.kpi_id
        source_name = alert.kpi.name if alert.kpi else None
    else:
        source_kind = "dataset"
        source_id = None
        cfg = alert.standalone_config or {}
        schema = cfg.get("schema_name") or ""
        table = cfg.get("table_name") or ""
        source_name = f"{schema}.{table}".strip(".") if (schema or table) else None

    # Last log entry — gives us last_fire_at
    latest_log = (
        AlertLog.objects.filter(alert=alert, fired=True)
        .order_by("-evaluated_at")
        .only("evaluated_at")
        .first()
    )
    last_fire_at = latest_log.evaluated_at if latest_log else None

    rag_states = None
    kpi_rag_context = None
    if alert.alert_type == AlertType.KPI_RAG:
        if isinstance(alert.condition, dict):
            rs = alert.condition.get("rag_states")
            if isinstance(rs, list):
                rag_states = rs
        if alert.kpi is not None:
            kpi_rag_context = KpiRagContext(
                target_value=alert.kpi.target_value,
                direction=alert.kpi.direction,
                green_threshold_pct=alert.kpi.green_threshold_pct,
                amber_threshold_pct=alert.kpi.amber_threshold_pct,
            )

    return AlertListItem(
        id=alert.id,
        name=alert.name,
        alert_type=alert.alert_type,
        source_kind=source_kind,
        source_id=source_id,
        source_name=source_name,
        condition_pretty=condition_helpers.pretty(alert.alert_type, alert.condition),
        rag_states=rag_states,
        kpi_rag_context=kpi_rag_context,
        schedule_frequency=scheduling.derive_frequency_label(alert.schedule_cron),
        schedule_cron=alert.schedule_cron,
        is_active=alert.is_active,
        last_fire_at=last_fire_at,
        fire_streak=AlertService.compute_fire_streak(alert),
    )


def _build_log_out(log: AlertLog) -> AlertLogOut:
    snap = log.alert_snapshot or {}
    deliveries = [
        LogDeliveryOut(
            channel=d.get("channel"),
            target=d.get("target"),
            status=d.get("status"),
            error_reason=d.get("error_reason"),
            http_status=d.get("http_status"),
            sent_at=d.get("sent_at"),
        )
        for d in (log.deliveries or [])
    ]
    rag_status = snap.get("rag_status")
    if rag_status not in ("red", "amber", "green"):
        rag_status = None
    return AlertLogOut(
        id=log.id,
        scheduled_for=log.scheduled_for,
        evaluated_at=log.evaluated_at,
        value=log.value,
        fired=log.fired,
        rag_status=rag_status,
        condition_pretty=condition_helpers.pretty(
            snap.get("alert_type"), snap.get("condition") or {}
        ),
        sql_executed=log.sql_executed,
        message=log.message,
        deliveries=deliveries,
    )


# ── Endpoints ───────────────────────────────────────────────────────────────


@alert_router.get("/", response=AlertListResponse)
@has_permission(["can_view_alerts"])
def list_alerts(
    request,
    page: int = 1,
    page_size: int = 10,
    search: Optional[str] = None,
    is_active: Optional[bool] = None,
    frequency: Optional[str] = None,
):
    """Paginated list of alerts for the org."""
    orguser: OrgUser = request.orguser
    if page < 1:
        page = 1
    if page_size < 1 or page_size > 100:
        page_size = 10

    alerts, total = AlertService.list_alerts(
        org=orguser.org,
        page=page,
        page_size=page_size,
        search=search,
        is_active=is_active,
        frequency=frequency,
    )
    total_pages = (total + page_size - 1) // page_size if total else 0
    return AlertListResponse(
        data=[_build_list_item(a) for a in alerts],
        total=total,
        page=page,
        page_size=page_size,
        total_pages=total_pages,
    )


@alert_router.post("/", response=AlertResponse)
@has_permission(["can_create_alerts"])
def create_alert(request, payload: AlertCreate):
    """Create a new alert."""
    orguser: OrgUser = request.orguser
    try:
        alert = AlertService.create_alert(payload, orguser)
    except AlertValidationError as e:
        raise HttpError(400, e.message) from None

    create_audit_log(
        org=orguser.org,
        orguser=orguser,
        resource_type=AuditLogResourceType.ALERT,
        resource_id=str(alert.id),
        resource_name=alert.name,
        action=AuditLogAction.CREATE,
        resource_fields={
            "source": _resolve_alert_source_label(
                orguser.org_id,
                payload.alert_type,
                payload.metric_id,
                payload.kpi_id,
                payload.standalone_config,
            ),
            "condition": payload.condition.model_dump(),
            "schedule_cron": payload.schedule_cron,
            "delivery_channels": payload.delivery_channels,
            "message_template": payload.message_template,
            "recipients": _summarize_recipients(
                [r.model_dump() for r in payload.recipients], orguser.org_id
            ),
            "is_active": True,
        },
    )

    return _build_alert_response(alert)


# ── Static paths declared BEFORE dynamic /{alert_id}/ to keep Ninja from ──
# shadowing them. Django's URL resolver matches in declaration order; a
# path like /test/ would otherwise match /{alert_id}/ (with alert_id="test")
# before reaching the static handler and return 405 for POST.


@alert_router.post("/test/", response=AlertTestResponse)
@has_permission(["can_create_alerts"])
def test_alert(request, payload: AlertTestRequest):
    """Dry-run an alert configuration. No persistence, no delivery."""
    orguser: OrgUser = request.orguser
    return AlertService.dry_run(orguser.org, payload)


@alert_router.post("/test-slack-webhook/", response=SlackTestResponse)
@has_permission(["can_create_alerts"])
def test_slack_webhook(request, payload: SlackTestRequest):
    """POST a fixed test payload to a Slack webhook URL and return the outcome."""
    if not payload.webhook_url or not payload.webhook_url.strip():
        raise HttpError(400, "webhook_url is required")
    success, http_status, body = AlertService.test_slack_webhook(payload.webhook_url.strip())
    return SlackTestResponse(success=success, http_status=http_status, response_body=body)


@alert_router.get("/{alert_id}/", response=AlertResponse)
@has_permission(["can_view_alerts"])
def get_alert(request, alert_id: int):
    orguser: OrgUser = request.orguser
    try:
        alert = AlertService.get_alert(alert_id, orguser.org)
    except AlertNotFoundError:
        raise HttpError(404, "Alert not found") from None
    return _build_alert_response(alert)


@alert_router.put("/{alert_id}/", response=AlertResponse)
@has_permission(["can_edit_alerts"])
def update_alert(request, alert_id: int, payload: AlertUpdate):
    orguser: OrgUser = request.orguser
    try:
        alert = AlertService.update_alert(alert_id, orguser.org, orguser, payload)
    except AlertNotFoundError:
        raise HttpError(404, "Alert not found") from None
    except AlertValidationError as e:
        raise HttpError(400, e.message) from None

    # Only fields actually present in this request are logged — AlertUpdate
    # is a genuine partial patch (AlertService only touches fields that are
    # not None), so a field missing here means "not touched", not "cleared".
    raw_resource_fields = {
        "name": payload.name,
        "source": _resolve_alert_source_label(
            orguser.org_id,
            alert.alert_type,
            payload.metric_id,
            payload.kpi_id,
            payload.standalone_config,
        ),
        "condition": payload.condition.model_dump() if payload.condition is not None else None,
        "schedule_cron": payload.schedule_cron,
        "delivery_channels": payload.delivery_channels,
        "message_template": payload.message_template,
        "recipients": (
            _summarize_recipients([r.model_dump() for r in payload.recipients], orguser.org_id)
            if payload.recipients is not None
            else None
        ),
        "is_active": payload.is_active,
    }

    create_audit_log(
        org=orguser.org,
        orguser=orguser,
        resource_type=AuditLogResourceType.ALERT,
        resource_id=str(alert.id),
        resource_name=alert.name,
        action=AuditLogAction.UPDATE,
        resource_fields={k: v for k, v in raw_resource_fields.items() if v is not None},
    )

    return _build_alert_response(alert)


@alert_router.patch("/{alert_id}/toggle/", response=AlertResponse)
@has_permission(["can_edit_alerts"])
def toggle_alert(request, alert_id: int, payload: AlertToggle):
    """Flip is_active without going through the full update flow."""
    orguser: OrgUser = request.orguser
    try:
        alert = AlertService.toggle_alert(alert_id, orguser.org, orguser, payload.is_active)
    except AlertNotFoundError:
        raise HttpError(404, "Alert not found") from None

    create_audit_log(
        org=orguser.org,
        orguser=orguser,
        resource_type=AuditLogResourceType.ALERT,
        resource_id=str(alert.id),
        resource_name=alert.name,
        action=AuditLogAction.UPDATE,
        resource_fields={"is_active": payload.is_active},
    )

    return _build_alert_response(alert)


@alert_router.delete("/{alert_id}/")
@has_permission(["can_delete_alerts"])
def delete_alert(request, alert_id: int):
    orguser: OrgUser = request.orguser
    try:
        alert = AlertService.get_alert(alert_id, orguser.org)
        AlertService.delete_alert(alert_id, orguser.org, orguser)
    except AlertNotFoundError:
        raise HttpError(404, "Alert not found") from None
    except AlertPermissionError as e:
        raise HttpError(403, e.message) from None

    create_audit_log(
        org=orguser.org,
        orguser=orguser,
        resource_type=AuditLogResourceType.ALERT,
        resource_id=str(alert_id),
        resource_name=alert.name,
        action=AuditLogAction.DELETE,
    )

    return api_response(success=True)


@alert_router.get("/{alert_id}/logs/", response=LogListResponse)
@has_permission(["can_view_alerts"])
def get_alert_logs(request, alert_id: int, page: int = 1, page_size: int = 20):
    """Paginated evaluation history for the Alert log modal."""
    orguser: OrgUser = request.orguser
    if page < 1:
        page = 1
    if page_size < 1 or page_size > 100:
        page_size = 20

    try:
        logs, total = AlertService.get_log(alert_id, orguser.org, page=page, page_size=page_size)
    except AlertNotFoundError:
        raise HttpError(404, "Alert not found") from None

    total_pages = (total + page_size - 1) // page_size if total else 0
    return LogListResponse(
        data=[_build_log_out(l) for l in logs],
        total=total,
        page=page,
        page_size=page_size,
        total_pages=total_pages,
    )

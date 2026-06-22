"""AlertService — CRUD + listing + log queries + dry-run.

The Celery evaluator lives in `ddpui.celeryworkers.alert_tasks` and reuses
`alert_query`, `condition`, `rendering`, and `delivery` modules from
`ddpui.core.alerts`.
"""

from typing import List, Optional, Tuple

from django.db import transaction
from django.db.models import F, OuterRef, Q, Subquery

from ddpui.core.alerts import alert_query
from ddpui.core.alerts import condition as condition_helpers
from ddpui.core.alerts import rendering, scheduling
from ddpui.core.alerts.exceptions import (
    AlertNotFoundError,
    AlertValidationError,
)
from ddpui.models.alert import Alert, AlertLog, AlertType
from ddpui.models.metric import KPI, Metric
from ddpui.models.org import Org, OrgWarehouse
from ddpui.models.org_user import OrgUser
from ddpui.schemas.alert_schema import (
    AlertCreate,
    AlertTestRequest,
    AlertTestResponse,
    AlertUpdate,
)
from ddpui.utils.custom_logger import CustomLogger


logger = CustomLogger("ddpui.alert_service")


VALID_CHANNELS = {"email", "slack"}
VALID_RECIPIENT_TYPES = {"orguser", "external"}


# ── Validation helpers ─────────────────────────────────────────────────────


def _validate_recipients(recipients: list, org: Org) -> None:
    if not recipients:
        raise AlertValidationError("At least one recipient is required")
    for idx, r in enumerate(recipients):
        rtype = r.get("type") if isinstance(r, dict) else r.type
        orguser_id = r.get("orguser_id") if isinstance(r, dict) else r.orguser_id
        email = r.get("email") if isinstance(r, dict) else r.email

        if rtype not in VALID_RECIPIENT_TYPES:
            raise AlertValidationError(f"Recipient[{idx}]: type must be 'orguser' or 'external'")
        if rtype == "orguser":
            if not orguser_id:
                raise AlertValidationError(
                    f"Recipient[{idx}]: orguser_id is required for type='orguser'"
                )
            if not OrgUser.objects.filter(id=orguser_id, org=org).exists():
                raise AlertValidationError(
                    f"Recipient[{idx}]: OrgUser {orguser_id} not in this org"
                )
        else:  # external
            if not email:
                raise AlertValidationError(
                    f"Recipient[{idx}]: email is required for type='external'"
                )


def _validate_delivery_channels(channels: list, slack_webhook_url: Optional[str]) -> None:
    if not channels:
        raise AlertValidationError("At least one delivery channel is required")
    for c in channels:
        if c not in VALID_CHANNELS:
            raise AlertValidationError(f"Invalid delivery channel '{c}'")
    if "slack" in channels and not slack_webhook_url:
        raise AlertValidationError(
            "slack_webhook_url is required when slack is in delivery_channels"
        )


def _validate_source(payload_or_alert) -> None:
    """Verify that the right source is populated for the alert_type."""
    at = getattr(payload_or_alert, "alert_type", None)
    metric_id = getattr(payload_or_alert, "metric_id", None)
    kpi_id = getattr(payload_or_alert, "kpi_id", None)
    standalone_config = getattr(payload_or_alert, "standalone_config", None)

    if at == AlertType.METRIC_THRESHOLD:
        if metric_id is None:
            raise AlertValidationError("metric_id is required for metric_threshold alerts")
        if kpi_id is not None or standalone_config is not None:
            raise AlertValidationError(
                "metric_threshold alerts cannot also set kpi_id or standalone_config"
            )
    elif at == AlertType.KPI_RAG:
        if kpi_id is None:
            raise AlertValidationError("kpi_id is required for kpi_rag alerts")
        if metric_id is not None or standalone_config is not None:
            raise AlertValidationError(
                "kpi_rag alerts cannot also set metric_id or standalone_config"
            )
    elif at == AlertType.STANDALONE:
        if standalone_config is None:
            raise AlertValidationError("standalone_config is required for standalone alerts")
        if metric_id is not None or kpi_id is not None:
            raise AlertValidationError("standalone alerts cannot also set metric_id or kpi_id")
    else:
        raise AlertValidationError(f"Unknown alert_type '{at}'")


def _serialize_recipients(recipients: list) -> list:
    """RecipientIn → dict, dropping None fields so the JSONField stays tidy.

    e.g. a `type="orguser"` recipient doesn't need to persist `email: null`.
    """
    return [{k: v for k, v in r.model_dump().items() if v is not None} for r in recipients]


# ── Service ────────────────────────────────────────────────────────────────


class AlertService:
    """CRUD + listing operations for Alert."""

    # --- read ---

    @staticmethod
    def get_alert(alert_id: int, org: Org) -> Alert:
        try:
            return Alert.objects.select_related("metric", "kpi").get(id=alert_id, org=org)
        except Alert.DoesNotExist:
            raise AlertNotFoundError(alert_id)

    @staticmethod
    def list_alerts(
        org: Org,
        page: int = 1,
        page_size: int = 10,
        search: Optional[str] = None,
        is_active: Optional[bool] = None,
        frequency: Optional[str] = None,  # daily/weekly/monthly
    ) -> Tuple[List[Alert], int]:
        query = Q(org=org)
        if search:
            query &= Q(name__icontains=search)
        if is_active is not None:
            query &= Q(is_active=is_active)

        # Default sort: most-recently-fired first, NULLS LAST (never-fired alerts
        # sink to the bottom). Tie-break by latest update.
        latest_fire = AlertLog.objects.filter(alert=OuterRef("pk"), fired=True).order_by(
            "-evaluated_at"
        )
        queryset = (
            Alert.objects.filter(query)
            .select_related("metric", "kpi")
            .annotate(latest_fire_at=Subquery(latest_fire.values("evaluated_at")[:1]))
            .order_by(F("latest_fire_at").desc(nulls_last=True), "-updated_at")
        )

        # Frequency filter is post-fetch because schedule_cron is a string we
        # parse via scheduling.derive_frequency_label. At v1 scale this is fine.
        if frequency:
            ids = [
                a.id
                for a in queryset.only("id", "schedule_cron")
                if scheduling.derive_frequency_label(a.schedule_cron) == frequency
            ]
            queryset = (
                Alert.objects.filter(id__in=ids)
                .select_related("metric", "kpi")
                .annotate(latest_fire_at=Subquery(latest_fire.values("evaluated_at")[:1]))
                .order_by(F("latest_fire_at").desc(nulls_last=True), "-updated_at")
            )

        total = queryset.count()
        offset = (page - 1) * page_size
        return list(queryset[offset : offset + page_size]), total

    @staticmethod
    def get_log(
        alert_id: int,
        org: Org,
        page: int = 1,
        page_size: int = 20,
    ) -> Tuple[List[AlertLog], int]:
        alert = AlertService.get_alert(alert_id, org)
        queryset = AlertLog.objects.filter(alert=alert).order_by("-evaluated_at")
        total = queryset.count()
        offset = (page - 1) * page_size
        return list(queryset[offset : offset + page_size]), total

    @staticmethod
    def compute_fire_streak(alert: Alert, window: int = 10) -> int:
        """Count consecutive recent fires from the most recent log row backward."""
        logs = list(
            AlertLog.objects.filter(alert=alert)
            .order_by("-evaluated_at")
            .values_list("fired", flat=True)[:window]
        )
        streak = 0
        for fired in logs:
            if fired:
                streak += 1
            else:
                break
        return streak

    # --- write ---

    @staticmethod
    @transaction.atomic
    def create_alert(payload: AlertCreate, orguser: OrgUser) -> Alert:
        org = orguser.org

        # Basic uniqueness
        if Alert.objects.filter(org=org, name=payload.name).exists():
            raise AlertValidationError(f"An alert named '{payload.name}' already exists")

        # Schedule + condition + recipients + delivery
        try:
            scheduling.validate_cron(payload.schedule_cron)
        except ValueError as e:
            raise AlertValidationError(str(e))

        condition_dict = payload.condition.model_dump()
        try:
            condition_helpers.validate_condition(payload.alert_type, condition_dict)
        except ValueError as e:
            raise AlertValidationError(str(e))

        _validate_source(payload)
        _validate_delivery_channels(payload.delivery_channels, payload.slack_webhook_url)

        recipients_dict = _serialize_recipients(payload.recipients)
        _validate_recipients(recipients_dict, org)

        # Resolve FKs
        metric = None
        kpi = None
        if payload.alert_type == AlertType.METRIC_THRESHOLD:
            metric = Metric.objects.filter(id=payload.metric_id, org=org).first()
            if metric is None:
                raise AlertValidationError(f"Metric {payload.metric_id} not in this org")
        elif payload.alert_type == AlertType.KPI_RAG:
            kpi = KPI.objects.filter(id=payload.kpi_id, org=org).first()
            if kpi is None:
                raise AlertValidationError(f"KPI {payload.kpi_id} not in this org")

        alert = Alert(
            org=org,
            name=payload.name,
            alert_type=payload.alert_type,
            metric=metric,
            kpi=kpi,
            standalone_config=(
                payload.standalone_config.model_dump() if payload.standalone_config else None
            ),
            condition=condition_dict,
            schedule_cron=payload.schedule_cron,
            delivery_channels=list(payload.delivery_channels),
            slack_webhook_url=payload.slack_webhook_url,
            message_template=payload.message_template,
            recipients=recipients_dict,
            is_active=True,
            created_by=orguser,
            last_modified_by=orguser,
        )
        alert.save()
        logger.info(f"Created alert {alert.id} '{alert.name}' for org {org.id}")
        return alert

    @staticmethod
    @transaction.atomic
    def update_alert(alert_id: int, org: Org, orguser: OrgUser, payload: AlertUpdate) -> Alert:
        alert = AlertService.get_alert(alert_id, org)

        if payload.name is not None:
            if Alert.objects.filter(org=org, name=payload.name).exclude(id=alert_id).exists():
                raise AlertValidationError(f"An alert named '{payload.name}' already exists")
            alert.name = payload.name

        # Source change — only the field matching the alert's existing alert_type is
        # honored; alert_type itself is immutable on update.
        if payload.metric_id is not None:
            if alert.alert_type != AlertType.METRIC_THRESHOLD:
                raise AlertValidationError(
                    "metric_id can only be updated on metric_threshold alerts"
                )
            try:
                alert.metric = Metric.objects.get(id=payload.metric_id, org=org)
            except Metric.DoesNotExist:
                raise AlertValidationError(f"metric {payload.metric_id} not found")

        if payload.kpi_id is not None:
            if alert.alert_type != AlertType.KPI_RAG:
                raise AlertValidationError("kpi_id can only be updated on kpi_rag alerts")
            try:
                alert.kpi = KPI.objects.get(id=payload.kpi_id, org=org)
            except KPI.DoesNotExist:
                raise AlertValidationError(f"kpi {payload.kpi_id} not found")

        if payload.standalone_config is not None:
            if alert.alert_type != AlertType.STANDALONE:
                raise AlertValidationError(
                    "standalone_config can only be updated on standalone alerts"
                )
            alert.standalone_config = payload.standalone_config.model_dump()

        if payload.condition is not None:
            cond_dict = payload.condition.model_dump()
            try:
                condition_helpers.validate_condition(alert.alert_type, cond_dict)
            except ValueError as e:
                raise AlertValidationError(str(e))
            alert.condition = cond_dict

        if payload.schedule_cron is not None:
            try:
                scheduling.validate_cron(payload.schedule_cron)
            except ValueError as e:
                raise AlertValidationError(str(e))
            alert.schedule_cron = payload.schedule_cron

        if payload.delivery_channels is not None:
            # If only channels changed, validate against the existing webhook URL too
            webhook_url = (
                payload.slack_webhook_url
                if payload.slack_webhook_url is not None
                else alert.slack_webhook_url
            )
            _validate_delivery_channels(payload.delivery_channels, webhook_url)
            alert.delivery_channels = list(payload.delivery_channels)

        if payload.slack_webhook_url is not None:
            alert.slack_webhook_url = payload.slack_webhook_url

        if payload.message_template is not None:
            alert.message_template = payload.message_template

        if payload.recipients is not None:
            recipients_dict = _serialize_recipients(payload.recipients)
            _validate_recipients(recipients_dict, org)
            alert.recipients = recipients_dict

        if payload.is_active is not None:
            alert.is_active = payload.is_active

        alert.last_modified_by = orguser
        alert.save()
        logger.info(f"Updated alert {alert.id}")
        return alert

    @staticmethod
    def toggle_alert(alert_id: int, org: Org, orguser: OrgUser, is_active: bool) -> Alert:
        alert = AlertService.get_alert(alert_id, org)
        alert.is_active = is_active
        alert.last_modified_by = orguser
        alert.save(update_fields=["is_active", "last_modified_by", "updated_at"])
        return alert

    @staticmethod
    def delete_alert(alert_id: int, org: Org, orguser: OrgUser) -> None:
        alert = AlertService.get_alert(alert_id, org)
        alert_name = alert.name
        alert.delete()
        logger.info(f"Deleted alert '{alert_name}' (id={alert_id}) by {orguser.user.email}")

    @staticmethod
    def dry_run(org: Org, payload: AlertTestRequest) -> AlertTestResponse:
        """Build SQL, execute, evaluate, render — no persistence, no delivery."""
        org_warehouse = OrgWarehouse.objects.filter(org=org).first()
        if not org_warehouse:
            return AlertTestResponse(
                would_fire=False,
                current_value=None,
                sql_executed="",
                message="",
                error="No warehouse configured for this org",
            )

        try:
            value, sql_str, rag_status = alert_query.compute_from_config(
                payload.alert_type,
                org_warehouse,
                metric_id=payload.metric_id,
                kpi_id=payload.kpi_id,
                standalone_config=(
                    payload.standalone_config.model_dump() if payload.standalone_config else None
                ),
            )
        except AlertValidationError as e:
            return AlertTestResponse(
                would_fire=False, current_value=None, sql_executed="", message="", error=e.message
            )
        except Exception as e:  # pragma: no cover — warehouse errors vary
            logger.error(f"dry_run query failed: {e}")
            return AlertTestResponse(
                would_fire=False, current_value=None, sql_executed="", message="", error=str(e)
            )

        # Convert pydantic condition Union → plain dict for downstream helpers
        cond_dict = payload.condition.model_dump()

        # Evaluator's input is the value for threshold; the RAG state for kpi_rag
        eval_input = rag_status if payload.alert_type == AlertType.KPI_RAG else value
        would_fire = condition_helpers.evaluate(payload.alert_type, cond_dict, eval_input)

        # Resolve names for tokens
        metric_name = None
        kpi_name = None
        dataset_name = None
        target_value = None
        if payload.alert_type == AlertType.METRIC_THRESHOLD and payload.metric_id:
            m = Metric.objects.filter(id=payload.metric_id).first()
            metric_name = m.name if m else None
            target_value = cond_dict.get("value")
        elif payload.alert_type == AlertType.KPI_RAG and payload.kpi_id:
            k = KPI.objects.filter(id=payload.kpi_id).first()
            kpi_name = k.name if k else None
            target_value = k.target_value if k else None
        elif payload.alert_type == AlertType.STANDALONE and payload.standalone_config:
            cfg = payload.standalone_config
            dataset_name = f"{cfg.schema_name}.{cfg.table_name}".strip(".") or None
            target_value = cond_dict.get("value")

        tokens = rendering.resolve_tokens(
            payload.alert_type,
            alert_name=payload.name or "Test alert",
            metric_name=metric_name,
            kpi_name=kpi_name,
            dataset_name=dataset_name,
            target_value=target_value,
            current_value=value,
            rag_status=rag_status,
        )
        message = rendering.render(payload.message_template, tokens)

        return AlertTestResponse(
            would_fire=would_fire,
            current_value=value,
            sql_executed=sql_str,
            message=message,
            error=None,
        )

    @staticmethod
    def test_slack_webhook(webhook_url: str) -> Tuple[bool, int, str]:
        """POST a fixed test payload to a Slack-style webhook URL.

        Returns (success, http_status, response_body). Network errors are
        captured and returned as success=False with http_status=0.
        """
        import requests

        payload = {"text": "This is a test message from Dalgo platform"}
        try:
            response = requests.post(webhook_url, json=payload, timeout=10)
            return (200 <= response.status_code < 300, response.status_code, response.text or "")
        except requests.RequestException as e:
            return (False, 0, str(e))

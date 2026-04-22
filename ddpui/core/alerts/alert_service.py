"""Alert business logic — Batch 3 rework.

Three alert types, each with a distinct evaluation path:

  - threshold  (Metric-backed): Metric value compared to a numeric condition
  - rag        (KPI-backed): current KPI RAG status matches the configured level
  - standalone (ad-hoc SQL): query_config drives the SQL directly

Per-alert notification cooldown gates outgoing notifications — evaluations
still log every time, but the cooldown suppresses repeat sends while the
condition persists. `null` cooldown = "notify only on state change" (default).

Pipeline triggers: empty list = evaluate on every transform-pipeline
completion (current behavior). Non-empty list = only listed deployment IDs.

Recipients are stored as a typed list of ``{type: "email"|"user", ref}`` and
resolved to email addresses at send time.
"""

from datetime import datetime, timedelta
from typing import Literal

from django.db import models as db_models
from django.db.models import Max, Q
from django.utils import timezone

from ddpui.core.alerts.alert_query_builder import (
    build_alert_query_builder,
    build_count_query_builder,
    build_paginated_query_builder,
    compile_query,
)
from ddpui.core.alerts.rendering import normalize_result_rows, render_alert_message
from ddpui.core.alerts.delivery import send_alert_emails
from ddpui.core.alerts.exceptions import (
    AlertNotFoundError,
    AlertValidationError,
    AlertWarehouseError,
)
from ddpui.core.metrics_service import compute_rag_status, fetch_current_value
from ddpui.models.alert import (
    Alert,
    AlertEvaluation,
    AlertQueryConfig,
    AlertRecipient,
)
from ddpui.models.org import Org, OrgWarehouse
from ddpui.models.org_user import OrgUser
from ddpui.models.metrics import KPI, Metric
from ddpui.models.tasks import DataflowOrgTask, TaskType
from ddpui.schemas.alert_schema import AlertCreate, AlertUpdate, AlertTestRequest
from ddpui.utils.custom_logger import CustomLogger
from ddpui.utils.warehouse.client.warehouse_factory import WarehouseFactory

logger = CustomLogger("ddpui.core.alerts")

MetricRagLevel = Literal["red", "amber", "green"]


# ═══════════════════════════════════════════════════════════════════════════
# Recipient normalization
# ═══════════════════════════════════════════════════════════════════════════


def _normalize_recipients_for_storage(raw: list) -> list[dict]:
    """Accepts a mixed list of strings / typed-dicts. Emits the typed-dict form."""
    out: list[dict] = []
    for item in raw or []:
        if isinstance(item, str):
            out.append({"type": "email", "ref": item})
        elif hasattr(item, "dict"):  # Ninja schema instance
            out.append(item.dict())
        elif isinstance(item, dict):
            out.append({"type": item.get("type", "email"), "ref": str(item.get("ref", ""))})
        else:
            raise AlertValidationError(f"Unsupported recipient payload: {item!r}")
    return out


def resolve_recipient_emails(alert: Alert) -> list[str]:
    """Convert Alert.recipients into a flat email list for delivery.

    user-type recipients resolve to their current OrgUser email, so address
    changes propagate without touching the alert config.
    """
    emails: list[str] = []
    user_ids: list[int] = []
    for r in alert.get_recipients():
        if r.type == "email":
            emails.append(r.ref)
        elif r.type == "user":
            try:
                user_ids.append(int(r.ref))
            except (TypeError, ValueError):
                logger.warning(f"Alert {alert.id}: skipping invalid user ref {r.ref!r}")

    if user_ids:
        users = OrgUser.objects.filter(id__in=user_ids, org=alert.org).select_related("user")
        for ou in users:
            if ou.user and ou.user.email:
                emails.append(ou.user.email)

    # De-dupe, preserve order
    seen: set = set()
    deduped: list[str] = []
    for e in emails:
        if e and e not in seen:
            seen.add(e)
            deduped.append(e)
    return deduped


# ═══════════════════════════════════════════════════════════════════════════
# Service
# ═══════════════════════════════════════════════════════════════════════════


class AlertService:
    """Service class for alert operations"""

    # ── Listing / single-read ────────────────────────────────────────────────

    @staticmethod
    def list_alerts(
        org: Org,
        page: int = 1,
        page_size: int = 10,
        kpi_id: int | None = None,
        metric_id: int | None = None,
    ):
        """List alerts for org, sorted by last fired (most recent first)."""
        queryset = Alert.objects.filter(org=org)
        if kpi_id is not None:
            queryset = queryset.filter(kpi_id=kpi_id)
        if metric_id is not None:
            queryset = queryset.filter(metric_id=metric_id)

        queryset = (
            queryset.select_related("kpi", "kpi__metric", "metric")
            .annotate(
                _last_evaluated_at=Max("evaluations__created_at"),
                _last_fired_at=Max(
                    "evaluations__created_at",
                    filter=Q(evaluations__fired=True),
                ),
            )
            .order_by(
                db_models.F("_last_fired_at").desc(nulls_last=True),
                db_models.F("_last_evaluated_at").desc(nulls_last=True),
            )
        )

        total = queryset.count()
        offset = (page - 1) * page_size
        alerts = list(queryset[offset : offset + page_size])

        results = []
        for alert in alerts:
            streak = AlertService.compute_fire_streak(alert)
            results.append(
                {
                    "alert": alert,
                    "last_evaluated_at": alert._last_evaluated_at,
                    "last_fired_at": alert._last_fired_at,
                    "fire_streak": streak,
                }
            )

        return results, total

    @staticmethod
    def get_alert(alert_id: int, org: Org) -> Alert:
        try:
            return Alert.objects.select_related("kpi", "kpi__metric", "metric").get(
                id=alert_id, org=org
            )
        except Alert.DoesNotExist:
            raise AlertNotFoundError(alert_id)

    # ── Create / update / delete ─────────────────────────────────────────────

    @staticmethod
    def create_alert(data: AlertCreate, orguser: OrgUser) -> Alert:
        org = orguser.org
        alert_type = data.alert_type
        kpi = AlertService._resolve_kpi(org, data.kpi_id) if alert_type == "rag" else None
        metric = (
            AlertService._resolve_metric(org, data.metric_id)
            if alert_type == "threshold"
            else None
        )
        metric_rag_level = data.metric_rag_level if alert_type == "rag" else None

        AlertService._validate_type_configuration(
            alert_type=alert_type, kpi=kpi, metric=metric, metric_rag_level=metric_rag_level
        )

        config = AlertQueryConfig.from_dict(data.query_config.dict())
        if alert_type == "rag" and kpi:
            config = AlertService._build_kpi_backed_query_config(kpi)

        alert = Alert.objects.create(
            name=data.name,
            org=org,
            created_by=orguser,
            alert_type=alert_type,
            kpi=kpi,
            metric=metric,
            metric_rag_level=metric_rag_level,
            query_config=config.to_dict(),
            recipients=_normalize_recipients_for_storage(data.recipients),
            pipeline_triggers=list(data.pipeline_triggers or []),
            notification_cooldown_days=data.notification_cooldown_days,
            message=data.message,
            group_message=data.group_message,
        )
        return alert

    @staticmethod
    def update_alert(alert_id: int, org: Org, orguser: OrgUser, data: AlertUpdate) -> Alert:
        alert = AlertService.get_alert(alert_id, org)
        payload = data.dict(exclude_unset=True)

        if "alert_type" in payload and data.alert_type is not None:
            alert.alert_type = data.alert_type
        if data.name is not None:
            alert.name = data.name
        if data.query_config is not None:
            alert.set_query_config(AlertQueryConfig.from_dict(data.query_config.dict()))

        if "kpi_id" in payload:
            alert.kpi = (
                AlertService._resolve_kpi(org, data.kpi_id) if alert.alert_type == "rag" else None
            )
        if "metric_id" in payload:
            alert.metric = (
                AlertService._resolve_metric(org, data.metric_id)
                if alert.alert_type == "threshold"
                else None
            )
        if "metric_rag_level" in payload:
            alert.metric_rag_level = (
                data.metric_rag_level if alert.alert_type == "rag" else None
            )

        AlertService._validate_type_configuration(
            alert_type=alert.alert_type,
            kpi=alert.kpi,
            metric=alert.metric,
            metric_rag_level=alert.metric_rag_level,
        )
        # Rebuild the stored query_config for RAG alerts so the source snapshot
        # stays consistent with the linked KPI's current Metric definition.
        if alert.alert_type == "rag" and alert.kpi:
            alert.set_query_config(AlertService._build_kpi_backed_query_config(alert.kpi))

        if data.recipients is not None:
            alert.recipients = _normalize_recipients_for_storage(data.recipients)
        if data.pipeline_triggers is not None:
            alert.pipeline_triggers = list(data.pipeline_triggers)
        if "notification_cooldown_days" in payload:
            alert.notification_cooldown_days = data.notification_cooldown_days
        if data.message is not None:
            alert.message = data.message
        if data.group_message is not None:
            alert.group_message = data.group_message
        if data.is_active is not None:
            alert.is_active = data.is_active

        alert.save()
        return alert

    @staticmethod
    def delete_alert(alert_id: int, org: Org):
        alert = AlertService.get_alert(alert_id, org)
        alert.delete()

    # ── Warehouse + test + evaluation ────────────────────────────────────────

    @staticmethod
    def _get_warehouse_client(org: Org):
        org_warehouse = OrgWarehouse.objects.filter(org=org).first()
        if not org_warehouse:
            raise AlertWarehouseError("No warehouse configured for this organization")
        return WarehouseFactory.get_warehouse_client(org_warehouse)

    @staticmethod
    def test_alert(data: AlertTestRequest, org: Org) -> dict:
        # Resolve what type we're previewing. Falls back to explicit fields.
        alert_type = data.alert_type or (
            "rag"
            if data.kpi_id and data.metric_rag_level
            else ("threshold" if data.metric_id else "standalone")
        )

        if alert_type == "rag":
            kpi = AlertService._resolve_kpi(org, data.kpi_id)
            if not kpi or not data.metric_rag_level:
                raise AlertValidationError("RAG preview requires kpi_id + metric_rag_level")
            return AlertService._test_kpi_rag_alert(
                kpi=kpi,
                metric_rag_level=data.metric_rag_level,
                org=org,
                message=data.message,
                group_message=data.group_message,
                page=data.page,
                page_size=data.page_size,
            )

        if alert_type == "threshold":
            metric = AlertService._resolve_metric(org, data.metric_id)
            if not metric:
                raise AlertValidationError("Threshold preview requires metric_id")
            return AlertService._test_metric_threshold_alert(
                metric=metric,
                org=org,
                query_config=AlertQueryConfig.from_dict(data.query_config.dict()),
                message=data.message,
                group_message=data.group_message,
                page=data.page,
                page_size=data.page_size,
            )

        # Standalone — free-form SQL query.
        return AlertService._test_standalone_alert(
            org=org,
            query_config=AlertQueryConfig.from_dict(data.query_config.dict()),
            message=data.message,
            group_message=data.group_message,
            page=data.page,
            page_size=data.page_size,
        )

    @staticmethod
    def evaluate_alert(
        alert: Alert,
        trigger_flow_run_id: str | None = None,
        last_pipeline_update: datetime | None = None,
    ) -> tuple[bool, int, str]:
        """Run the alert once and persist an AlertEvaluation. Does NOT send email.

        Returns (fired, rows_count, rendered_message). Caller is responsible
        for calling `send_notification_if_allowed` separately so the cooldown
        bookkeeping stays in one place.
        """
        # Idempotent per trigger_flow_run_id.
        if (
            trigger_flow_run_id
            and AlertEvaluation.objects.filter(
                alert=alert, trigger_flow_run_id=trigger_flow_run_id
            ).exists()
        ):
            latest = (
                AlertEvaluation.objects.filter(
                    alert=alert, trigger_flow_run_id=trigger_flow_run_id
                )
                .order_by("-created_at")
                .first()
            )
            return (
                bool(latest and latest.fired),
                latest.rows_returned if latest else 0,
                latest.rendered_message if latest else "",
            )

        if alert.alert_type == "rag":
            return AlertService._evaluate_kpi_rag_alert(
                alert,
                trigger_flow_run_id=trigger_flow_run_id,
                last_pipeline_update=last_pipeline_update,
            )
        if alert.alert_type == "threshold":
            return AlertService._evaluate_metric_threshold_alert(
                alert,
                trigger_flow_run_id=trigger_flow_run_id,
                last_pipeline_update=last_pipeline_update,
            )
        return AlertService._evaluate_standalone_alert(
            alert,
            trigger_flow_run_id=trigger_flow_run_id,
            last_pipeline_update=last_pipeline_update,
        )

    @staticmethod
    def get_evaluations(alert_id: int, org: Org, page: int = 1, page_size: int = 20):
        alert = AlertService.get_alert(alert_id, org)
        queryset = AlertEvaluation.objects.filter(alert=alert).order_by("-created_at")
        total = queryset.count()
        offset = (page - 1) * page_size
        evaluations = list(queryset[offset : offset + page_size])
        return evaluations, total

    @staticmethod
    def list_fired_evaluations(
        org: Org,
        page: int = 1,
        page_size: int = 20,
        kpi_id: int | None = None,
        metric_id: int | None = None,
    ):
        queryset = AlertEvaluation.objects.filter(alert__org=org, fired=True).select_related(
            "alert", "alert__kpi", "alert__kpi__metric", "alert__metric"
        )
        if kpi_id is not None:
            queryset = queryset.filter(alert__kpi_id=kpi_id)
        if metric_id is not None:
            queryset = queryset.filter(alert__metric_id=metric_id)

        queryset = queryset.order_by("-created_at")
        total = queryset.count()
        offset = (page - 1) * page_size
        evaluations = list(queryset[offset : offset + page_size])
        return evaluations, total

    @staticmethod
    def evaluate_alerts_for_completed_flow(
        org: Org,
        deployment_id: str | None,
        trigger_flow_run_id: str,
        completed_at: datetime | None = None,
    ) -> dict[str, int]:
        """Pipeline-completion hook. Evaluates every active alert that this
        pipeline triggers, then applies cooldown before sending notifications.
        """
        if not deployment_id:
            return {"evaluated": 0, "fired": 0, "notified": 0}
        if not AlertService._deployment_has_transform_tasks(deployment_id):
            return {"evaluated": 0, "fired": 0, "notified": 0}

        completed_at = completed_at or timezone.now()
        active_alerts = Alert.objects.filter(org=org, is_active=True).select_related(
            "kpi", "kpi__metric", "metric"
        )

        evaluated_count = 0
        fired_count = 0
        notified_count = 0

        for alert in active_alerts:
            if not AlertService._alert_triggered_by(alert, deployment_id):
                continue
            try:
                if AlertEvaluation.objects.filter(
                    alert=alert, trigger_flow_run_id=trigger_flow_run_id
                ).exists():
                    continue

                fired, _, rendered_message = AlertService.evaluate_alert(
                    alert,
                    trigger_flow_run_id=trigger_flow_run_id,
                    last_pipeline_update=completed_at,
                )
                evaluated_count += 1
                if fired:
                    fired_count += 1
                    if AlertService._should_send_notification(alert, fired=True):
                        recipients = resolve_recipient_emails(alert)
                        if recipients:
                            send_alert_emails(alert, rendered_message, recipients=recipients)
                            AlertService._mark_latest_notification_sent(
                                alert, trigger_flow_run_id
                            )
                            notified_count += 1
            except Exception as err:
                logger.error(
                    "Failed to evaluate alert %s for flow run %s: %s",
                    alert.id,
                    trigger_flow_run_id,
                    err,
                )

        return {
            "evaluated": evaluated_count,
            "fired": fired_count,
            "notified": notified_count,
        }

    # ── KPI / Metric reference helpers ───────────────────────────────────────

    @staticmethod
    def sync_alerts_for_kpi(kpi: KPI):
        """Rewrite KPI-owned query fields for all alerts linked to this KPI."""
        for alert in Alert.objects.filter(kpi=kpi):
            config = AlertService.get_effective_query_config(alert)
            alert.set_query_config(config)
            alert.save(update_fields=["query_config", "updated_at"])

    @staticmethod
    def kpi_has_linked_alerts(kpi: KPI) -> bool:
        return Alert.objects.filter(kpi=kpi).exists()

    @staticmethod
    def metric_has_linked_alerts(metric: Metric) -> bool:
        return Alert.objects.filter(metric=metric).exists()

    @staticmethod
    def get_effective_query_config(alert: Alert) -> AlertQueryConfig:
        if alert.alert_type == "rag" and alert.kpi:
            return AlertService._build_kpi_backed_query_config(alert.kpi)
        return alert.get_query_config()

    @staticmethod
    def compute_fire_streak(alert: Alert) -> int:
        evaluations = (
            AlertEvaluation.objects.filter(alert=alert)
            .order_by("-created_at")
            .values_list("fired", flat=True)
        )
        streak = 0
        for fired in evaluations:
            if fired:
                streak += 1
            else:
                break
        return streak

    # ── Internal helpers ────────────────────────────────────────────────────

    @staticmethod
    def _resolve_kpi(org: Org, kpi_id: int | None) -> KPI | None:
        """Resolve the KPI for a RAG alert, if one was selected.

        count_distinct constraint is LIFTED — any Metric aggregation is allowed.
        """
        if kpi_id is None:
            return None
        try:
            return KPI.objects.select_related("metric").get(id=kpi_id, org=org)
        except KPI.DoesNotExist as err:
            raise AlertValidationError("Selected KPI does not exist") from err

    @staticmethod
    def _resolve_metric(org: Org, metric_id: int | None) -> Metric | None:
        """Resolve the Metric primitive for a threshold alert."""
        if metric_id is None:
            return None
        try:
            return Metric.objects.get(id=metric_id, org=org)
        except Metric.DoesNotExist as err:
            raise AlertValidationError("Selected Metric does not exist") from err

    @staticmethod
    def _validate_type_configuration(
        *,
        alert_type: str,
        kpi: KPI | None,
        metric: Metric | None,
        metric_rag_level: MetricRagLevel | None,
    ):
        if alert_type == "rag":
            if not kpi:
                raise AlertValidationError("RAG alerts require a KPI")
            if not metric_rag_level:
                raise AlertValidationError(
                    "RAG alerts require a metric_rag_level (red/amber/green)"
                )
            if kpi.target_value in (None, 0):
                raise AlertValidationError(
                    "KPI-backed RAG alerts require the KPI to have a non-zero target"
                )
        elif alert_type == "threshold":
            if not metric:
                raise AlertValidationError("Threshold alerts require a Metric")
        elif alert_type == "standalone":
            if kpi or metric or metric_rag_level:
                raise AlertValidationError(
                    "Standalone alerts must not reference a KPI, Metric, or RAG level"
                )
        else:
            raise AlertValidationError(f"Unknown alert_type: {alert_type!r}")

    @staticmethod
    def _build_kpi_backed_query_config(kpi: KPI) -> AlertQueryConfig:
        """Display-only snapshot for RAG alerts — actual eval uses fetch_current_value."""
        m = kpi.metric
        return AlertQueryConfig(
            schema_name=m.schema_name,
            table_name=m.table_name,
            aggregation="FORMULA",
            measure_column=m.name,
            group_by_column=None,
            filters=[],
            filter_connector="AND",
            condition_operator="=",
            condition_value=0,
        )

    @staticmethod
    def _deployment_has_transform_tasks(deployment_id: str) -> bool:
        return DataflowOrgTask.objects.filter(
            dataflow__deployment_id=deployment_id,
            orgtask__task__type__in=[TaskType.DBT, TaskType.DBTCLOUD],
        ).exists()

    @staticmethod
    def _alert_triggered_by(alert: Alert, deployment_id: str) -> bool:
        """Does this alert fire on completion of the given pipeline?

        Empty pipeline_triggers = "fire on every transform completion for the
        org" (current behavior). Non-empty = only when deployment_id is listed.
        """
        triggers = alert.pipeline_triggers or []
        if not triggers:
            return True
        return deployment_id in triggers

    # ── Cooldown / delivery bookkeeping ──────────────────────────────────────

    @staticmethod
    def _should_send_notification(alert: Alert, *, fired: bool) -> bool:
        """Apply the per-alert cooldown policy.

        Default (cooldown is None): notify only on state change — i.e. send
        iff the most-recent prior evaluation was NOT firing.

        Cooldown = N days: notify iff (a) state just flipped, OR (b) the last
        sent notification is at least N days old.
        """
        if not fired:
            return False
        cooldown = alert.notification_cooldown_days

        last_eval = (
            AlertEvaluation.objects.filter(alert=alert)
            .exclude(id=None)
            .order_by("-created_at")
            # Skip the evaluation we just created so we see the *previous* one.
            # Each evaluation is at most ~1s apart; we compare strictly.
            [1:2]
            .first()
        )

        if cooldown is None:
            # State-change: send only if the previous evaluation didn't fire
            # (or no prior evaluation exists).
            return last_eval is None or not last_eval.fired

        # Re-notify every N days. Find the last evaluation that actually sent.
        last_sent = (
            AlertEvaluation.objects.filter(alert=alert, notification_sent=True)
            .order_by("-created_at")
            .first()
        )
        if last_sent is None:
            return True
        return (timezone.now() - last_sent.created_at) >= timedelta(days=cooldown)

    @staticmethod
    def _mark_latest_notification_sent(alert: Alert, trigger_flow_run_id: str | None):
        """Flip notification_sent on the evaluation we just created for this run."""
        qs = AlertEvaluation.objects.filter(alert=alert)
        if trigger_flow_run_id:
            qs = qs.filter(trigger_flow_run_id=trigger_flow_run_id)
        evaluation = qs.order_by("-created_at").first()
        if evaluation:
            evaluation.notification_sent = True
            evaluation.save(update_fields=["notification_sent"])

    # ── Evaluation paths (one per type) ──────────────────────────────────────

    @staticmethod
    def _get_kpi_rag_state(kpi: KPI, org: Org) -> dict:
        wclient = AlertService._get_warehouse_client(org)
        current_value, error = fetch_current_value(wclient, kpi.metric)
        if error:
            raise AlertWarehouseError(f"KPI evaluation failed: {error}")

        rag_status, achievement_pct = compute_rag_status(
            current_value,
            kpi.target_value,
            kpi.amber_threshold_pct,
            kpi.green_threshold_pct,
            kpi.direction,
        )

        return {
            "alert_value": current_value,
            "rag_status": rag_status,
            "achievement_pct": achievement_pct,
            "target_value": kpi.target_value,
            "selected_rag_level": None,
        }

    @staticmethod
    def _build_kpi_rag_query_summary(kpi: KPI, metric_rag_level: MetricRagLevel) -> str:
        m = kpi.metric
        return (
            f"KPI RAG evaluation for {m.name} "
            f"({m.schema_name}.{m.table_name}) "
            f"targeting {metric_rag_level}"
        )

    @staticmethod
    def _test_kpi_rag_alert(
        *,
        kpi: KPI,
        metric_rag_level: MetricRagLevel,
        org: Org,
        message: str,
        group_message: str,
        page: int,
        page_size: int,
    ) -> dict:
        state = AlertService._get_kpi_rag_state(kpi, org)
        state["selected_rag_level"] = metric_rag_level
        would_fire = state["rag_status"] == metric_rag_level
        rows = [state]
        rendered_message = render_alert_message(
            alert_name="Alert preview",
            message=message,
            group_message=group_message,
            rows=rows,
            table_name=kpi.metric.table_name,
            metric_name=kpi.metric.name,
            group_by_column=None,
        )

        return {
            "would_fire": would_fire,
            "total_rows": len(rows),
            "results": rows,
            "page": page,
            "page_size": page_size,
            "query_executed": AlertService._build_kpi_rag_query_summary(kpi, metric_rag_level),
            "rendered_message": rendered_message,
        }

    @staticmethod
    def _test_metric_threshold_alert(
        *,
        metric: Metric,
        org: Org,
        query_config: AlertQueryConfig,
        message: str,
        group_message: str,
        page: int,
        page_size: int,
    ) -> dict:
        wclient = AlertService._get_warehouse_client(org)
        current_value, error = fetch_current_value(wclient, metric)
        if error:
            raise AlertWarehouseError(f"Metric evaluation failed: {error}")

        would_fire = AlertService._threshold_fires(
            current_value, query_config.condition_operator, query_config.condition_value
        )
        rows = [
            {
                "metric_name": metric.name,
                "alert_value": current_value,
                "condition": f"{query_config.condition_operator} {query_config.condition_value}",
            }
        ]
        rendered_message = render_alert_message(
            alert_name="Alert preview",
            message=message,
            group_message=group_message,
            rows=rows,
            table_name=metric.table_name,
            metric_name=metric.name,
            group_by_column=None,
        )
        return {
            "would_fire": would_fire,
            "total_rows": 1 if would_fire else 0,
            "results": rows,
            "page": page,
            "page_size": page_size,
            "query_executed": (
                f"Metric threshold evaluation for {metric.name}: "
                f"value {query_config.condition_operator} {query_config.condition_value}"
            ),
            "rendered_message": rendered_message,
        }

    @staticmethod
    def _test_standalone_alert(
        *,
        org: Org,
        query_config: AlertQueryConfig,
        message: str,
        group_message: str,
        page: int,
        page_size: int,
    ) -> dict:
        wclient = AlertService._get_warehouse_client(org)
        base_qb = build_alert_query_builder(query_config)
        base_sql = compile_query(base_qb, wclient)

        count_qb = build_count_query_builder(base_qb)
        count_sql = compile_query(count_qb, wclient)

        paginated_qb = build_paginated_query_builder(base_qb, page, page_size)
        paginated_sql = compile_query(paginated_qb, wclient)

        try:
            count_result = wclient.execute(count_sql)
            total_rows = count_result[0]["cnt"] if count_result else 0
            results = wclient.execute(paginated_sql) if total_rows > 0 else []
        except Exception as e:
            raise AlertWarehouseError(f"Query execution failed: {str(e)}")

        normalized_results = normalize_result_rows(results)
        rendered_message = render_alert_message(
            alert_name="Alert preview",
            message=message,
            group_message=group_message,
            rows=normalized_results,
            table_name=query_config.table_name,
            metric_name="",
            group_by_column=query_config.group_by_column,
        )

        return {
            "would_fire": total_rows > 0,
            "total_rows": total_rows,
            "results": normalized_results,
            "page": page,
            "page_size": page_size,
            "query_executed": base_sql,
            "rendered_message": rendered_message,
        }

    @staticmethod
    def _threshold_fires(current_value, operator: str, threshold: float) -> bool:
        if current_value is None:
            return False
        try:
            v = float(current_value)
        except (TypeError, ValueError):
            return False
        if operator == ">":
            return v > threshold
        if operator == "<":
            return v < threshold
        if operator == ">=":
            return v >= threshold
        if operator == "<=":
            return v <= threshold
        if operator == "=":
            return v == threshold
        if operator == "!=":
            return v != threshold
        return False

    @staticmethod
    def _evaluate_kpi_rag_alert(
        alert: Alert,
        *,
        trigger_flow_run_id: str | None = None,
        last_pipeline_update: datetime | None = None,
    ) -> tuple[bool, int, str]:
        kpi = alert.kpi
        if not kpi or not alert.metric_rag_level:
            raise AlertValidationError("KPI-backed alert is missing its KPI or RAG level")

        state = AlertService._get_kpi_rag_state(kpi, alert.org)
        state["selected_rag_level"] = alert.metric_rag_level
        fired = state["rag_status"] == alert.metric_rag_level
        rows = [state] if fired else []
        sql = AlertService._build_kpi_rag_query_summary(kpi, alert.metric_rag_level)
        rendered_message = render_alert_message(
            alert_name=alert.name,
            message=alert.message,
            group_message=alert.group_message,
            rows=[state],
            table_name=kpi.metric.table_name,
            metric_name=kpi.metric.name,
            group_by_column=None,
        )

        AlertEvaluation.objects.create(
            alert=alert,
            query_config=AlertService.get_effective_query_config(alert).to_dict(),
            query_executed=sql,
            recipients=alert.recipients,
            message=alert.message,
            fired=fired,
            rows_returned=len(rows),
            result_preview=rows[:25],
            rendered_message=rendered_message,
            trigger_flow_run_id=trigger_flow_run_id,
            last_pipeline_update=last_pipeline_update,
        )
        return fired, len(rows), rendered_message

    @staticmethod
    def _evaluate_metric_threshold_alert(
        alert: Alert,
        *,
        trigger_flow_run_id: str | None = None,
        last_pipeline_update: datetime | None = None,
    ) -> tuple[bool, int, str]:
        metric = alert.metric
        if not metric:
            raise AlertValidationError("Threshold alert is missing its Metric")

        wclient = AlertService._get_warehouse_client(alert.org)
        current_value, error = fetch_current_value(wclient, metric)
        config = alert.get_query_config()
        if error:
            AlertEvaluation.objects.create(
                alert=alert,
                query_config=config.to_dict(),
                query_executed=f"Metric threshold evaluation for {metric.name}",
                recipients=alert.recipients,
                message=alert.message,
                fired=False,
                rows_returned=0,
                result_preview=[],
                rendered_message="",
                trigger_flow_run_id=trigger_flow_run_id,
                last_pipeline_update=last_pipeline_update,
                error_message=error,
            )
            raise AlertWarehouseError(f"Metric evaluation failed: {error}")

        fired = AlertService._threshold_fires(
            current_value, config.condition_operator, config.condition_value
        )
        rows = (
            [
                {
                    "metric_name": metric.name,
                    "alert_value": current_value,
                    "condition": f"{config.condition_operator} {config.condition_value}",
                }
            ]
            if fired
            else []
        )
        rendered_message = render_alert_message(
            alert_name=alert.name,
            message=alert.message,
            group_message=alert.group_message,
            rows=[
                {
                    "metric_name": metric.name,
                    "alert_value": current_value,
                    "condition": f"{config.condition_operator} {config.condition_value}",
                }
            ],
            table_name=metric.table_name,
            metric_name=metric.name,
            group_by_column=None,
        )

        AlertEvaluation.objects.create(
            alert=alert,
            query_config=config.to_dict(),
            query_executed=(
                f"Metric threshold: {metric.name} value "
                f"{config.condition_operator} {config.condition_value}"
            ),
            recipients=alert.recipients,
            message=alert.message,
            fired=fired,
            rows_returned=len(rows),
            result_preview=rows[:25],
            rendered_message=rendered_message,
            trigger_flow_run_id=trigger_flow_run_id,
            last_pipeline_update=last_pipeline_update,
        )
        return fired, len(rows), rendered_message

    @staticmethod
    def _evaluate_standalone_alert(
        alert: Alert,
        *,
        trigger_flow_run_id: str | None = None,
        last_pipeline_update: datetime | None = None,
    ) -> tuple[bool, int, str]:
        wclient = AlertService._get_warehouse_client(alert.org)
        config: AlertQueryConfig = alert.get_query_config()

        base_qb = build_alert_query_builder(config)
        sql = compile_query(base_qb, wclient)

        try:
            results = wclient.execute(sql)
        except Exception as e:
            AlertEvaluation.objects.create(
                alert=alert,
                query_config=config.to_dict(),
                query_executed=sql,
                recipients=alert.recipients,
                message=alert.message,
                fired=False,
                rows_returned=0,
                result_preview=[],
                rendered_message="",
                trigger_flow_run_id=trigger_flow_run_id,
                last_pipeline_update=last_pipeline_update,
                error_message=str(e),
            )
            raise AlertWarehouseError(f"Query execution failed: {str(e)}")

        normalized_results = normalize_result_rows(results)
        fired = len(normalized_results) > 0
        rows_count = len(normalized_results)
        rendered_message = render_alert_message(
            alert_name=alert.name,
            message=alert.message,
            group_message=alert.group_message,
            rows=normalized_results,
            table_name=config.table_name,
            metric_name="",
            group_by_column=config.group_by_column,
        )

        AlertEvaluation.objects.create(
            alert=alert,
            query_config=config.to_dict(),
            query_executed=sql,
            recipients=alert.recipients,
            message=alert.message,
            fired=fired,
            rows_returned=rows_count,
            result_preview=normalized_results[:25],
            rendered_message=rendered_message,
            trigger_flow_run_id=trigger_flow_run_id,
            last_pipeline_update=last_pipeline_update,
        )
        return fired, rows_count, rendered_message

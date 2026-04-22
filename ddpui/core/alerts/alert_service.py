"""Alert business logic — Batch 1 patch.

RAG-backed alerts now reference a **KPI** (the tracked layer), not a raw
MetricDefinition. Source fields (schema / table / aggregation) come from the
KPI's linked Metric; target / direction / thresholds come from the KPI.

The `count_distinct` constraint is lifted per product call — alerts can now
be built on any Metric regardless of aggregation.

Batch 3 will split this into three explicit alert types (threshold / rag /
standalone) with per-alert cooldown and structured recipients.
"""

from typing import Literal

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
from django.db import models as db_models
from django.db.models import Max, Q

from ddpui.models.alert import (
    Alert,
    AlertEvaluation,
    AlertQueryConfig,
)
from ddpui.models.org import Org, OrgWarehouse
from ddpui.models.org_user import OrgUser
from ddpui.models.metrics import KPI, Metric
from ddpui.models.tasks import DataflowOrgTask, TaskType
from ddpui.core.metrics_service import compute_rag_status, fetch_current_value
from ddpui.schemas.alert_schema import AlertCreate, AlertUpdate, AlertTestRequest
from ddpui.utils.custom_logger import CustomLogger
from ddpui.utils.warehouse.client.warehouse_factory import WarehouseFactory

logger = CustomLogger("ddpui.core.alerts")

MetricRagLevel = Literal["red", "amber", "green"]


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
        config = AlertQueryConfig.from_dict(data.query_config.dict())
        kpi = AlertService._resolve_kpi(orguser.org, data.kpi_id)
        metric_rag_level = data.metric_rag_level
        AlertService._validate_rag_configuration(kpi, metric_rag_level, creating=True)
        config = (
            AlertService._build_kpi_backed_query_config(kpi)
            if kpi and metric_rag_level
            else config
        )

        alert = Alert.objects.create(
            name=data.name,
            org=orguser.org,
            created_by=orguser,
            kpi=kpi,
            metric_rag_level=metric_rag_level,
            query_config=config.to_dict(),
            recipients=data.recipients,
            message=data.message,
            group_message=data.group_message,
        )
        return alert

    @staticmethod
    def update_alert(alert_id: int, org: Org, orguser: OrgUser, data: AlertUpdate) -> Alert:
        alert = AlertService.get_alert(alert_id, org)
        config = alert.get_query_config()
        payload = data.dict(exclude_unset=True)

        if data.name is not None:
            alert.name = data.name
        if data.query_config is not None:
            config = AlertQueryConfig.from_dict(data.query_config.dict())

        kpi = alert.kpi
        metric_rag_level = alert.metric_rag_level
        if "kpi_id" in payload:
            kpi = AlertService._resolve_kpi(org, data.kpi_id)
            alert.kpi = kpi
            if kpi is None:
                metric_rag_level = None
        if "metric_rag_level" in payload:
            metric_rag_level = data.metric_rag_level

        AlertService._validate_rag_configuration(kpi, metric_rag_level, creating=False)
        config = (
            AlertService._build_kpi_backed_query_config(kpi)
            if kpi and metric_rag_level
            else config
        )
        alert.metric_rag_level = metric_rag_level
        alert.set_query_config(config)

        if data.recipients is not None:
            alert.recipients = data.recipients
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
        wclient = AlertService._get_warehouse_client(org)
        config = AlertQueryConfig.from_dict(data.query_config.dict())
        kpi = AlertService._resolve_kpi(org, data.kpi_id)
        metric_rag_level = data.metric_rag_level
        AlertService._validate_rag_configuration(kpi, metric_rag_level, creating=False)
        if kpi and metric_rag_level:
            return AlertService._test_kpi_rag_alert(
                kpi=kpi,
                metric_rag_level=metric_rag_level,
                org=org,
                message=data.message,
                group_message=data.group_message,
                page=data.page,
                page_size=data.page_size,
            )

        # Standalone path — query_config drives the SQL directly.
        base_qb = build_alert_query_builder(config)
        base_sql = compile_query(base_qb, wclient)

        count_qb = build_count_query_builder(base_qb)
        count_sql = compile_query(count_qb, wclient)

        paginated_qb = build_paginated_query_builder(base_qb, data.page, data.page_size)
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
            message=data.message,
            group_message=data.group_message,
            rows=normalized_results,
            table_name=config.table_name,
            metric_name="",
            group_by_column=config.group_by_column,
        )

        return {
            "would_fire": total_rows > 0,
            "total_rows": total_rows,
            "results": normalized_results,
            "page": data.page,
            "page_size": data.page_size,
            "query_executed": base_sql,
            "rendered_message": rendered_message,
        }

    @staticmethod
    def evaluate_alert(
        alert: Alert, trigger_flow_run_id: str | None = None
    ) -> tuple[bool, int, str]:
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

        if alert.kpi_id and alert.metric_rag_level:
            return AlertService._evaluate_kpi_rag_alert(
                alert, trigger_flow_run_id=trigger_flow_run_id
            )

        wclient = AlertService._get_warehouse_client(alert.org)
        config: AlertQueryConfig = AlertService.get_effective_query_config(alert)

        base_qb = build_alert_query_builder(config)
        sql = compile_query(base_qb, wclient)

        try:
            results = wclient.execute(sql)
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
            )
            return fired, rows_count, rendered_message

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
                error_message=str(e),
            )
            raise AlertWarehouseError(f"Query execution failed: {str(e)}")

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
    ) -> dict[str, int]:
        if not deployment_id:
            return {"evaluated": 0, "fired": 0}
        if not AlertService._deployment_has_transform_tasks(deployment_id):
            return {"evaluated": 0, "fired": 0}

        active_alerts = Alert.objects.filter(org=org, is_active=True).select_related(
            "kpi", "kpi__metric", "metric"
        )
        evaluated_count = 0
        fired_count = 0

        for alert in active_alerts:
            try:
                if AlertEvaluation.objects.filter(
                    alert=alert, trigger_flow_run_id=trigger_flow_run_id
                ).exists():
                    continue

                fired, _, rendered_message = AlertService.evaluate_alert(
                    alert, trigger_flow_run_id=trigger_flow_run_id
                )
                evaluated_count += 1
                if fired:
                    fired_count += 1
                    send_alert_emails(alert, rendered_message)
            except Exception as err:
                logger.error(
                    "Failed to evaluate alert %s for flow run %s: %s",
                    alert.id,
                    trigger_flow_run_id,
                    err,
                )

        return {"evaluated": evaluated_count, "fired": fired_count}

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
        if alert.kpi and alert.metric_rag_level:
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

        Note: count_distinct constraint has been LIFTED — any Metric aggregation
        (including count_distinct) is allowed to back an alert.
        """
        if kpi_id is None:
            return None
        try:
            return KPI.objects.select_related("metric").get(id=kpi_id, org=org)
        except KPI.DoesNotExist as err:
            raise AlertValidationError("Selected KPI does not exist") from err

    @staticmethod
    def _validate_rag_configuration(
        kpi: KPI | None,
        metric_rag_level: MetricRagLevel | None,
        *,
        creating: bool,
    ):
        if metric_rag_level and not kpi:
            raise AlertValidationError("RAG alerts require a selected KPI")
        if kpi and creating and not metric_rag_level:
            raise AlertValidationError("Choose Red, Amber, or Green for KPI-backed alerts")
        if kpi and metric_rag_level and kpi.target_value in (None, 0):
            raise AlertValidationError("KPI-backed alerts require the KPI to have a target")

    @staticmethod
    def _build_kpi_backed_query_config(kpi: KPI) -> AlertQueryConfig:
        """Store a readable source snapshot for KPI-backed alerts.

        The actual RAG evaluation path does not go through this SQL — it uses
        `fetch_current_value` against the KPI's underlying Metric. This config
        is kept for the UI's "here's what data this alert reads" affordance.
        """
        m = kpi.metric
        return AlertQueryConfig(
            schema_name=m.schema_name,
            table_name=m.table_name,
            aggregation="FORMULA",  # compound Metric expressions can't reduce to one
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
    def _evaluate_kpi_rag_alert(
        alert: Alert,
        *,
        trigger_flow_run_id: str | None = None,
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
        )

        return fired, len(rows), rendered_message

"""Alert business logic and CRUD operations"""

from dataclasses import replace

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
    AlertMessagePlaceholderConfig,
)
from ddpui.models.org import Org, OrgWarehouse
from ddpui.models.org_user import OrgUser
from ddpui.models.metrics import MetricDefinition
from ddpui.models.tasks import DataflowOrgTask, TaskType
from ddpui.schemas.alert_schema import AlertCreate, AlertUpdate, AlertTestRequest
from ddpui.utils.custom_logger import CustomLogger
from ddpui.utils.warehouse.client.warehouse_factory import WarehouseFactory

logger = CustomLogger("ddpui.core.alerts")

RESERVED_PLACEHOLDER_KEYS = {
    "alert_name",
    "alert_value",
    "metric_name",
    "table_name",
    "group_by_column",
    "group_by_value",
    "failing_group_count",
}


class AlertService:
    """Service class for alert operations"""

    @staticmethod
    def list_alerts(org: Org, page: int = 1, page_size: int = 10, metric_id: int | None = None):
        """List alerts for org, sorted by last fired (most recent first)"""
        queryset = Alert.objects.filter(org=org)
        if metric_id is not None:
            queryset = queryset.filter(metric_id=metric_id)

        queryset = (
            queryset.select_related("metric")
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
        """Get single alert, ensure org match"""
        try:
            return Alert.objects.select_related("metric").get(id=alert_id, org=org)
        except Alert.DoesNotExist:
            raise AlertNotFoundError(alert_id)

    @staticmethod
    def create_alert(data: AlertCreate, orguser: OrgUser) -> Alert:
        """Create alert from validated schema"""
        config = AlertQueryConfig.from_dict(data.query_config.dict())
        metric = AlertService._resolve_metric(orguser.org, data.metric_id)
        config = AlertService._apply_metric_to_query_config(config, metric)
        placeholders = AlertService._build_message_placeholders(data.message_placeholders)

        alert = Alert.objects.create(
            name=data.name,
            org=orguser.org,
            created_by=orguser,
            metric=metric,
            query_config=config.to_dict(),
            recipients=data.recipients,
            message=data.message,
            group_message=data.group_message,
            message_placeholders=[placeholder.to_dict() for placeholder in placeholders],
        )
        return alert

    @staticmethod
    def update_alert(alert_id: int, org: Org, orguser: OrgUser, data: AlertUpdate) -> Alert:
        """Update alert — partial update of provided fields"""
        alert = AlertService.get_alert(alert_id, org)
        config = alert.get_query_config()
        payload = data.dict(exclude_unset=True)

        if data.name is not None:
            alert.name = data.name
        if data.query_config is not None:
            config = AlertQueryConfig.from_dict(data.query_config.dict())
        metric = alert.metric
        if "metric_id" in payload:
            metric = AlertService._resolve_metric(org, data.metric_id)
            alert.metric = metric
        config = AlertService._apply_metric_to_query_config(config, metric)
        alert.set_query_config(config)
        if data.recipients is not None:
            alert.recipients = data.recipients
        if data.message is not None:
            alert.message = data.message
        if data.group_message is not None:
            alert.group_message = data.group_message
        if data.message_placeholders is not None:
            placeholders = AlertService._build_message_placeholders(data.message_placeholders)
            alert.message_placeholders = [placeholder.to_dict() for placeholder in placeholders]
        if data.is_active is not None:
            alert.is_active = data.is_active

        alert.save()
        return alert

    @staticmethod
    def delete_alert(alert_id: int, org: Org):
        """Delete alert and all its evaluations (CASCADE)"""
        alert = AlertService.get_alert(alert_id, org)
        alert.delete()

    @staticmethod
    def _get_warehouse_client(org: Org):
        """Get warehouse client for an org"""
        org_warehouse = OrgWarehouse.objects.filter(org=org).first()
        if not org_warehouse:
            raise AlertWarehouseError("No warehouse configured for this organization")
        return WarehouseFactory.get_warehouse_client(org_warehouse)

    @staticmethod
    def test_alert(data: AlertTestRequest, org: Org) -> dict:
        """
        Build SQL from config, execute against warehouse, return paginated results.
        Used by the "Test Alert" button in the UI.
        """
        wclient = AlertService._get_warehouse_client(org)
        config = AlertQueryConfig.from_dict(data.query_config.dict())
        metric = AlertService._resolve_metric(org, data.metric_id)
        config = AlertService._apply_metric_to_query_config(config, metric)
        placeholders = AlertService._build_message_placeholders(data.message_placeholders)

        # Build base query builder
        base_qb = build_alert_query_builder(config, placeholders)
        base_sql = compile_query(base_qb, wclient)

        # Count total matching rows
        count_qb = build_count_query_builder(base_qb)
        count_sql = compile_query(count_qb, wclient)

        # Paginated results
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
            placeholders=placeholders,
            table_name=config.table_name,
            metric_name=metric.name if metric else "",
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
        """
        Evaluate a single alert: build SQL, execute, log result.
        Returns (fired: bool, rows_count: int, rendered_message: str)
        """
        wclient = AlertService._get_warehouse_client(alert.org)
        if (
            trigger_flow_run_id
            and AlertEvaluation.objects.filter(
                alert=alert, trigger_flow_run_id=trigger_flow_run_id
            ).exists()
        ):
            latest = (
                AlertEvaluation.objects.filter(alert=alert, trigger_flow_run_id=trigger_flow_run_id)
                .order_by("-created_at")
                .first()
            )
            return (
                bool(latest and latest.fired),
                latest.rows_returned if latest else 0,
                latest.rendered_message if latest else "",
            )

        config: AlertQueryConfig = AlertService.get_effective_query_config(alert)
        placeholders = alert.get_message_placeholders()

        base_qb = build_alert_query_builder(config, placeholders)
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
                placeholders=placeholders,
                table_name=config.table_name,
                metric_name=alert.metric.name if alert.metric_id else "",
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
        """Get paginated evaluation history for an alert"""
        alert = AlertService.get_alert(alert_id, org)
        queryset = AlertEvaluation.objects.filter(alert=alert).order_by("-created_at")
        total = queryset.count()
        offset = (page - 1) * page_size
        evaluations = list(queryset[offset : offset + page_size])
        return evaluations, total

    @staticmethod
    def list_fired_evaluations(
        org: Org, page: int = 1, page_size: int = 20, metric_id: int | None = None
    ):
        """List recent fired alert evaluations across the org."""
        queryset = AlertEvaluation.objects.filter(alert__org=org, fired=True).select_related(
            "alert", "alert__metric"
        )
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
        """Evaluate active alerts after a successful DBT/DBT Cloud flow run."""
        if not deployment_id:
            return {"evaluated": 0, "fired": 0}
        if not AlertService._deployment_has_transform_tasks(deployment_id):
            return {"evaluated": 0, "fired": 0}

        active_alerts = Alert.objects.filter(org=org, is_active=True).select_related("metric")
        evaluated_count = 0
        fired_count = 0

        for alert in active_alerts:
            try:
                if AlertEvaluation.objects.filter(
                    alert=alert,
                    trigger_flow_run_id=trigger_flow_run_id,
                ).exists():
                    continue

                fired, _, rendered_message = AlertService.evaluate_alert(
                    alert,
                    trigger_flow_run_id=trigger_flow_run_id,
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

    @staticmethod
    def sync_alerts_for_metric(metric: MetricDefinition):
        """Rewrite metric-owned query fields for all alerts linked to a metric."""
        alerts = Alert.objects.filter(metric=metric)
        for alert in alerts:
            config = AlertService.get_effective_query_config(alert)
            alert.set_query_config(config)
            alert.save(update_fields=["query_config", "updated_at"])

    @staticmethod
    def metric_has_linked_alerts(metric: MetricDefinition) -> bool:
        """Check whether any alerts are linked to this metric."""
        return Alert.objects.filter(metric=metric).exists()

    @staticmethod
    def get_effective_query_config(alert: Alert) -> AlertQueryConfig:
        """Read an alert query config with current metric-owned fields applied."""
        return AlertService._apply_metric_to_query_config(alert.get_query_config(), alert.metric)

    @staticmethod
    def compute_fire_streak(alert: Alert) -> int:
        """Count consecutive fired=True evaluations from most recent"""
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

    @staticmethod
    def _resolve_metric(org: Org, metric_id: int | None) -> MetricDefinition | None:
        """Resolve a metric for the alert if one was selected."""
        if metric_id is None:
            return None
        try:
            metric = MetricDefinition.objects.get(id=metric_id, org=org)
        except MetricDefinition.DoesNotExist as err:
            raise AlertValidationError("Selected metric does not exist") from err

        if metric.aggregation == "count_distinct":
            raise AlertValidationError("COUNT DISTINCT metrics are not supported for alerts yet")

        return metric

    @staticmethod
    def _apply_metric_to_query_config(
        config: AlertQueryConfig, metric: MetricDefinition | None
    ) -> AlertQueryConfig:
        """Metric-backed alerts inherit the metric-owned query fields."""
        if not metric:
            return config

        return replace(
            config,
            schema_name=metric.schema_name,
            table_name=metric.table_name,
            aggregation=metric.aggregation.upper(),
            measure_column=metric.column,
        )

    @staticmethod
    def _build_message_placeholders(
        placeholder_payloads,
    ) -> list[AlertMessagePlaceholderConfig]:
        """Validate and convert placeholder payloads into typed configs."""
        placeholders: list[AlertMessagePlaceholderConfig] = []
        seen_keys: set[str] = set()

        for payload in placeholder_payloads or []:
            placeholder = AlertMessagePlaceholderConfig.from_dict(payload.dict())
            if placeholder.key in RESERVED_PLACEHOLDER_KEYS:
                raise AlertValidationError(f"Placeholder key '{placeholder.key}' is reserved")
            if placeholder.key in seen_keys:
                raise AlertValidationError(f"Duplicate placeholder key '{placeholder.key}'")
            if placeholder.aggregation != "COUNT" and not placeholder.column:
                raise AlertValidationError(f"Placeholder '{placeholder.key}' must select a column")
            seen_keys.add(placeholder.key)
            placeholders.append(placeholder)

        return placeholders

    @staticmethod
    def _deployment_has_transform_tasks(deployment_id: str) -> bool:
        """Alerts only evaluate after successful transform runs."""
        return DataflowOrgTask.objects.filter(
            dataflow__deployment_id=deployment_id,
            orgtask__task__type__in=[TaskType.DBT, TaskType.DBTCLOUD],
        ).exists()

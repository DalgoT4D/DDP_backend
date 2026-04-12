"""Alert business logic and CRUD operations"""

from ddpui.core.alerts.alert_query_builder import (
    build_alert_query_builder,
    build_count_query_builder,
    build_paginated_query_builder,
    compile_query,
)
from ddpui.core.alerts.exceptions import (
    AlertNotFoundError,
    AlertValidationError,
    AlertWarehouseError,
)
from django.db import models as db_models
from django.db.models import Max, Q

from ddpui.models.alert import Alert, AlertEvaluation, AlertQueryConfig
from ddpui.models.org import Org, OrgWarehouse
from ddpui.models.org_user import OrgUser
from ddpui.schemas.alert_schema import AlertCreate, AlertUpdate, AlertTestRequest
from ddpui.utils.custom_logger import CustomLogger
from ddpui.utils.warehouse.client.warehouse_factory import WarehouseFactory

logger = CustomLogger("ddpui.core.alerts")


class AlertService:
    """Service class for alert operations"""

    @staticmethod
    def list_alerts(org: Org, page: int = 1, page_size: int = 10):
        """List alerts for org, sorted by last fired (most recent first)"""
        queryset = (
            Alert.objects.filter(org=org)
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
            return Alert.objects.get(id=alert_id, org=org)
        except Alert.DoesNotExist:
            raise AlertNotFoundError(alert_id)

    @staticmethod
    def create_alert(data: AlertCreate, orguser: OrgUser) -> Alert:
        """Create alert from validated schema"""
        config = AlertQueryConfig.from_dict(data.query_config.dict())

        alert = Alert.objects.create(
            name=data.name,
            org=orguser.org,
            created_by=orguser,
            query_config=config.to_dict(),
            cron=data.cron,
            recipients=data.recipients,
            message=data.message,
        )
        return alert

    @staticmethod
    def update_alert(alert_id: int, org: Org, orguser: OrgUser, data: AlertUpdate) -> Alert:
        """Update alert — partial update of provided fields"""
        alert = AlertService.get_alert(alert_id, org)

        if data.name is not None:
            alert.name = data.name
        if data.query_config is not None:
            config = AlertQueryConfig.from_dict(data.query_config.dict())
            alert.set_query_config(config)
        if data.cron is not None:
            alert.cron = data.cron
        if data.recipients is not None:
            alert.recipients = data.recipients
        if data.message is not None:
            alert.message = data.message
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

        # Build base query builder
        base_qb = build_alert_query_builder(config)
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

        return {
            "would_fire": total_rows > 0,
            "total_rows": total_rows,
            "results": results,
            "page": data.page,
            "page_size": data.page_size,
            "query_executed": base_sql,
        }

    @staticmethod
    def evaluate_alert(alert: Alert) -> tuple[bool, int]:
        """
        Evaluate a single alert: build SQL, execute, log result.
        Returns (fired: bool, rows_count: int)
        """
        wclient = AlertService._get_warehouse_client(alert.org)
        config: AlertQueryConfig = alert.get_query_config()

        base_qb = build_alert_query_builder(config)
        sql = compile_query(base_qb, wclient)

        try:
            results = wclient.execute(sql)
            fired = len(results) > 0
            rows_count = len(results)

            AlertEvaluation.objects.create(
                alert=alert,
                query_config=alert.query_config,
                query_executed=sql,
                cron=alert.cron,
                recipients=alert.recipients,
                message=alert.message,
                fired=fired,
                rows_returned=rows_count,
            )
            return fired, rows_count

        except Exception as e:
            AlertEvaluation.objects.create(
                alert=alert,
                query_config=alert.query_config,
                query_executed=sql,
                cron=alert.cron,
                recipients=alert.recipients,
                message=alert.message,
                fired=False,
                rows_returned=0,
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

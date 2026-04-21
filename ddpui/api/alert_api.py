"""Alert API endpoints"""

from ninja import Router
from ninja.errors import HttpError

from ddpui.auth import has_permission
from ddpui.models.org_user import OrgUser
from ddpui.utils.custom_logger import CustomLogger
from ddpui.utils.response_wrapper import api_response

from ddpui.core.alerts.alert_service import AlertService
from ddpui.core.alerts.exceptions import (
    AlertNotFoundError,
    AlertValidationError,
    AlertWarehouseError,
)
from ddpui.schemas.alert_schema import (
    AlertCreate,
    AlertUpdate,
    AlertTestRequest,
    AlertResponse,
    AlertEvaluationResponse,
    AlertTestResponse,
    TriggeredAlertEventResponse,
)

logger = CustomLogger("ddpui.alert_api")

alert_router = Router()


@alert_router.get("/")
@has_permission(["can_view_alerts"])
def list_alerts(request, page: int = 1, page_size: int = 10, metric_id: int | None = None):
    """List alerts for the org with pagination"""
    orguser: OrgUser = request.orguser

    results, total = AlertService.list_alerts(
        org=orguser.org,
        page=page,
        page_size=page_size,
        metric_id=metric_id,
    )

    return api_response(
        success=True,
        data={
            "data": [
                AlertResponse.from_model(
                    r["alert"],
                    last_evaluated_at=r["last_evaluated_at"],
                    last_fired_at=r["last_fired_at"],
                    fire_streak=r["fire_streak"],
                ).dict()
                for r in results
            ],
            "total": total,
            "page": page,
            "page_size": page_size,
        },
    )


@alert_router.get("/fired/")
@has_permission(["can_view_alerts"])
def list_fired_alerts(request, page: int = 1, page_size: int = 20, metric_id: int | None = None):
    """List recent fired alert evaluations for the org."""
    orguser: OrgUser = request.orguser

    evaluations, total = AlertService.list_fired_evaluations(
        org=orguser.org,
        page=page,
        page_size=page_size,
        metric_id=metric_id,
    )

    return api_response(
        success=True,
        data={
            "data": [TriggeredAlertEventResponse.from_model(e).dict() for e in evaluations],
            "total": total,
            "page": page,
            "page_size": page_size,
        },
    )


@alert_router.get("/{alert_id}/")
@has_permission(["can_view_alerts"])
def get_alert(request, alert_id: int):
    """Get a specific alert"""
    orguser: OrgUser = request.orguser

    try:
        alert = AlertService.get_alert(alert_id, orguser.org)
        last_eval = None
        last_fired = None
        evaluations = alert.evaluations.order_by("-created_at")
        first_eval = evaluations.first()
        first_fired = evaluations.filter(fired=True).first()
        if first_eval:
            last_eval = first_eval.created_at
        if first_fired:
            last_fired = first_fired.created_at
        streak = AlertService.compute_fire_streak(alert)

        return api_response(
            success=True,
            data=AlertResponse.from_model(
                alert,
                last_evaluated_at=last_eval,
                last_fired_at=last_fired,
                fire_streak=streak,
            ).dict(),
        )
    except AlertNotFoundError as err:
        raise HttpError(404, str(err)) from err


@alert_router.post("/")
@has_permission(["can_create_alerts"])
def create_alert(request, payload: AlertCreate):
    """Create a new alert"""
    orguser: OrgUser = request.orguser

    try:
        alert = AlertService.create_alert(payload, orguser)
        return api_response(
            success=True,
            data=AlertResponse.from_model(alert).dict(),
            message="Alert created successfully",
        )
    except AlertValidationError as err:
        raise HttpError(400, str(err)) from err
    except Exception as e:
        logger.error(f"Error creating alert: {e}")
        raise HttpError(500, "Failed to create alert") from e


@alert_router.post("/test")
@has_permission(["can_create_alerts"])
def test_alert(request, payload: AlertTestRequest):
    """Test an alert — execute query and return paginated results"""
    orguser: OrgUser = request.orguser

    try:
        result = AlertService.test_alert(payload, orguser.org)
        return api_response(
            success=True,
            data=AlertTestResponse(
                would_fire=result["would_fire"],
                total_rows=result["total_rows"],
                results=result["results"],
                page=result["page"],
                page_size=result["page_size"],
                query_executed=result["query_executed"],
                rendered_message=result["rendered_message"],
            ).dict(),
        )
    except AlertWarehouseError as err:
        raise HttpError(502, str(err)) from err
    except Exception as e:
        logger.error(f"Error testing alert: {e}")
        raise HttpError(500, "Failed to test alert") from e


@alert_router.put("/{alert_id}/")
@has_permission(["can_edit_alerts"])
def update_alert(request, alert_id: int, payload: AlertUpdate):
    """Update an alert"""
    orguser: OrgUser = request.orguser

    try:
        alert = AlertService.update_alert(alert_id, orguser.org, orguser, payload)
        return api_response(
            success=True,
            data=AlertResponse.from_model(alert).dict(),
            message="Alert updated successfully",
        )
    except AlertNotFoundError as err:
        raise HttpError(404, str(err)) from err
    except AlertValidationError as err:
        raise HttpError(400, str(err)) from err


@alert_router.delete("/{alert_id}/")
@has_permission(["can_delete_alerts"])
def delete_alert(request, alert_id: int):
    """Delete an alert"""
    orguser: OrgUser = request.orguser

    try:
        AlertService.delete_alert(alert_id, orguser.org)
        return api_response(success=True, message="Alert deleted successfully")
    except AlertNotFoundError as err:
        raise HttpError(404, str(err)) from err


@alert_router.get("/{alert_id}/evaluations/")
@has_permission(["can_view_alerts"])
def get_evaluations(request, alert_id: int, page: int = 1, page_size: int = 20):
    """Get paginated evaluation history for an alert"""
    orguser: OrgUser = request.orguser

    try:
        evaluations, total = AlertService.get_evaluations(
            alert_id, orguser.org, page=page, page_size=page_size
        )
        return api_response(
            success=True,
            data={
                "data": [AlertEvaluationResponse.from_model(e).dict() for e in evaluations],
                "total": total,
                "page": page,
                "page_size": page_size,
            },
        )
    except AlertNotFoundError as err:
        raise HttpError(404, str(err)) from err

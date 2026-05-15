from ninja import Router
from ninja.errors import HttpError

from ddpui.auth import has_permission
from ddpui.models.admin_audit_log import AdminAuditLog

admin_audit_router = Router()


@admin_audit_router.get("/v1/audit-logs")
@admin_audit_router.get("/v1/audit-logs/")
@has_permission(["can_manage_organization"])
def get_audit_logs(
    request,
    org_id: int | None = None,
    action: str | None = None,
):
    """List admin audit logs with optional filters."""
    queryset = AdminAuditLog.objects.select_related("org").all()

    if org_id:
        queryset = queryset.filter(org_id=org_id)

    if action:
        queryset = queryset.filter(action=action)

    logs = [
        {
            "id": log.id,
            "org_id": log.org.id,
            "org_name": log.org.name,
            "action": log.action,
            "old_data": log.old_data,
            "new_data": log.new_data,
            "created_at": log.created_at,
        }
        for log in queryset
    ]

    return {
        "success": True,
        "count": len(logs),
        "data": logs,
    }


@admin_audit_router.get("/v1/audit-logs/{log_id}")
@admin_audit_router.get("/v1/audit-logs/{log_id}/")
@has_permission(["can_manage_organization"])
def get_single_audit_log(request, log_id: int):
    """Get single admin audit log."""
    log = (
        AdminAuditLog.objects.select_related("org")
        .filter(id=log_id)
        .first()
    )

    if not log:
        raise HttpError(404, "Audit log not found")

    return {
        "success": True,
        "data": {
            "id": log.id,
            "org_id": log.org.id,
            "org_name": log.org.name,
            "action": log.action,
            "old_data": log.old_data,
            "new_data": log.new_data,
            "created_at": log.created_at,
        },
    }

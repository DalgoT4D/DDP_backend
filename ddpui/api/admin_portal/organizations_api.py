from django.db.models import Q
from ninja import Router
from ninja.errors import HttpError

from ddpui.auth import has_permission
from ddpui.models.org import Org
from ddpui.models.admin_audit_log import AdminAuditLog
from ddpui.schemas.admin_org_schema import UpdateOrganizationSchema

admin_org_router = Router()


@admin_org_router.get("/v1/organizations")
@admin_org_router.get("/v1/organizations/")
@has_permission(["can_manage_organization"])
def get_admin_organizations(request, name: str | None = None):
    """List organizations with optional filtering."""
    queryset = Org.objects.all()

    if name:
        queryset = queryset.filter(name__icontains=name)

    orgs = list(queryset.values("id", "name", "slug"))

    return {
        "success": True,
        "count": len(orgs),
        "data": orgs,
    }


@admin_org_router.get("/v1/organizations/{org_id}")
@admin_org_router.get("/v1/organizations/{org_id}/")
@has_permission(["can_manage_organization"])
def get_single_org(request, org_id: int):
    """Get single organization by ID."""
    org = (
        Org.objects.filter(id=org_id)
        .values("id", "name", "slug")
        .first()
    )

    if not org:
        raise HttpError(404, "Organization not found")

    return {
        "success": True,
        "data": org,
    }


@admin_org_router.put("/v1/organizations/{org_id}")
@admin_org_router.put("/v1/organizations/{org_id}/")
@has_permission(["can_manage_organization"])
def update_organization(
    request,
    org_id: int,
    payload: UpdateOrganizationSchema,
):
    """Update organization details."""
    org = Org.objects.filter(id=org_id).first()

    if not org:
        raise HttpError(404, "Organization not found")

    existing_slug = (
        Org.objects.filter(slug=payload.slug)
        .exclude(id=org_id)
        .exists()
    )

    if existing_slug:
        raise HttpError(400, "Slug already exists")

    old_data = {
        "name": org.name,
        "slug": org.slug,
    }

    org.name = payload.name
    org.slug = payload.slug
    org.save()

    new_data = {
        "name": org.name,
        "slug": org.slug,
    }

    AdminAuditLog.objects.create(
        org=org,
        action="organization_updated",
        old_data=old_data,
        new_data=new_data,
    )

    return {
        "success": True,
        "message": "Organization updated successfully",
        "data": {
            "id": org.id,
            "name": org.name,
            "slug": org.slug,
        },
    }


@admin_org_router.get("/v1/audit-logs")
@admin_org_router.get("/v1/audit-logs/")
@has_permission(["can_manage_organization"])
def get_audit_logs(
    request,
    limit: int = 20,
    offset: int = 0,
    action: str | None = None,
    org_id: int | None = None,
    search: str | None = None,
    order_by: str = "-created_at",
):
    """List admin audit logs with pagination, filtering and ordering."""

    if limit < 1 or limit > 100:
        raise HttpError(400, "limit must be between 1 and 100")

    if offset < 0:
        raise HttpError(400, "offset must be non-negative")

    allowed_order_fields = [
        "created_at",
        "-created_at",
        "action",
        "-action",
    ]

    if order_by not in allowed_order_fields:
        raise HttpError(400, "Invalid order_by field")

    queryset = AdminAuditLog.objects.select_related("org")

    if action:
        queryset = queryset.filter(action=action)

    if org_id:
        queryset = queryset.filter(org_id=org_id)

    if search:
        queryset = queryset.filter(
            Q(org__name__icontains=search)
            | Q(action__icontains=search)
        )

    queryset = queryset.order_by(order_by)

    total_count = queryset.count()

    logs = list(
        queryset[offset : offset + limit].values(
            "id",
            "action",
            "created_at",
            "org__id",
            "org__name",
            "old_data",
            "new_data",
        )
    )

    return {
        "success": True,
        "count": total_count,
        "limit": limit,
        "offset": offset,
        "data": logs,
    }


@admin_org_router.get("/v1/audit-logs/{log_id}")
@admin_org_router.get("/v1/audit-logs/{log_id}/")
@has_permission(["can_manage_organization"])
def get_single_audit_log(request, log_id: int):
    """Get single audit log by ID."""
    log = (
        AdminAuditLog.objects.select_related("org")
        .filter(id=log_id)
        .values(
            "id",
            "action",
            "created_at",
            "org__id",
            "org__name",
            "old_data",
            "new_data",
        )
        .first()
    )

    if not log:
        raise HttpError(404, "Audit log not found")

    return {
        "success": True,
        "data": log,
    }

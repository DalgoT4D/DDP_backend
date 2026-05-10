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

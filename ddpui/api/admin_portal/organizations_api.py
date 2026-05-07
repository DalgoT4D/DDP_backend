from ninja import Router
from ninja.errors import HttpError

from ddpui.auth import has_permission
from ddpui.models.org import Org

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
    
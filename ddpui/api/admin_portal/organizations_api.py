from ninja import Router
from django.http import Http404
from ddpui.models.org import Org

admin_org_router = Router()


@admin_org_router.get("/v1/organizations")
@admin_org_router.get("/v1/organizations/")
def get_admin_organizations(request, name: str = None):
    """
    List organizations with optional filtering
    """
    queryset = Org.objects.all()

    if name:
        queryset = queryset.filter(name__icontains=name)

    orgs = queryset.values("id", "name", "slug")

    return {
        "success": True,
        "count": queryset.count(),
        "data": list(orgs)
    }


@admin_org_router.get("/v1/organizations/{org_id}")
@admin_org_router.get("/v1/organizations/{org_id}/")
def get_single_org(request, org_id: int):
    """
    Get single organization by ID
    """
    org = Org.objects.filter(id=org_id).values("id", "name", "slug").first()

    if not org:
        raise Http404("Organization not found")

    return {
        "success": True,
        "data": org
    }
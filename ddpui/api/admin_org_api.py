from typing import List
from ninja import Router
from ninja.errors import HttpError
from django.shortcuts import get_object_or_404
from django.db import transaction

from ddpui.auth import is_system_admin
from ddpui.models.org import Org, OrgDbt
from ddpui.schemas.org_schema import OrgSchema, CreateOrgSchema
from ddpui.services.audit_service import AuditService
from ddpui.core import orgfunctions

admin_org_router = Router()

@admin_org_router.get("/organizations/", response=List[OrgSchema])
@is_system_admin
def list_organizations(request):
    """
    Returns a list of all organizations. Platform Admin only.
    """
    orgs = Org.objects.all()
    return [
        OrgSchema(name=org.name, airbyte_workspace_id=org.airbyte_workspace_id, slug=org.slug)
        for org in orgs
    ]

@admin_org_router.get("/organizations/{slug}/", response=OrgSchema)
@is_system_admin
def get_organization(request, slug: str):
    """
    Returns details of a specific organization. Platform Admin only.
    """
    org = get_object_or_404(Org, slug=slug)
    return OrgSchema(name=org.name, airbyte_workspace_id=org.airbyte_workspace_id, slug=org.slug)

@admin_org_router.post("/organizations/", response=OrgSchema)
@is_system_admin
@transaction.atomic
def create_organization_admin(request, payload: CreateOrgSchema):
    """
    Creates a new organization. Platform Admin only.
    """
    org, error = orgfunctions.create_organization(payload)
    if error:
        raise HttpError(400, error)
    
    # Log the action
    AuditService.log_action(
        user=request.user,
        action="org_create",
        resource_type="Org",
        resource_id=org.slug,
        org=org,
        payload=payload.model_dump(),
        request=request
    )
    
    return OrgSchema(name=org.name, airbyte_workspace_id=org.airbyte_workspace_id, slug=org.slug)

@admin_org_router.put("/organizations/{slug}/", response=OrgSchema)
@is_system_admin
@transaction.atomic
def update_organization_admin(request, slug: str, payload: CreateOrgSchema):
    """
    Updates an existing organization. Platform Admin only.
    """
    org = get_object_or_404(Org, slug=slug)
    
    old_data = {
        "name": org.name,
        "slug": org.slug
    }
    
    org.name = payload.name
    # Update other fields as necessary. Currently OrgSchema is simple.
    org.save()
    
    # Log the action
    AuditService.log_action(
        user=request.user,
        action="org_update",
        resource_type="Org",
        resource_id=org.slug,
        org=org,
        payload={"old": old_data, "new": payload.model_dump()},
        request=request
    )
    
    return OrgSchema(name=org.name, airbyte_workspace_id=org.airbyte_workspace_id, slug=org.slug)

@admin_org_router.delete("/organizations/{slug}/")
@is_system_admin
@transaction.atomic
def delete_organization_admin(request, slug: str):
    """
    Deactivates or deletes an organization. Platform Admin only.
    """
    org = get_object_or_404(Org, slug=slug)
    
    # In a real system, we'd probably soft delete or archive.
    # For now, let's log the attempt and delete if safe.
    org_name = org.name
    org.delete()
    
    # Log the action
    AuditService.log_action(
        user=request.user,
        action="org_delete",
        resource_type="Org",
        resource_id=slug,
        payload={"name": org_name},
        request=request
    )
    
    return {"success": True, "message": f"Organization {org_name} deleted."}

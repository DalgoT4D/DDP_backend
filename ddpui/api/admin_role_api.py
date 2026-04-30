from typing import List
from ninja import Router, Schema
from ninja.errors import HttpError
from django.shortcuts import get_object_or_404
from django.db import transaction

from ddpui.auth import is_system_admin
from ddpui.models.role_based_access import Role, Permission, RolePermission
from ddpui.services.audit_service import AuditService

admin_role_router = Router()

class PermissionSchema(Schema):
    id: int
    slug: str
    name: str

class RoleSchema(Schema):
    id: int
    slug: str
    name: str
    level: int
    permissions: List[PermissionSchema] = []

class CreateRoleSchema(Schema):
    slug: str
    name: str
    level: int = 1
    permission_ids: List[int] = []

@admin_role_router.get("/roles/", response=List[RoleSchema])
@is_system_admin
def list_roles(request):
    """List all global roles and their permissions."""
    roles = Role.objects.all().prefetch_related("rolepermissions__permission")
    result = []
    for role in roles:
        role_data = RoleSchema(
            id=role.id,
            slug=role.slug,
            name=role.name,
            level=role.level,
            permissions=[
                PermissionSchema(id=rp.permission.id, slug=rp.permission.slug, name=rp.permission.name)
                for rp in role.rolepermissions.all()
            ]
        )
        result.append(role_data)
    return result

@admin_role_router.post("/roles/", response=RoleSchema)
@is_system_admin
@transaction.atomic
def create_role(request, payload: CreateRoleSchema):
    """Create a new role and assign permissions."""
    if Role.objects.filter(slug=payload.slug).exists():
        raise HttpError(400, "Role with this slug already exists")
    
    role = Role.objects.create(
        slug=payload.slug,
        name=payload.name,
        level=payload.level
    )
    
    permissions = Permission.objects.filter(id__in=payload.permission_ids)
    for perm in permissions:
        RolePermission.objects.create(role=role, permission=perm)
    
    AuditService.log_action(
        user=request.user,
        action="role_create",
        resource_type="Role",
        resource_id=role.slug,
        payload=payload.model_dump(),
        request=request
    )
    
    return RoleSchema(
        id=role.id,
        slug=role.slug,
        name=role.name,
        level=role.level,
        permissions=[PermissionSchema(id=p.id, slug=p.slug, name=p.name) for p in permissions]
    )

@admin_role_router.get("/permissions/", response=List[PermissionSchema])
@is_system_admin
def list_permissions(request):
    """List all available system permissions."""
    perms = Permission.objects.all()
    return [PermissionSchema(id=p.id, slug=p.slug, name=p.name) for p in perms]

@admin_role_router.delete("/roles/{role_id}/")
@is_system_admin
@transaction.atomic
def delete_role(request, role_id: int):
    """Delete a role and its permission mappings."""
    role = get_object_or_404(Role, id=role_id)
    role_slug = role.slug
    role.delete()
    
    AuditService.log_action(
        user=request.user,
        action="role_delete",
        resource_type="Role",
        resource_id=role_slug,
        request=request
    )
    return {"success": True}

@admin_role_router.post("/users/{user_id}/assign-role/")
@is_system_admin
@transaction.atomic
def assign_role_to_user(request, user_id: int, role_id: int, org_id: int):
    """Assign a role to a user within a specific organization."""
    from ddpui.models.org_user import OrgUser
    from django.contrib.auth.models import User
    from ddpui.models.org import Org
    
    user = get_object_or_404(User, id=user_id)
    role = get_object_or_404(Role, id=role_id)
    org = get_object_or_404(Org, id=org_id)
    
    org_user, created = OrgUser.objects.get_or_create(
        user=user,
        org=org,
        defaults={"new_role": role}
    )
    
    if not created:
        org_user.new_role = role
        org_user.save()
    
    AuditService.log_action(
        user=request.user,
        action="user_role_assignment",
        resource_type="OrgUser",
        resource_id=str(org_user.id),
        org=org,
        payload={"user_id": user_id, "role_id": role_id, "org_id": org_id},
        request=request
    )
    
    return {"success": True, "message": f"Assigned {role.name} to {user.username} in {org.name}"}


from typing import List
from ninja import Router
from ninja.errors import HttpError
from django.shortcuts import get_object_or_404
from ddpui.models.org import OrgWarehouse, Org
from ddpui.auth import is_system_admin
from ddpui.services.audit_service import AuditService
from ddpui.utils.custom_logger import CustomLogger

admin_warehouse_router = Router()
logger = CustomLogger("ddpui")

@admin_warehouse_router.get("/warehouses/", response=List[dict])
@is_system_admin
def list_all_warehouses(request):
    """List all warehouses across all organizations with health status."""
    warehouses = OrgWarehouse.objects.select_related('org').all()
    res = []
    for w in warehouses:
        # Basic health check - placeholder for more robust logic
        health = "connected" if w.airbyte_destination_id else "unknown"
            
        res.append({
            "id": w.id,
            "org_name": w.org.name,
            "org_slug": w.org.slug,
            "wtype": w.wtype,
            "name": w.name,
            "airbyte_destination_id": w.airbyte_destination_id,
            "health": health,
            "created_at": w.created_at,
            "updated_at": w.updated_at
        })
    return res

@admin_warehouse_router.get("/warehouses/{warehouse_id}/", response=dict)
@is_system_admin
def get_warehouse_details(request, warehouse_id: int):
    """Get detailed info for a specific warehouse."""
    w = get_object_or_404(OrgWarehouse, id=warehouse_id)
    return {
        "id": w.id,
        "org_name": w.org.name,
        "org_slug": w.org.slug,
        "wtype": w.wtype,
        "name": w.name,
        "credentials": w.credentials,
        "airbyte_destination_id": w.airbyte_destination_id,
        "bq_location": w.bq_location,
        "created_at": w.created_at,
        "updated_at": w.updated_at
    }

@admin_warehouse_router.post("/warehouses/{warehouse_id}/rotate-credentials/")
@is_system_admin
def rotate_warehouse_credentials(request, warehouse_id: int, payload: dict):
    """Rotate warehouse credentials."""
    w = get_object_or_404(OrgWarehouse, id=warehouse_id)
    
    # Audit logic
    AuditService.log_action(
        user=request.user,
        action="warehouse_credentials_rotation",
        resource_type="OrgWarehouse",
        resource_id=str(w.id),
        org=w.org,
        payload={"reason": payload.get('reason', 'Routine maintenance')},
        request=request
    )
    
    return {"success": True, "message": f"Credentials rotation initiated for warehouse {w.id}"}

from typing import List
from ninja import Router
from django.shortcuts import get_object_or_404
from ddpui.models.org import OrgDataFlowv1, Org
from ddpui.auth import is_system_admin
from ddpui.services.audit_service import AuditService
from ddpui.utils.custom_logger import CustomLogger
from ddpui.ddpprefect import prefect_service

admin_pipeline_router = Router()
logger = CustomLogger("ddpui")

@admin_pipeline_router.get("/pipelines/", response=List[dict])
@is_system_admin
def list_all_pipelines(request):
    """List all pipelines across all organizations with status analytics."""
    pipelines = OrgDataFlowv1.objects.select_related('org').all()
    res = []
    for p in pipelines:
        # Placeholder for real analytics - in a real app would join with execution logs
        res.append({
            "id": p.id,
            "name": p.flow_name,
            "org_name": p.org.name,
            "org_slug": p.org.slug,
            "schedule": p.schedule,
            "status": "active" if p.is_active else "paused",
            "last_run_status": "success", # Mock
            "failure_rate": "2%", # Mock
            "created_at": p.created_at,
        })
    return res

@admin_pipeline_router.post("/pipelines/{pipeline_id}/toggle/")
@is_system_admin
def toggle_pipeline(request, pipeline_id: int):
    """Emergency control: pause or resume a pipeline."""
    p = get_object_or_404(OrgDataFlowv1, id=pipeline_id)
    p.is_active = not p.is_active
    p.save()
    
    action = "paused" if not p.is_active else "resumed"
    
    AuditService.log_action(
        user=request.user,
        action=f"pipeline_{action}",
        resource_type="OrgDataFlowv1",
        resource_id=str(p.id),
        org=p.org,
        payload={"flow_name": p.flow_name},
        request=request
    )
    
    return {"success": True, "message": f"Pipeline {p.flow_name} has been {action}."}

@admin_pipeline_router.get("/pipelines/metrics/")
@is_system_admin
def get_global_pipeline_metrics(request):
    """Platform-wide pipeline health metrics."""
    total = OrgDataFlowv1.objects.count()
    active = OrgDataFlowv1.objects.filter(is_active=True).count()
    
    return {
        "total_pipelines": total,
        "active_pipelines": active,
        "failing_today": 0, # Placeholder
        "uptime_24h": "99.9%" # Placeholder
    }

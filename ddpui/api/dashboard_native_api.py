"""Native Dashboard API endpoints"""

from typing import Optional, List
from datetime import timedelta

from ninja import Router
from ninja.errors import HttpError
from django.db import transaction
from django.db.models import Prefetch

from ddpui.models.dashboard import (
    Dashboard,
    DashboardFilter,
    DashboardLock,
    DashboardFilterType,
)
from ddpui.models.org_user import OrgUser
from ddpui.models.visualization import Chart
from ddpui.auth import extract_resource, has_permission, has_resource_permission
from ddpui.core.sharing import coverage, sharing_actions
from ddpui.core.sharing.access_resolver import effective_permission
from ddpui.core.sharing.chart_access import chart_ids_in_tabs, dashboard_chart_ids
from ddpui.core.sharing.exceptions import SharingPermissionError, SharingValidationError
from ddpui.schemas.access_schema import DashboardChartCoverageResponse, EmbedCoverageConfirmation
from ddpui.utils.custom_logger import CustomLogger
from ddpui.services.dashboard_service import (
    DashboardService,
    DashboardData,
    FilterData,
    DashboardNotFoundError,
    DashboardLockedError,
    DashboardPermissionError,
    DashboardServiceError,
    FilterNotFoundError,
    FilterValidationError,
    delete_dashboard_safely,
)
from ddpui.schemas.dashboard_schema import (
    DashboardCreate,
    DashboardUpdate,
    DashboardResponse,
    DashboardFilterResponse,
    FilterCreate,
    FilterUpdate,
    FilterOptionResponse,
    FilterOptionsResponse,
    LockResponse,
    DashboardShareToggle,
    DashboardShareResponse,
    DashboardShareStatus,
    LandingPageResponse,
    LandingPageResolveResponse,
)

logger = CustomLogger("ddpui")

dashboard_native_router = Router()


def _pool_level(request) -> Optional[str]:
    """Viewer's level on ``request.resource``, read off the pool ③ attached —
    no extra query. "edit" beats "view"; ③ passing guarantees at least view."""
    pool = getattr(request, "resource_permissions", None) or set()
    return "edit" if "can_edit_dashboards" in pool else "view" if pool else None


# Endpoints
@dashboard_native_router.get("/", response=List[DashboardResponse])
@has_permission(["can_view_dashboards"])
def list_dashboards(
    request,
    dashboard_type: Optional[str] = None,
    search: Optional[str] = None,
    is_published: Optional[bool] = None,
):
    """List all dashboards with optional filters"""
    orguser: OrgUser = request.orguser

    dashboards = DashboardService.list_dashboards(
        org=orguser.org,
        orguser=orguser,
        dashboard_type=dashboard_type,
        search=search,
        is_published=is_published,
    )

    return [
        DashboardResponse(**DashboardService.get_dashboard_response(d, orguser=orguser))
        for d in dashboards
    ]


@dashboard_native_router.get("/{dashboard_id}/", response=DashboardResponse)
@has_permission(["can_view_dashboards"])
@extract_resource("dashboard")
@has_resource_permission("can_view_dashboards")
def get_dashboard(request, dashboard_id: int):
    """Get a specific dashboard"""
    orguser: OrgUser = request.orguser
    dashboard = request.resource

    return DashboardResponse(
        **DashboardService.get_dashboard_response(dashboard, orguser=orguser),
        user_permission=_pool_level(request),
    )


@dashboard_native_router.post("/", response=DashboardResponse)
@has_permission(["can_create_dashboards"])
def create_dashboard(request, payload: DashboardCreate):
    """Create a new dashboard"""
    orguser: OrgUser = request.orguser

    dashboard_data = DashboardData(
        title=payload.title,
        description=payload.description,
        grid_columns=payload.grid_columns,
    )
    dashboard = DashboardService.create_dashboard(dashboard_data, orguser)

    # --- Custom logic for org default and landing dashboard (permission-driven) ---
    has_org_default = Dashboard.objects.filter(org=orguser.org, is_org_default=True).exists()

    # If no org default dashboard exists, assign based on permission
    if not has_org_default:
        if "can_manage_org_default_dashboard" in getattr(request, "permissions", []):
            dashboard.is_org_default = True
            dashboard.save(update_fields=["is_org_default"])
        else:
            # If user does not have permission and has no landing_dashboard, set this as landing_dashboard
            if not orguser.landing_dashboard:
                orguser.landing_dashboard = dashboard
                orguser.save(update_fields=["landing_dashboard"])

    return DashboardResponse(**DashboardService.get_dashboard_response(dashboard, orguser=orguser))


@dashboard_native_router.get(
    "/{dashboard_id}/chart-coverage/", response=DashboardChartCoverageResponse
)
@has_permission(["can_view_dashboards"])
@extract_resource("dashboard")
@has_resource_permission("can_edit_dashboards")
def get_dashboard_chart_coverage(request, dashboard_id: int, chart_id: Optional[int] = None):
    """Chart-coverage verdicts for this dashboard. With ``chart_id``: that one
    chart's verdict (the embed pre-flight — the chart need not be a tile yet).
    Without: every under-covering tile. Gate: dashboard edit."""
    orguser: OrgUser = request.orguser
    dashboard = request.resource

    if chart_id is not None:
        chart = Chart.objects.filter(id=chart_id, org=orguser.org).first()
        if chart is None:
            raise HttpError(404, "Chart not found")
        verdicts = coverage.coverage_for_charts(orguser, dashboard, [chart])
    else:
        verdicts = coverage.dashboard_under_covering_charts(orguser, dashboard)

    return DashboardChartCoverageResponse(
        dashboard_id=dashboard.id,
        covered=all(v.covered for v in verdicts),
        charts=verdicts,
    )


def _validate_new_tile_charts(orguser: OrgUser, dashboard, payload: DashboardUpdate):
    """`update_dashboard` overwrites `tabs` as raw JSON — without this, any
    chart id could be embedded blind. Every chart id newly present in the
    incoming tabs must be org-owned (400), viewable by the caller (403), and —
    if it under-covers the dashboard's audience — confirmed via
    `extend_chart_ids`/`proceed`, else the coverage verdicts come back in a
    409 and nothing saves. A confirmed embed is never blocked.

    Returns (confirmation, charts_to_extend): a non-None confirmation means
    "reply 409, save nothing"; otherwise charts_to_extend is the confirmed
    subset to extend after the save commits.
    """
    if payload.tabs is None:
        return None, []

    incoming_ids = chart_ids_in_tabs([tab.model_dump() for tab in payload.tabs])
    new_ids = incoming_ids - dashboard_chart_ids(dashboard)
    if not new_ids:
        return None, []

    charts = {c.id: c for c in Chart.objects.filter(id__in=new_ids, org=orguser.org)}
    missing = sorted(new_ids - charts.keys())
    if missing:
        # cross-org ids are indistinguishable from nonexistent ones
        raise HttpError(400, f"unknown chart ids in tabs payload: {missing}")

    for chart in charts.values():
        if effective_permission(orguser, "chart", chart) is None:
            raise HttpError(403, "You do not have access to this chart")

    verdicts = coverage.coverage_for_charts(orguser, dashboard, list(charts.values()))
    under_covering = [v for v in verdicts if not v.covered]
    if not under_covering:
        # Clean coverage returns before any subset check — unlike
        # `sharing_actions._validate_extend_subset`, which 400s a garbage
        # extend_chart_ids even when clean. Mind this before unifying the two.
        return None, []

    confirmed = payload.extend_chart_ids is not None or bool(payload.proceed)
    if not confirmed:
        # 409, not a 200-with-flag: the 200 response shape (DashboardResponse)
        # stays stable for every already-working save path.
        return EmbedCoverageConfirmation(under_covering_charts=under_covering), []

    # Inline copy of `sharing_actions._validate_extend_subset` — the embed path
    # runs before the dashboard save, off a different shape. Once here,
    # under_covering is non-empty and both copies validate identically.
    extend_ids = set(payload.extend_chart_ids or [])
    warned_ids = {v.chart_id for v in under_covering}
    if not extend_ids <= warned_ids:
        raise HttpError(400, "extend_chart_ids must be a subset of the under-covering charts")
    return None, [charts[chart_id] for chart_id in sorted(extend_ids)]


@dashboard_native_router.put(
    "/{dashboard_id}/", response={200: DashboardResponse, 409: EmbedCoverageConfirmation}
)
@has_permission(["can_view_dashboards"])
@extract_resource("dashboard")
@has_resource_permission("can_edit_dashboards")
def update_dashboard(request, dashboard_id: int, payload: DashboardUpdate):
    """Update dashboard with auto-save support. Chart ids newly present in
    ``tabs`` are validated, and under-covering embeds 409 with the coverage
    verdicts unless the request carries the embed confirmation."""
    orguser: OrgUser = request.orguser
    dashboard = request.resource

    confirmation, charts_to_extend = _validate_new_tile_charts(orguser, dashboard, payload)
    if confirmation is not None:
        return 409, confirmation

    try:
        dashboard = DashboardService.update_dashboard(
            dashboard_id=dashboard_id,
            org=orguser.org,
            orguser=orguser,
            data=payload,
        )
    except DashboardNotFoundError as err:
        raise HttpError(404, "Dashboard not found") from err
    except DashboardLockedError as err:
        raise HttpError(423, err.message) from err

    if charts_to_extend:
        try:
            sharing_actions.extend_charts_to_cover_dashboard(orguser, dashboard, charts_to_extend)
        except SharingPermissionError as err:
            # The embed itself is saved (never blocked once confirmed); the
            # extend half needs Edit on each chart and fails loudly.
            raise HttpError(403, err.message) from err

    # plain (not `200, ...`) so ninja still infers 200 and direct-call tests
    # keep receiving the DashboardResponse itself
    return DashboardResponse(
        **DashboardService.get_dashboard_response(dashboard, orguser=orguser),
        user_permission=_pool_level(request),
    )


@dashboard_native_router.delete("/{dashboard_id}/")
@has_permission(["can_delete_dashboards"])
def delete_dashboard(request, dashboard_id: int):
    """Delete a dashboard"""
    orguser: OrgUser = request.orguser

    try:
        DashboardService.delete_dashboard(dashboard_id, orguser.org, orguser)
    except DashboardNotFoundError as err:
        raise HttpError(404, "Dashboard not found") from err
    except DashboardPermissionError as err:
        raise HttpError(403, err.message) from err
    except DashboardLockedError as err:
        raise HttpError(423, "Cannot delete a locked dashboard") from err

    return {"success": True}


@dashboard_native_router.post("/{dashboard_id}/duplicate/", response=DashboardResponse)
@has_permission(["can_create_dashboards"])
@extract_resource("dashboard")
@has_resource_permission("can_view_dashboards")
def duplicate_dashboard(request, dashboard_id: int):
    """Duplicate a dashboard with all its configurations, filters, and tabs.
    The copy inherits the source's general access — a same-or-narrower audience
    by construction, so no coverage warning is needed. Grants and public-link
    state are not copied.

    Gate: view on the original — duplicating clones its full content, same
    exposure as reading it directly. The create slug stays in ①."""
    orguser: OrgUser = request.orguser
    original_dashboard = request.resource

    # The copy inherits the source's general access, not the org's defaults:
    # seeding at wider defaults would silently broaden a deliberately
    # narrowed source around the same tiles, bypassing the coverage warnings.
    analyst_level = original_dashboard.analyst_level
    member_level = original_dashboard.member_level

    with transaction.atomic():
        new_dashboard = Dashboard.objects.create(
            title=f"Copy of {original_dashboard.title}",
            description=original_dashboard.description,
            dashboard_type=original_dashboard.dashboard_type,
            grid_columns=original_dashboard.grid_columns,
            target_screen_size=original_dashboard.target_screen_size,
            created_by=orguser,
            org=orguser.org,
            last_modified_by=orguser,
            analyst_level=analyst_level,
            member_level=member_level,
        )

        # Copy all filters and create ID mapping
        filter_id_mapping = {}  # old_filter_id -> new_filter_id

        for original_filter in original_dashboard.filters.all():
            new_filter = DashboardFilter.objects.create(
                dashboard=new_dashboard,
                name=original_filter.name,
                filter_type=original_filter.filter_type,
                schema_name=original_filter.schema_name,
                table_name=original_filter.table_name,
                column_name=original_filter.column_name,
                settings=original_filter.settings,
                order=original_filter.order,
            )
            filter_id_mapping[str(original_filter.id)] = str(new_filter.id)

        new_dashboard.tabs = DashboardService.copy_tabs_with_filter_remapping(
            original_dashboard.tabs or [], filter_id_mapping
        )
        new_dashboard.save()

        logger.info(
            f"Duplicated dashboard {dashboard_id} as {new_dashboard.id} for org {orguser.org.id}"
        )

    return DashboardResponse(
        **DashboardService.get_dashboard_response(new_dashboard, orguser=orguser)
    )


# Dashboard Lock endpoints
@dashboard_native_router.post("/{dashboard_id}/lock/", response=LockResponse)
@has_permission(["can_view_dashboards"])
@extract_resource("dashboard")
@has_resource_permission("can_edit_dashboards")
def lock_dashboard(request, dashboard_id: int):
    """Lock dashboard for editing"""
    orguser: OrgUser = request.orguser

    try:
        lock_info = DashboardService.lock_dashboard(dashboard_id, orguser.org, orguser)
    except DashboardNotFoundError as err:
        raise HttpError(404, "Dashboard not found") from err
    except DashboardLockedError as err:
        raise HttpError(423, err.message) from err

    return LockResponse(
        lock_token=lock_info.lock_token,
        expires_at=lock_info.expires_at,
        locked_by=lock_info.locked_by_email,
    )


@dashboard_native_router.put("/{dashboard_id}/lock/refresh/")
@has_permission(["can_view_dashboards"])
@extract_resource("dashboard")
@has_resource_permission("can_edit_dashboards")
def refresh_dashboard_lock(request, dashboard_id: int):
    """Refresh dashboard lock to extend expiry"""
    orguser: OrgUser = request.orguser

    try:
        lock_info = DashboardService.refresh_lock(dashboard_id, orguser.org, orguser)
    except DashboardNotFoundError as err:
        raise HttpError(404, "Dashboard not found") from err
    except DashboardPermissionError as err:
        raise HttpError(403, err.message) from err
    except DashboardServiceError as err:
        if err.error_code == "LOCK_EXPIRED":
            raise HttpError(410, "Lock has expired") from err
        elif err.error_code == "NO_LOCK":
            raise HttpError(404, "No active lock found") from err
        raise HttpError(400, err.message) from err

    return LockResponse(
        lock_token=lock_info.lock_token,
        expires_at=lock_info.expires_at,
        locked_by=lock_info.locked_by_email,
    )


@dashboard_native_router.delete("/{dashboard_id}/lock/")
@has_permission(["can_view_dashboards"])
@extract_resource("dashboard")
@has_resource_permission("can_edit_dashboards")
def unlock_dashboard(request, dashboard_id: int):
    """Unlock dashboard"""
    orguser: OrgUser = request.orguser

    try:
        DashboardService.unlock_dashboard(dashboard_id, orguser.org, orguser)
    except DashboardNotFoundError as err:
        raise HttpError(404, "Dashboard not found") from err
    except DashboardPermissionError as err:
        raise HttpError(403, err.message) from err

    return {"success": True}


# Filter endpoints
@dashboard_native_router.post("/{dashboard_id}/filters/", response=DashboardFilterResponse)
@has_permission(["can_view_dashboards"])
@extract_resource("dashboard")
@has_resource_permission("can_edit_dashboards")
def create_filter(request, dashboard_id: int, payload: FilterCreate):
    """Add a filter to dashboard"""
    orguser: OrgUser = request.orguser

    try:
        filter_data = FilterData(
            name=payload.name,
            filter_type=payload.filter_type,
            schema_name=payload.schema_name,
            table_name=payload.table_name,
            column_name=payload.column_name,
            settings=payload.settings,
            order=payload.order,
        )
        filter_obj = DashboardService.create_filter(dashboard_id, orguser.org, filter_data)
    except DashboardNotFoundError as err:
        raise HttpError(404, "Dashboard not found") from err
    except FilterValidationError as err:
        raise HttpError(400, err.message) from err

    return DashboardFilterResponse(**filter_obj.to_json())


@dashboard_native_router.get(
    "/{dashboard_id}/filters/{filter_id}/", response=DashboardFilterResponse
)
@has_permission(["can_view_dashboards"])
@extract_resource("dashboard")
@has_resource_permission("can_view_dashboards")
def get_filter(request, dashboard_id: int, filter_id: int):
    """Get a specific dashboard filter"""
    orguser: OrgUser = request.orguser

    try:
        filter_obj = DashboardService.get_filter(dashboard_id, filter_id, orguser.org)
    except DashboardNotFoundError as err:
        raise HttpError(404, "Dashboard not found") from err
    except FilterNotFoundError as err:
        raise HttpError(404, "Filter not found") from err

    return DashboardFilterResponse(**filter_obj.to_json())


@dashboard_native_router.put(
    "/{dashboard_id}/filters/{filter_id}/", response=DashboardFilterResponse
)
@has_permission(["can_view_dashboards"])
@extract_resource("dashboard")
@has_resource_permission("can_edit_dashboards")
def update_filter(request, dashboard_id: int, filter_id: int, payload: FilterUpdate):
    """Update a dashboard filter"""
    orguser: OrgUser = request.orguser

    try:
        filter_obj = DashboardService.update_filter(
            dashboard_id=dashboard_id,
            filter_id=filter_id,
            org=orguser.org,
            data=payload,
        )
    except DashboardNotFoundError as err:
        raise HttpError(404, "Dashboard not found") from err
    except FilterNotFoundError as err:
        raise HttpError(404, "Filter not found") from err
    except FilterValidationError as err:
        raise HttpError(400, err.message) from err

    return DashboardFilterResponse(**filter_obj.to_json())


@dashboard_native_router.delete("/{dashboard_id}/filters/{filter_id}/")
@has_permission(["can_view_dashboards"])
@extract_resource("dashboard")
@has_resource_permission("can_edit_dashboards")
def delete_filter(request, dashboard_id: int, filter_id: int):
    """Delete a dashboard filter"""
    orguser: OrgUser = request.orguser

    try:
        DashboardService.delete_filter(dashboard_id, filter_id, orguser.org)
    except DashboardNotFoundError as err:
        raise HttpError(404, "Dashboard not found") from err
    except FilterNotFoundError as err:
        raise HttpError(404, "Filter not found") from err

    return {"success": True}


# Filter options endpoint
@dashboard_native_router.get("/filter-options/", response=FilterOptionsResponse)
@has_permission(["can_view_dashboards"])
def get_filter_options(
    request, schema_name: str, table_name: str, column_name: str, limit: int = 100
):
    """Get distinct values for a column to use in filters"""
    orguser: OrgUser = request.orguser

    # Get org warehouse
    from ddpui.models.org import OrgWarehouse

    org_warehouse = OrgWarehouse.objects.filter(org=orguser.org).first()
    if not org_warehouse:
        raise HttpError(400, "No warehouse configured for organization")

    # Get filter options from service
    options = DashboardService.generate_filter_options(
        schema=schema_name,
        table=table_name,
        column=column_name,
        org_warehouse=org_warehouse,
        limit=limit,
    )

    return FilterOptionsResponse(options=options, total_count=len(options))


# ===== Dashboard Sharing Endpoints =====


@dashboard_native_router.put("/{dashboard_id}/share/")
@has_permission(["can_share_dashboards"])
@extract_resource("dashboard")
@has_resource_permission("can_edit_dashboards")
def toggle_dashboard_sharing(request, dashboard_id: int, payload: DashboardShareToggle):
    """Toggle public sharing for a dashboard. Any editor with the share slug
    may toggle, not just the creator."""
    orguser: OrgUser = request.orguser
    dashboard = request.resource

    is_public = payload.is_public

    # The actual flip lives in `sharing_actions.set_public` (shared with the
    # bulk toggle). Enabling without `proceed` returns the under-covering
    # charts and flips nothing; the client re-sends with proceed=true.
    try:
        under_covering = sharing_actions.set_public(
            orguser, "dashboard", dashboard, is_public, proceed=bool(payload.proceed)
        )
    except SharingValidationError as err:
        raise HttpError(403, err.message) from err

    if under_covering:
        return DashboardShareResponse(
            is_public=dashboard.is_public,
            message="Confirmation required: enabling the public link exposes charts inline",
            requires_confirmation=True,
            under_covering_charts=under_covering,
        )

    # Build response
    response_data = {
        "is_public": dashboard.is_public,
        "message": f'Dashboard {"made public" if is_public else "made private"}',
    }

    if dashboard.is_public and dashboard.public_share_token:
        # Generate the full public URL
        from django.conf import settings

        # Use FRONTEND_URL_V2 for webapp_v2, fallback to FRONTEND_URL, then localhost
        FRONTEND_URL_V2 = getattr(settings, "FRONTEND_URL_V2", None)
        frontend_url = FRONTEND_URL_V2 or getattr(settings, "FRONTEND_URL", "http://localhost:3001")
        response_data[
            "public_url"
        ] = f"{frontend_url}/share/dashboard/{dashboard.public_share_token}"
        response_data["public_share_token"] = dashboard.public_share_token

    # Audit logging
    action = "enabled_public_sharing" if is_public else "disabled_public_sharing"
    logger.info(
        f"Dashboard {dashboard_id} sharing {action} by user {orguser.user.email}, token: {dashboard.public_share_token}"
    )

    return DashboardShareResponse(**response_data)


@dashboard_native_router.get("/{dashboard_id}/share/")
@has_permission(["can_view_dashboards"])
@extract_resource("dashboard")
@has_resource_permission("can_view_dashboards")
def get_dashboard_sharing_status(request, dashboard_id: int):
    """Get dashboard sharing status. View access suffices: the status GET
    only reveals whether a resource the viewer can already see is public."""
    orguser: OrgUser = request.orguser
    dashboard = request.resource

    response_data = {
        "is_public": dashboard.is_public,
        "public_access_count": dashboard.public_access_count,
        "last_public_accessed": dashboard.last_public_accessed,
        "public_shared_at": dashboard.public_shared_at,
    }

    if dashboard.is_public and dashboard.public_share_token:
        from django.conf import settings

        # Use FRONTEND_URL_V2 for webapp_v2, fallback to FRONTEND_URL, then localhost
        FRONTEND_URL_V2 = getattr(settings, "FRONTEND_URL_V2", None)
        frontend_url = FRONTEND_URL_V2 or getattr(settings, "FRONTEND_URL", "http://localhost:3001")
        response_data[
            "public_url"
        ] = f"{frontend_url}/share/dashboard/{dashboard.public_share_token}"

    return DashboardShareStatus(**response_data)


# =============================================================================
# Landing Page Management APIs
# =============================================================================


@dashboard_native_router.post(
    "/landing-page/set-personal/{dashboard_id}", response=LandingPageResponse
)
@has_permission(["can_view_dashboards", "can_create_dashboards"])
def set_personal_landing_dashboard(request, dashboard_id: int):
    """Set a dashboard as user's personal landing page"""
    orguser: OrgUser = request.orguser

    # Check if dashboard exists and belongs to user's org
    try:
        dashboard = Dashboard.objects.get(id=dashboard_id, org=orguser.org)
    except Dashboard.DoesNotExist as err:
        raise HttpError(404, "Dashboard not found") from err

    # Set as personal landing page
    orguser.landing_dashboard = dashboard
    orguser.save()

    logger.info(
        f"User {orguser.user.email} set dashboard {dashboard.title} as personal landing page"
    )
    return LandingPageResponse(success=True, message="Dashboard set as personal landing page")


@dashboard_native_router.delete("/landing-page/remove-personal", response=LandingPageResponse)
@has_permission(["can_view_dashboards", "can_create_dashboards"])
def remove_personal_landing_dashboard(request):
    """Remove user's personal landing page preference"""
    orguser: OrgUser = request.orguser

    if orguser.landing_dashboard:
        previous_dashboard = orguser.landing_dashboard.title
        orguser.landing_dashboard = None
        orguser.save()

        logger.info(
            f"User {orguser.user.email} removed personal landing page: {previous_dashboard}"
        )
        return LandingPageResponse(success=True, message="Personal landing page preference removed")
    else:
        return LandingPageResponse(success=True, message="No personal landing page was set")


@dashboard_native_router.post(
    "/landing-page/set-org-default/{dashboard_id}", response=LandingPageResponse
)
@has_permission(["can_manage_org_default_dashboard"])
def set_org_default_dashboard(request, dashboard_id: int):
    """Set a dashboard as organization's default landing page (Admin only)"""
    orguser: OrgUser = request.orguser

    # Check if dashboard exists and belongs to user's org
    try:
        dashboard = Dashboard.objects.get(id=dashboard_id, org=orguser.org)
    except Dashboard.DoesNotExist as err:
        raise HttpError(404, "Dashboard not found") from err

    with transaction.atomic():
        # Remove previous org default
        Dashboard.objects.filter(org=orguser.org, is_org_default=True).update(is_org_default=False)

        # Set new org default
        dashboard.is_org_default = True
        dashboard.save()

    logger.info(
        f"User {orguser.user.email} set dashboard {dashboard.title} as org default landing page"
    )
    return LandingPageResponse(
        success=True, message="Dashboard set as organization default landing page"
    )


@dashboard_native_router.delete("/landing-page/remove-org-default", response=LandingPageResponse)
@has_permission(["can_manage_org_default_dashboard"])
def remove_org_default_dashboard(request):
    """Remove organization's default landing page (Admin only)"""
    orguser: OrgUser = request.orguser

    org_default = Dashboard.objects.filter(org=orguser.org, is_org_default=True).first()
    if org_default:
        org_default.is_org_default = False
        org_default.save()

        logger.info(
            f"User {orguser.user.email} removed org default landing page: {org_default.title}"
        )
        return LandingPageResponse(
            success=True, message="Organization default landing page removed"
        )
    else:
        return LandingPageResponse(
            success=True, message="No organization default landing page was set"
        )


@dashboard_native_router.get("/landing-page/resolve", response=dict)
@has_permission(["can_view_dashboards"])
def resolve_user_landing_page(request):
    """Resolve which dashboard should be the user's landing page"""
    orguser: OrgUser = request.orguser

    # 1. Check personal preference first
    if orguser.landing_dashboard:
        return {
            "dashboard_id": orguser.landing_dashboard.id,
            "dashboard_title": orguser.landing_dashboard.title,
            "dashboard_type": orguser.landing_dashboard.dashboard_type,
            "source": "personal",
        }

    # 2. Check org default
    org_default = Dashboard.objects.filter(org=orguser.org, is_org_default=True).first()
    if org_default:
        return {
            "dashboard_id": org_default.id,
            "dashboard_title": org_default.title,
            "dashboard_type": org_default.dashboard_type,
            "source": "org_default",
        }

    # 3. No landing page set
    return {"dashboard_id": None, "dashboard_title": None, "dashboard_type": None, "source": "none"}

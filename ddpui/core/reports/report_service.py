"""Report service — snapshot creation, viewing, CRUD"""

from typing import Optional, List, Dict, Any
from datetime import date

from django.db.models import Q

from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser
from ddpui.models.dashboard import Dashboard
from ddpui.models.report import ReportSnapshot, SnapshotStatus
from ddpui.models.visualization import Chart
from ddpui.utils.custom_logger import CustomLogger

from .exceptions import SnapshotNotFoundError, SnapshotValidationError

logger = CustomLogger("ddpui.core.reports")

ALLOWED_UPDATE_FIELDS = {"summary"}


class ReportService:
    """Service class for snapshot operations"""

    # =========================================================================
    # Config Freezing
    # =========================================================================

    @staticmethod
    def _freeze_dashboard(dashboard: Dashboard) -> Dict[str, Any]:
        """Freeze dashboard layout, structure & filters into one dict."""
        filters = dashboard.filters.all().order_by("order")
        return {
            "title": dashboard.title,
            "description": dashboard.description,
            "grid_columns": dashboard.grid_columns,
            "target_screen_size": dashboard.target_screen_size,
            "layout_config": dashboard.layout_config,
            "components": dashboard.components,
            "filter_layout": dashboard.filter_layout,
            "filters": [f.to_json() for f in filters],
        }

    @staticmethod
    def _freeze_chart_configs(dashboard: Dashboard) -> Dict[str, Any]:
        """
        Layer 3: Freeze ALL chart configs referenced in dashboard components.

        Walks through components, extracts chartId from each chart component,
        batch-fetches the Chart records, and stores full configs.
        """
        components = dashboard.components or {}
        chart_ids = []

        for comp_id, component in components.items():
            if component.get("type") == "chart":
                chart_id = component.get("config", {}).get("chartId")
                if chart_id:
                    chart_ids.append(chart_id)

        charts = Chart.objects.filter(id__in=chart_ids, org=dashboard.org)
        frozen = {}
        for chart in charts:
            frozen[str(chart.id)] = {
                "id": chart.id,
                "title": chart.title,
                "description": chart.description,
                "chart_type": chart.chart_type,
                "schema_name": chart.schema_name,
                "table_name": chart.table_name,
                "extra_config": chart.extra_config,
            }
        return frozen

    # =========================================================================
    # Snapshot CRUD
    # =========================================================================

    @staticmethod
    def create_snapshot(
        title: str,
        dashboard_id: int,
        date_column: Dict[str, str],
        period_end: date,
        orguser: OrgUser,
        period_start: Optional[date] = None,
    ) -> ReportSnapshot:
        """
        Create a snapshot from a dashboard.

        Steps:
        1. Validate date range
        2. Validate dashboard exists
        3. Validate date_column matches a datetime filter on the dashboard
        4. Freeze 2 layers of config
        5. Create snapshot record (no FK to dashboard — fully self-contained)
        """
        if period_start is not None and period_start > period_end:
            raise SnapshotValidationError("period_start must be before period_end")

        # Fetch dashboard with filters prefetched (used only for freezing)
        try:
            dashboard = Dashboard.objects.prefetch_related("filters").get(
                id=dashboard_id, org=orguser.org
            )
        except Dashboard.DoesNotExist:
            raise SnapshotValidationError(f"Dashboard {dashboard_id} not found")

        # Validate date_column against dashboard's datetime filters
        datetime_filters = dashboard.filters.filter(filter_type="datetime")
        match = datetime_filters.filter(
            schema_name=date_column["schema_name"],
            table_name=date_column["table_name"],
            column_name=date_column["column_name"],
        ).exists()
        if not match:
            raise SnapshotValidationError(
                f"No datetime filter found for "
                f"{date_column['schema_name']}.{date_column['table_name']}.{date_column['column_name']}"
            )

        frozen_dashboard = ReportService._freeze_dashboard(dashboard)
        frozen_chart_configs = ReportService._freeze_chart_configs(dashboard)

        snapshot = ReportSnapshot.objects.create(
            title=title,
            date_column=date_column,
            period_start=period_start,
            period_end=period_end,
            frozen_dashboard=frozen_dashboard,
            frozen_chart_configs=frozen_chart_configs,
            created_by=orguser,
            org=orguser.org,
        )

        logger.info(f"Created snapshot {snapshot.id} from dashboard {dashboard_id}")
        return snapshot

    @staticmethod
    def list_snapshots(org: Org, search: Optional[str] = None) -> List[ReportSnapshot]:
        """List all snapshots for an org."""
        query = Q(org=org)
        if search:
            query &= Q(title__icontains=search)
        return list(
            ReportSnapshot.objects.filter(query)
            .select_related("created_by__user")
            .order_by("-created_at")
        )

    @staticmethod
    def get_snapshot(snapshot_id: int, org: Org) -> ReportSnapshot:
        """Get a snapshot by ID."""
        try:
            return ReportSnapshot.objects.select_related("created_by__user").get(
                id=snapshot_id, org=org
            )
        except ReportSnapshot.DoesNotExist:
            raise SnapshotNotFoundError(snapshot_id)

    @staticmethod
    def get_snapshot_view_data(snapshot_id: int, org: Org) -> Dict[str, Any]:
        """
        Get full data to render a snapshot in the frontend.

        Returns dashboard_data shaped like Dashboard.to_json() so
        DashboardNativeView can render it directly.
        """
        snapshot = ReportService.get_snapshot(snapshot_id, org)

        # Mark as viewed
        if snapshot.status == SnapshotStatus.GENERATED.value:
            snapshot.status = SnapshotStatus.VIEWED.value
            snapshot.save(update_fields=["status"])

        # Build dashboard-like response from frozen dashboard
        # No dashboard_id — snapshot is fully self-contained
        dashboard_data = {
            **snapshot.frozen_dashboard,
            "id": snapshot.id,
            "dashboard_type": "native",
            "is_published": True,
            "is_locked": False,
            "locked_by": None,
            "is_public": False,
            "created_by": snapshot.created_by.user.email if snapshot.created_by else None,
            "org_id": snapshot.org.id,
            "created_at": snapshot.created_at.isoformat(),
            "updated_at": snapshot.created_at.isoformat(),
        }

        report_metadata = {
            "snapshot_id": snapshot.id,
            "title": snapshot.title,
            "date_column": snapshot.date_column,
            "period_start": snapshot.period_start.isoformat() if snapshot.period_start else None,
            "period_end": snapshot.period_end.isoformat(),
            "summary": snapshot.summary,
            "status": snapshot.status,
            "created_at": snapshot.created_at.isoformat(),
            "created_by": snapshot.created_by.user.email if snapshot.created_by else None,
            "dashboard_title": snapshot.frozen_dashboard.get("title", ""),
        }

        return {
            "dashboard_data": dashboard_data,
            "report_metadata": report_metadata,
            "frozen_chart_configs": snapshot.frozen_chart_configs or {},
        }

    @staticmethod
    def update_snapshot(snapshot_id: int, org: Org, **fields) -> ReportSnapshot:
        """Update mutable fields on a snapshot."""
        snapshot = ReportService.get_snapshot(snapshot_id, org)
        update_fields = []
        for field, value in fields.items():
            if field not in ALLOWED_UPDATE_FIELDS:
                raise SnapshotValidationError(f"Field '{field}' is not editable")
            if value is not None:
                setattr(snapshot, field, value)
                update_fields.append(field)
        if update_fields:
            snapshot.save(update_fields=update_fields)
        return snapshot

    @staticmethod
    def delete_snapshot(snapshot_id: int, org: Org) -> bool:
        """Delete a snapshot."""
        snapshot = ReportService.get_snapshot(snapshot_id, org)
        snapshot.delete()
        logger.info(f"Deleted snapshot {snapshot_id}")
        return True

"""Report service — snapshot creation, viewing, CRUD"""

import copy
from typing import Optional, List, Dict, Any
from datetime import date

from django.db import transaction
from django.db.models import Q

from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser
from ddpui.models.dashboard import Dashboard
from ddpui.models.report import ReportSnapshot, SnapshotStatus
from ddpui.models.visualization import Chart
from ddpui.utils.custom_logger import CustomLogger

from .exceptions import (
    SnapshotNotFoundError,
    SnapshotValidationError,
    SnapshotPermissionError,
)

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
    # Filter Injection for View Time
    # =========================================================================

    @staticmethod
    def _inject_period_into_filters(
        frozen_dashboard: Dict[str, Any], snapshot: "ReportSnapshot"
    ) -> bool:
        """Inject snapshot period dates into the matching datetime filter.

        Finds the frozen datetime filter matching snapshot.date_column and
        sets default_start_date / default_end_date + locked=True in its
        settings.

        When no matching filter exists (warehouse-discovered column), injects
        a display-only filter with a negative numeric ID so the UI shows the
        locked date range. Actual data filtering for this case is handled
        separately via _inject_period_into_chart_configs.

        Returns True if a pre-existing filter was matched, False if a
        display-only filter was injected instead.
        Mutates frozen_dashboard in place (caller must deep-copy first).
        """
        date_col = snapshot.date_column
        if not date_col:
            return True  # Nothing to inject

        filters = frozen_dashboard.get("filters")
        if filters is None:
            filters = []
            frozen_dashboard["filters"] = filters

        period_settings = {
            "default_start_date": (
                snapshot.period_start.isoformat() if snapshot.period_start else None
            ),
            "default_end_date": (
                snapshot.period_end.isoformat() if snapshot.period_end else None
            ),
            "locked": True,
        }

        # Try to find and enrich the matching datetime filter
        for f in filters:
            if (
                f.get("filter_type") == "datetime"
                and f.get("schema_name") == date_col.get("schema_name")
                and f.get("table_name") == date_col.get("table_name")
                and f.get("column_name") == date_col.get("column_name")
            ):
                settings = f.get("settings") or {}
                settings.update(period_settings)
                f["settings"] = settings
                return True

        # No matching dashboard filter — inject a display-only filter.
        # Use a negative numeric ID so the frontend can render it without
        # hitting "expected a number" errors from Django integer fields.
        display_filter = {
            "id": -snapshot.id,
            "dashboard_id": snapshot.id,
            "name": f"Date Filter ({date_col.get('column_name', '')})",
            "filter_type": "datetime",
            "schema_name": date_col.get("schema_name", ""),
            "table_name": date_col.get("table_name", ""),
            "column_name": date_col.get("column_name", ""),
            "settings": period_settings,
            "order": 0,
            "created_at": snapshot.created_at.isoformat(),
            "updated_at": snapshot.created_at.isoformat(),
        }
        filters.insert(0, display_filter)
        return False

    @staticmethod
    def _inject_period_into_chart_configs(
        frozen_chart_configs: Dict[str, Any], snapshot: "ReportSnapshot"
    ) -> None:
        """Inject date range filters directly into frozen chart configs.

        Used when the date column was discovered via warehouse introspection
        and has no matching dashboard-level datetime filter. Adds chart-level
        filters (column/operator/value) so the backend applies them when
        fetching chart data.

        Mutates frozen_chart_configs in place.
        """
        date_col = snapshot.date_column
        if not date_col or not frozen_chart_configs:
            return

        col_name = date_col.get("column_name", "")
        target_schema = date_col.get("schema_name", "")
        target_table = date_col.get("table_name", "")

        for chart_id, config in frozen_chart_configs.items():
            if (
                config.get("schema_name") == target_schema
                and config.get("table_name") == target_table
            ):
                extra_config = config.get("extra_config") or {}
                filters = list(extra_config.get("filters") or [])

                if snapshot.period_start:
                    filters.append(
                        {
                            "column": col_name,
                            "operator": "greater_than_equal",
                            "value": snapshot.period_start.isoformat(),
                        }
                    )
                if snapshot.period_end:
                    filters.append(
                        {
                            "column": col_name,
                            "operator": "less_than_equal",
                            "value": snapshot.period_end.isoformat()
                            + "T23:59:59",
                        }
                    )

                extra_config["filters"] = filters
                config["extra_config"] = extra_config

    # =========================================================================
    # Snapshot CRUD
    # =========================================================================

    @staticmethod
    @transaction.atomic
    def create_snapshot(
        title: str,
        dashboard_id: int,
        date_column: Dict[str, str],
        period_end: date,
        orguser: OrgUser,
        period_start: Optional[date] = None,
    ) -> ReportSnapshot:
        """Create a snapshot from a dashboard.

        Freezes dashboard layout and chart configs at snapshot creation time,
        allowing the snapshot to be viewed even if the original dashboard or
        charts are later deleted. All database operations are performed within
        a transaction to ensure consistency.

        Args:
            title: User-provided title for the snapshot
            dashboard_id: ID of the dashboard to snapshot
            date_column: Dictionary with {schema_name, table_name, column_name}
                        identifying the datetime column for period filtering
            period_end: End of reporting period (inclusive)
            orguser: The user creating the snapshot
            period_start: Start of reporting period (inclusive). None means no lower bound.

        Returns:
            ReportSnapshot: Created snapshot instance with frozen config

        Raises:
            SnapshotValidationError: If date range is invalid (start > end),
                                    dashboard doesn't exist, or date_column is
                                    not a valid datetime column in the warehouse
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

        # Validate date_column: first check dashboard filters, then fall back
        # to verifying the column exists in the warehouse as a datetime type
        datetime_filters = dashboard.filters.filter(filter_type="datetime")
        match_on_filter = datetime_filters.filter(
            schema_name=date_column["schema_name"],
            table_name=date_column["table_name"],
            column_name=date_column["column_name"],
        ).exists()

        if not match_on_filter:
            # Fallback: verify the column exists in the warehouse as datetime
            from ddpui.models.org import OrgWarehouse
            from ddpui.core.charts.charts_service import get_warehouse_client
            from ddpui.api.filter_api import (
                get_table_columns,
                determine_filter_type_from_column,
            )

            org_warehouse = OrgWarehouse.objects.filter(org=orguser.org).first()
            if not org_warehouse:
                raise SnapshotValidationError("Warehouse not configured")

            warehouse_client = get_warehouse_client(org_warehouse)
            all_columns = get_table_columns(
                warehouse_client,
                org_warehouse,
                date_column["schema_name"],
                date_column["table_name"],
            )

            # Find the specific column
            target_col = None
            for col in all_columns:
                if col["column_name"] == date_column["column_name"]:
                    target_col = col
                    break

            if not target_col:
                raise SnapshotValidationError(
                    f"Column '{date_column['column_name']}' not found in "
                    f"{date_column['schema_name']}.{date_column['table_name']}"
                )

            col_type = determine_filter_type_from_column(target_col["data_type"])
            if col_type != "datetime":
                raise SnapshotValidationError(
                    f"Column '{date_column['column_name']}' is not a datetime column "
                    f"(type: {target_col['data_type']})"
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
    def list_snapshots(
        org: Org,
        search: Optional[str] = None,
        dashboard_title: Optional[str] = None,
        created_by_email: Optional[str] = None,
    ) -> List[ReportSnapshot]:
        """List all snapshots for an organization with optional filtering.

        Args:
            org: The organization to list snapshots for
            search: Optional search term to filter snapshots by title (case-insensitive)
            dashboard_title: Optional filter for snapshots from dashboards with matching title
            created_by_email: Optional filter for snapshots created by user with matching email

        Returns:
            List[ReportSnapshot]: List of matching snapshots ordered by creation date (newest first)
        """
        query = Q(org=org)
        if search:
            query &= Q(title__icontains=search)
        if dashboard_title:
            query &= Q(frozen_dashboard__title__icontains=dashboard_title)
        if created_by_email:
            query &= Q(created_by__user__email__icontains=created_by_email)
        return list(
            ReportSnapshot.objects.filter(query)
            .select_related("created_by__user")
            .order_by("-created_at")
        )

    @staticmethod
    def get_snapshot(snapshot_id: int, org: Org) -> ReportSnapshot:
        """Get a snapshot by ID for an organization.

        Args:
            snapshot_id: The snapshot ID to retrieve
            org: The organization to filter by

        Returns:
            ReportSnapshot: The matching snapshot instance with creator user info

        Raises:
            SnapshotNotFoundError: If snapshot doesn't exist or doesn't belong to org
        """
        try:
            return ReportSnapshot.objects.select_related("created_by__user").get(
                id=snapshot_id, org=org
            )
        except ReportSnapshot.DoesNotExist:
            raise SnapshotNotFoundError(snapshot_id)

    @staticmethod
    def get_snapshot_view_data(snapshot_id: int, org: Org) -> Dict[str, Any]:
        """Get full data to render a snapshot in the frontend.

        Returns dashboard_data shaped like Dashboard.to_json() so
        DashboardNativeView can render it directly. Automatically injects
        period filters into either dashboard filters or chart configs.

        Args:
            snapshot_id: The snapshot ID to retrieve view data for
            org: The organization to filter by

        Returns:
            Dict containing:
                - dashboard_data: Dashboard config with injected period filters
                - report_metadata: Snapshot metadata (title, dates, status, etc.)
                - frozen_chart_configs: Chart configurations with injected filters

        Raises:
            SnapshotNotFoundError: If snapshot doesn't exist or doesn't belong to org
        """
        snapshot = ReportService.get_snapshot(snapshot_id, org)

        # Mark as viewed
        if snapshot.status == SnapshotStatus.GENERATED.value:
            snapshot.status = SnapshotStatus.VIEWED.value
            snapshot.save(update_fields=["status"])

        # Build dashboard-like response from frozen dashboard
        # No dashboard_id — snapshot is fully self-contained
        # Deep-copy so we never mutate the stored frozen_dashboard
        frozen_copy = copy.deepcopy(snapshot.frozen_dashboard)

        # Inject period dates into the matching datetime filter's settings
        # so the frontend auto-applies them and renders charts pre-filtered.
        # If no matching dashboard filter exists (warehouse-discovered column),
        # inject date filters directly into each chart's extra_config instead.
        filter_matched = ReportService._inject_period_into_filters(
            frozen_copy, snapshot
        )

        frozen_charts = copy.deepcopy(snapshot.frozen_chart_configs or {})
        if not filter_matched:
            ReportService._inject_period_into_chart_configs(
                frozen_charts, snapshot
            )

        dashboard_data = {
            **frozen_copy,
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
            "frozen_chart_configs": frozen_charts,
        }

    @staticmethod
    def update_snapshot(snapshot_id: int, org: Org, **fields) -> ReportSnapshot:
        """Update mutable fields on a snapshot.

        Only allows updates to fields in ALLOWED_UPDATE_FIELDS (currently: summary).
        Frozen dashboard and chart configs cannot be modified after creation.

        Args:
            snapshot_id: The snapshot ID to update
            org: The organization to filter by
            **fields: Field names and values to update (must be in ALLOWED_UPDATE_FIELDS)

        Returns:
            ReportSnapshot: The updated snapshot instance

        Raises:
            SnapshotNotFoundError: If snapshot doesn't exist or doesn't belong to org
            SnapshotValidationError: If attempting to update a non-editable field
        """
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
    def delete_snapshot(snapshot_id: int, org: Org, orguser: OrgUser) -> bool:
        """Delete a snapshot.

        Args:
            snapshot_id: The snapshot ID
            org: The organization
            orguser: The user deleting the snapshot

        Returns:
            True if deletion was successful

        Raises:
            SnapshotNotFoundError: If snapshot doesn't exist
            SnapshotPermissionError: If user doesn't have permission
        """
        snapshot = ReportService.get_snapshot(snapshot_id, org)

        # Only allow deletion if the current user is the creator
        if snapshot.created_by != orguser:
            raise SnapshotPermissionError("You can only delete reports you created.")

        snapshot.delete()
        logger.info(f"Deleted snapshot {snapshot_id} by user {orguser.user.email}")
        return True

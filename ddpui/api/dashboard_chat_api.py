"""
Dashboard Chat API endpoints for AI-powered dashboard analysis.
Provides context-aware chat functionality for dashboard insights.
"""

import json
import asyncio
import time
from typing import Optional, List, Dict, Any, Union
from django.http import JsonResponse, StreamingHttpResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods
from django.utils.decorators import method_decorator
from django.views import View
from ninja import Router, Schema
from ninja.security import HttpBearer

from ddpui.auth import has_permission
from ddpui.utils.custom_logger import CustomLogger
from ddpui.core.ai.factory import get_default_ai_provider
from ddpui.core.ai.interfaces import AIMessage
from ddpui.models.dashboard import Dashboard

# Remove unused import - orguser is accessed via request.orguser

logger = CustomLogger("ddpui.api.dashboard_chat")

# Ninja router for Dashboard Chat API
router = Router()


# Dashboard chat API routes are working - see OpenAPI spec at /api/docs


# Pydantic schemas
class DashboardChatMessage(Schema):
    role: str  # "user", "assistant", "system"
    content: str
    timestamp: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


class DashboardChatRequest(Schema):
    messages: List[DashboardChatMessage]
    include_data: bool = False  # Whether to include actual data or just schema
    max_rows: int = 100  # Maximum rows to include if data is enabled
    selected_chart_id: Optional[str] = None
    stream: bool = False
    provider_type: Optional[str] = None


class DashboardContextRequest(Schema):
    include_data: bool = False
    max_rows: int = 100


class DashboardChatResponse(Schema):
    content: str
    usage: Optional[Dict[str, int]] = None
    context_included: bool
    data_included: bool
    metadata: Optional[Dict[str, Any]] = None


class DashboardContextAnalyzer:
    """
    Analyzes dashboard context to provide AI with relevant information.
    """

    def __init__(
        self, dashboard_id: int, orguser_obj, include_data: bool = False, max_rows: int = 100
    ):
        self.dashboard_id = dashboard_id
        self.orguser = orguser_obj
        self.include_data = include_data
        self.max_rows = max_rows
        self.logger = CustomLogger("dashboard_context")

    def get_dashboard_context(self) -> Dict[str, Any]:
        """
        Get comprehensive dashboard context for AI analysis.

        Returns:
            Dictionary containing dashboard metadata, charts, and optionally data
        """
        try:
            # Get dashboard metadata
            dashboard = Dashboard.objects.filter(id=self.dashboard_id, org=self.orguser.org).first()

            if not dashboard:
                return {"error": "Dashboard not found or access denied"}

            context = {
                "dashboard": {
                    "id": dashboard.id,
                    "title": dashboard.title,
                    "description": dashboard.description or "",
                    "dashboard_type": dashboard.dashboard_type,
                    "created_at": dashboard.created_at.isoformat(),
                    "updated_at": dashboard.updated_at.isoformat(),
                    "filter_layout": dashboard.filter_layout or {},
                    "target_screen_size": dashboard.target_screen_size or "desktop",
                },
                "charts": [],
                "filters": [],
                "data_sources": [],
                "summary": {
                    "total_charts": 0,
                    "chart_types": {},
                    "data_included": self.include_data,
                    "max_rows": self.max_rows if self.include_data else 0,
                },
            }

            # Get dashboard configuration and charts
            if dashboard.dashboard_type == "native":
                context.update(self._get_native_dashboard_context(dashboard))
            else:
                context.update(self._get_superset_dashboard_context(dashboard))

            return context

        except Exception as e:
            self.logger.error(f"Error getting dashboard context: {e}")
            return {"error": f"Failed to analyze dashboard context: {str(e)}"}

    def _get_native_dashboard_context(self, dashboard) -> Dict[str, Any]:
        """Get context for native dashboard type."""
        from ddpui.models.dashboard import DashboardFilter

        context_data = {"charts": [], "filters": [], "data_sources": []}

        try:
            # Get dashboard filters
            filters = DashboardFilter.objects.filter(dashboard=dashboard)
            for filter_obj in filters:
                filter_info = {
                    "id": filter_obj.id,
                    "name": filter_obj.name,
                    "filter_type": filter_obj.filter_type,
                    "schema_name": getattr(filter_obj, "schema_name", None),
                    "table_name": getattr(filter_obj, "table_name", None),
                    "column_name": getattr(filter_obj, "column_name", None),
                }
                context_data["filters"].append(filter_info)

            # Get charts from dashboard layout_config and components
            layout_config = getattr(dashboard, "layout_config", [])
            components = getattr(dashboard, "components", {})

            if isinstance(layout_config, str):
                layout_config = json.loads(layout_config)
            if isinstance(components, str):
                components = json.loads(components)

            chart_types = {}

            for layout_item in layout_config:
                component_id = layout_item.get("i")
                if not component_id or component_id not in components:
                    continue

                component = components[component_id]
                if component.get("type") != "chart":
                    continue

                try:
                    chart_config = component.get("config", {})
                    chart_id = chart_config.get("chartId")

                    if not chart_id:
                        continue

                    # Get chart metadata
                    chart_info = {
                        "id": str(chart_id),
                        "component_id": component_id,
                        "type": chart_config.get("chart_type", "unknown"),
                        "title": chart_config.get("title", f"Chart {chart_id}"),
                        "position": {
                            "x": layout_item.get("x", 0),
                            "y": layout_item.get("y", 0),
                            "w": layout_item.get("w", 12),
                            "h": layout_item.get("h", 8),
                        },
                    }

                    # Count chart types
                    chart_type = chart_info["type"]
                    chart_types[chart_type] = chart_types.get(chart_type, 0) + 1

                    # Get chart schema/data if requested
                    if self.include_data:
                        chart_info.update(self._get_chart_data_context(chart_id))
                    else:
                        chart_info.update(self._get_chart_schema_context(chart_id))

                    context_data["charts"].append(chart_info)

                except Exception as e:
                    self.logger.error(f"Error processing chart {component_id}: {e}")
                    continue

            context_data["summary"] = {
                "total_charts": len(context_data["charts"]),
                "chart_types": chart_types,
                "data_included": self.include_data,
                "max_rows": self.max_rows if self.include_data else 0,
            }

        except Exception as e:
            self.logger.error(f"Error getting native dashboard context: {e}")

        return context_data

    def _get_superset_dashboard_context(self, dashboard) -> Dict[str, Any]:
        """Get context for Superset dashboard type."""
        # For Superset dashboards, we have limited access to internal structure
        return {
            "charts": [],
            "filters": [],
            "data_sources": [],
            "superset_info": {
                "external_dashboard": True,
                "superset_id": getattr(dashboard, "superset_id", None),
            },
            "summary": {
                "total_charts": 0,
                "chart_types": {"superset": 1},
                "data_included": False,
                "max_rows": 0,
            },
        }

    def _get_chart_schema_context(self, chart_id: int) -> Dict[str, Any]:
        """Get chart schema information without actual data."""
        try:
            from ddpui.models.visualization import Chart

            chart = Chart.objects.filter(id=chart_id, org=self.orguser.org).first()

            if not chart:
                return {"schema": {}, "sample_data": None}

            # Extract schema information from chart configuration
            extra_config = chart.extra_config or {}

            return {
                "schema": {
                    "schema_name": chart.schema_name,
                    "table_name": chart.table_name,
                    "chart_type": chart.chart_type,
                    "computation_type": chart.computation_type,
                    "columns": extra_config.get("columns", []),
                    "metrics": extra_config.get("metrics", []),
                    "dimensions": extra_config.get("dimensions", []),
                    "filters": extra_config.get("filters", []),
                    "aggregate_functions": extra_config.get("aggregate_functions", []),
                },
                "sample_data": None,
            }
        except Exception as e:
            self.logger.error(f"Error getting chart schema for {chart_id}: {e}")
            return {"schema": {}, "sample_data": None}

    def _get_chart_data_context(self, chart_id: int) -> Dict[str, Any]:
        """Get chart data and schema information."""
        self.logger.info(f"=== Starting data fetch for chart {chart_id} ===")

        try:
            # Get schema first
            schema_info = self._get_chart_schema_context(chart_id)

            # Try to get sample data using chart data API
            sample_data = None
            try:
                from ddpui.core.charts.charts_service import get_chart_data_table_preview
                from ddpui.schemas.chart_schema import ChartDataPayload
                from ddpui.models.visualization import Chart
                from ddpui.models.org import OrgWarehouse

                # Get chart object to build proper payload
                chart = Chart.objects.filter(id=chart_id, org=self.orguser.org).first()

                if not chart:
                    self.logger.error(f"Chart {chart_id} not found in database")
                    raise Exception("Chart not found")

                self.logger.info(f"Chart {chart_id} found: {chart.title}")
                self.logger.info(f"Chart {chart_id} type: {chart.chart_type}")
                self.logger.info(f"Chart {chart_id} computation: {chart.computation_type}")
                self.logger.info(f"Chart {chart_id} table: {chart.schema_name}.{chart.table_name}")

                # Get org warehouse
                org_warehouse = OrgWarehouse.objects.filter(org=self.orguser.org).first()
                if not org_warehouse:
                    self.logger.error(f"No warehouse found for org {self.orguser.org.slug}")
                    raise Exception("Organization warehouse not configured")

                self.logger.info(f"Using warehouse: {org_warehouse.name} ({org_warehouse.wtype})")

                # Build chart data payload from chart configuration
                extra_config = chart.extra_config or {}
                self.logger.info(f"Chart {chart_id} extra_config keys: {list(extra_config.keys())}")
                self.logger.info(f"Chart {chart_id} full extra_config: {extra_config}")

                # Check if this is a problematic chart and force raw mode for debugging
                should_force_raw = False

                # Force raw if no metrics/aggregate functions
                if (
                    chart.computation_type == "aggregated"
                    and not extra_config.get("metrics")
                    and not extra_config.get("aggregate_functions")
                ):
                    should_force_raw = True
                    self.logger.warning(
                        f"Chart {chart_id} has aggregated type but no metrics/aggregate_functions - forcing raw mode"
                    )

                # Also force raw for chart types that typically need dimension_col but don't have it
                elif (
                    chart.computation_type == "aggregated"
                    and chart.chart_type.lower()
                    in ["table", "pie", "doughnut", "map", "choropleth"]
                    and not extra_config.get("dimension_col")
                    and not extra_config.get("columns")  # No columns available for inference
                ):
                    should_force_raw = True
                    self.logger.warning(
                        f"Chart {chart_id} is {chart.chart_type} type without dimension_col or columns - forcing raw mode"
                    )

                if should_force_raw:
                    forced_computation_type = "raw"
                else:
                    forced_computation_type = chart.computation_type

                # Extract required fields from extra_config based on computation type
                payload_kwargs = {
                    "chart_type": chart.chart_type,
                    "computation_type": forced_computation_type,  # Use forced type if needed
                    "schema_name": chart.schema_name,
                    "table_name": chart.table_name,
                    "extra_config": extra_config,
                    "limit": min(self.max_rows, 500),  # Cap at 500 for performance
                }

                # Add fields specific to computation type
                if forced_computation_type == "raw":
                    # For raw data charts
                    if extra_config.get("x_axis"):
                        payload_kwargs["x_axis"] = extra_config["x_axis"]
                    if extra_config.get("y_axis"):
                        payload_kwargs["y_axis"] = extra_config["y_axis"]

                    # If no x_axis/y_axis specified for raw data, we'll let the fallback handle it
                    self.logger.info(
                        f"Chart {chart_id} using raw mode with x_axis={payload_kwargs.get('x_axis')} y_axis={payload_kwargs.get('y_axis')}"
                    )

                elif forced_computation_type == "aggregated":
                    # For aggregated charts - these are required
                    if extra_config.get("dimension_col"):
                        payload_kwargs["dimension_col"] = extra_config["dimension_col"]
                    if extra_config.get("extra_dimension"):
                        payload_kwargs["extra_dimension"] = extra_config["extra_dimension"]

                    # Metrics are required for aggregated charts
                    metrics = extra_config.get("metrics", [])
                    self.logger.info(f"Chart {chart_id} original metrics from config: {metrics}")

                    if not metrics and extra_config.get("aggregate_functions"):
                        # Try to build metrics from aggregate_functions if available
                        metrics = []
                        agg_functions = extra_config.get("aggregate_functions", [])
                        self.logger.info(f"Chart {chart_id} aggregate_functions: {agg_functions}")

                        for agg_func in agg_functions:
                            if isinstance(agg_func, dict):
                                metrics.append(
                                    {
                                        "column": agg_func.get("column"),
                                        "aggregation": agg_func.get("function", "COUNT"),
                                        "alias": agg_func.get("alias"),
                                    }
                                )
                        self.logger.info(
                            f"Chart {chart_id} metrics built from aggregate_functions: {metrics}"
                        )

                    if metrics:
                        payload_kwargs["metrics"] = metrics
                        self.logger.info(f"Chart {chart_id} using metrics: {metrics}")
                    else:
                        # Fallback: create a basic COUNT metric if none found
                        fallback_metrics = [
                            {"aggregation": "COUNT", "alias": "count", "column": None}
                        ]
                        payload_kwargs["metrics"] = fallback_metrics
                        self.logger.warning(
                            f"Chart {chart_id} no metrics found, using fallback: {fallback_metrics}"
                        )

                    # Check if dimension_col is required for this chart type and metrics combination
                    chart_type_lower = chart.chart_type.lower()
                    requires_dimension = chart_type_lower in [
                        "table",
                        "pie",
                        "doughnut",
                        "map",
                        "choropleth",
                    ] or (
                        len(payload_kwargs.get("metrics", [])) > 1
                    )  # Multiple metrics need dimension

                    if requires_dimension and not payload_kwargs.get("dimension_col"):
                        # Try to find a suitable dimension column from available columns
                        self.logger.warning(
                            f"Chart {chart_id} ({chart_type_lower}) needs dimension_col but none specified. Attempting to infer..."
                        )

                        # Get available columns from the schema
                        available_columns = extra_config.get("columns", [])
                        if not available_columns and schema_info.get("schema", {}).get("columns"):
                            available_columns = schema_info["schema"]["columns"]

                        self.logger.info(
                            f"Chart {chart_id} available columns for dimension inference: {available_columns}"
                        )

                        # Try to find a suitable dimension column - be more aggressive
                        dimension_candidates = []

                        # First pass: look for obvious dimension columns
                        for col in available_columns:
                            col_name = col.get("name") if isinstance(col, dict) else str(col)
                            col_lower = col_name.lower()

                            # Prefer common dimension column names
                            if any(
                                keyword in col_lower
                                for keyword in [
                                    "category",
                                    "region",
                                    "type",
                                    "status",
                                    "name",
                                    "group",
                                ]
                            ):
                                dimension_candidates.insert(
                                    0, col_name
                                )  # Insert at beginning (higher priority)
                            # Skip obvious metric columns
                            elif not any(
                                keyword in col_lower
                                for keyword in [
                                    "sum",
                                    "count",
                                    "total",
                                    "amount",
                                    "avg",
                                    "revenue",
                                    "sales",
                                    "cost",
                                ]
                            ):
                                dimension_candidates.append(col_name)

                        # If no good candidates found, just use the first available column
                        if not dimension_candidates and available_columns:
                            first_col = available_columns[0]
                            dimension_candidates.append(
                                first_col.get("name")
                                if isinstance(first_col, dict)
                                else str(first_col)
                            )
                            self.logger.info(
                                f"Chart {chart_id} no ideal dimension candidates, using first column as fallback"
                            )

                        if dimension_candidates:
                            inferred_dimension = dimension_candidates[0]
                            self.logger.info(
                                f"Chart {chart_id} using inferred dimension_col: {inferred_dimension}"
                            )
                            payload_kwargs["dimension_col"] = inferred_dimension
                        else:
                            self.logger.warning(
                                f"Chart {chart_id} could not infer suitable dimension_col from {len(available_columns)} columns, forcing to single metric"
                            )
                            # If we can't find dimension, ensure we only have one metric to avoid the requirement
                            if len(payload_kwargs.get("metrics", [])) > 1:
                                payload_kwargs["metrics"] = payload_kwargs["metrics"][
                                    :1
                                ]  # Keep only first metric
                                self.logger.info(
                                    f"Chart {chart_id} reduced to single metric to avoid dimension requirement"
                                )

                # Handle map-specific fields
                if chart.chart_type.lower() in ["map", "choropleth"]:
                    if extra_config.get("geographic_column"):
                        payload_kwargs["geographic_column"] = extra_config["geographic_column"]
                    if extra_config.get("value_column"):
                        payload_kwargs["value_column"] = extra_config["value_column"]
                    if extra_config.get("selected_geojson_id"):
                        payload_kwargs["selected_geojson_id"] = extra_config["selected_geojson_id"]

                # Add customizations if available
                if extra_config.get("customizations"):
                    payload_kwargs["customizations"] = extra_config["customizations"]

                self.logger.info(f"Chart {chart_id} final payload_kwargs: {payload_kwargs}")
                self.logger.info(
                    f"Chart {chart_id} final metrics in payload: {payload_kwargs.get('metrics')}"
                )

                payload = ChartDataPayload(**payload_kwargs)
                self.logger.info(f"Chart {chart_id} ChartDataPayload created successfully")

                # Get limited sample data for AI analysis
                try:
                    self.logger.info(f"Chart {chart_id} calling get_chart_data_table_preview...")
                    preview_data = get_chart_data_table_preview(
                        org_warehouse, payload, page=0, limit=min(self.max_rows, 500)
                    )
                    self.logger.info(
                        f"Chart {chart_id} preview_data keys: {list(preview_data.keys()) if preview_data else 'None'}"
                    )

                    if preview_data:
                        data_rows = preview_data.get("data", [])
                        columns = preview_data.get("columns", [])
                        self.logger.info(
                            f"Chart {chart_id} got {len(data_rows)} rows, {len(columns)} columns"
                        )

                        sample_data = {
                            "rows": data_rows,
                            "total_rows": len(data_rows),
                            "columns": columns,
                        }
                    else:
                        self.logger.warning(f"Chart {chart_id} preview_data is None/empty")
                except Exception as payload_error:
                    # If the configured payload fails, try a simpler raw data approach
                    error_msg = str(payload_error)
                    self.logger.warning(
                        f"Chart {chart_id} payload failed: {error_msg}, trying raw data fallback"
                    )

                    # Special handling for dimension_col errors - be more aggressive
                    if any(
                        keyword in error_msg.lower()
                        for keyword in ["dimension_col", "dimension", "multiple metrics"]
                    ):
                        self.logger.info(
                            f"Chart {chart_id} failed due to dimension/metrics issue, forcing raw data fallback"
                        )

                    try:
                        # Fallback: try getting raw data without complex configurations
                        self.logger.info(f"Chart {chart_id} trying raw data fallback...")
                        simple_payload = ChartDataPayload(
                            chart_type="table",  # Use simple table type
                            computation_type="raw",
                            schema_name=chart.schema_name,
                            table_name=chart.table_name,
                            extra_config={},
                            limit=min(self.max_rows, 100),  # Smaller limit for fallback
                        )
                        self.logger.info(
                            f"Chart {chart_id} fallback payload: {simple_payload.__dict__}"
                        )

                        preview_data = get_chart_data_table_preview(
                            org_warehouse, simple_payload, page=0, limit=min(self.max_rows, 100)
                        )

                        if preview_data:
                            fallback_rows = preview_data.get("data", [])
                            fallback_columns = preview_data.get("columns", [])
                            self.logger.info(
                                f"Chart {chart_id} fallback success: {len(fallback_rows)} rows, {len(fallback_columns)} columns"
                            )

                            sample_data = {
                                "rows": fallback_rows,
                                "total_rows": len(fallback_rows),
                                "columns": fallback_columns,
                                "fallback": True,  # Indicate this is fallback data
                            }
                        else:
                            self.logger.error(f"Chart {chart_id} raw data fallback returned None")
                            raise Exception("Raw data fallback also failed")

                    except Exception:
                        # If even the fallback fails, raise the original error
                        raise payload_error

            except Exception as data_error:
                self.logger.warning(
                    f"Could not fetch sample data for chart {chart_id}: {data_error}"
                )
                self.logger.debug(f"Chart {chart_id} config: {chart.extra_config}")
                self.logger.debug(f"Chart {chart_id} computation_type: {chart.computation_type}")

                # Continue without sample data
                error_msg = str(data_error)
                if "At least one metric is required" in error_msg:
                    error_msg = "Chart configuration missing required metrics for aggregated data"
                elif "column" in error_msg.lower() and "not found" in error_msg.lower():
                    error_msg = "Chart references columns that don't exist in the data source"

                sample_data = {
                    "rows": [],
                    "total_rows": 0,
                    "columns": schema_info.get("schema", {}).get("columns", []),
                    "error": f"Data fetch error: {error_msg}",
                }

            return {"schema": schema_info["schema"], "sample_data": sample_data}
        except Exception as e:
            self.logger.error(f"Error getting chart data for {chart_id}: {e}")
            return {"schema": {}, "sample_data": None}


@router.post("/{dashboard_id}/context")
@has_permission(["can_view_dashboards"])
def get_dashboard_context(request, dashboard_id: int, payload: DashboardContextRequest):
    """
    Get dashboard context for AI analysis.
    This provides the AI with information about the dashboard structure and optionally data.
    """
    try:
        orguser_obj = request.orguser
        if not orguser_obj:
            return JsonResponse({"error": "Unauthorized"}, status=401)

        analyzer = DashboardContextAnalyzer(
            dashboard_id=dashboard_id,
            orguser_obj=orguser_obj,
            include_data=payload.include_data,
            max_rows=payload.max_rows,
        )

        context = analyzer.get_dashboard_context()

        if "error" in context:
            return JsonResponse(context, status=400)

        return JsonResponse(
            {"dashboard_id": dashboard_id, "context": context, "timestamp": time.time()}
        )

    except Exception as e:
        logger.error(f"Error getting dashboard context: {e}")
        return JsonResponse({"error": "Internal server error"}, status=500)


@router.post("/{dashboard_id}/chat")
@has_permission(["can_view_dashboards"])
def dashboard_chat(request, dashboard_id: int, payload: DashboardChatRequest):
    """
    Chat with AI about a specific dashboard.
    Provides context-aware responses based on dashboard structure and data.
    """
    try:
        orguser_obj = request.orguser
        if not orguser_obj:
            return JsonResponse({"error": "Unauthorized"}, status=401)

        # Get AI provider
        provider = get_default_ai_provider()

        # Get dashboard context
        analyzer = DashboardContextAnalyzer(
            dashboard_id=dashboard_id,
            orguser_obj=orguser_obj,
            include_data=payload.include_data,
            max_rows=payload.max_rows,
        )

        context = analyzer.get_dashboard_context()
        if "error" in context:
            return JsonResponse(context, status=400)

        # Build AI messages with context
        ai_messages = []

        # Add system message with dashboard context
        system_prompt = _build_dashboard_system_prompt(context, payload.selected_chart_id)
        ai_messages.append(AIMessage(role="system", content=system_prompt))

        # Add conversation history
        for msg in payload.messages:
            ai_messages.append(AIMessage(role=msg.role, content=msg.content, metadata=msg.metadata))

        # Handle streaming vs non-streaming
        if payload.stream:
            return StreamingHttpResponse(
                _stream_dashboard_chat(provider, ai_messages, context),
                content_type="text/event-stream",
            )
        else:
            # Generate AI response
            response = provider.chat_completion(
                messages=ai_messages, temperature=0.7, max_tokens=2000
            )

            return JsonResponse(
                {
                    "content": response.content,
                    "usage": response.usage,
                    "context_included": True,
                    "data_included": payload.include_data,
                    "dashboard_id": dashboard_id,
                    "metadata": {
                        "charts_analyzed": len(context.get("charts", [])),
                        "filters_available": len(context.get("filters", [])),
                        "selected_chart": payload.selected_chart_id,
                        "provider": response.provider,
                    },
                }
            )

    except Exception as e:
        logger.error(f"Error in dashboard chat: {e}")
        return JsonResponse({"error": "Internal server error"}, status=500)


async def _stream_dashboard_chat(provider, ai_messages, context):
    """Generate streaming chat response with dashboard context."""
    try:
        async for chunk in provider.stream_chat_completion(ai_messages):
            data = {
                "content": chunk.content,
                "is_complete": chunk.is_complete,
                "context_included": True,
                "charts_analyzed": len(context.get("charts", [])),
                "timestamp": asyncio.get_event_loop().time(),
            }

            if chunk.usage:
                data["usage"] = chunk.usage

            yield f"data: {json.dumps(data)}\n\n"

            if chunk.is_complete:
                yield "data: [DONE]\n\n"
                break

    except Exception as e:
        error_data = {"error": {"message": str(e), "type": type(e).__name__}}
        yield f"data: {json.dumps(error_data)}\n\n"


def _build_dashboard_system_prompt(
    context: Dict[str, Any], selected_chart_id: Optional[str] = None
) -> str:
    """
    Build system prompt with dashboard context for AI.
    """
    dashboard = context.get("dashboard", {})
    charts = context.get("charts", [])
    filters = context.get("filters", [])
    summary = context.get("summary", {})

    prompt_parts = [
        "You are an AI assistant helping users analyze their dashboard data.",
        f"Current Dashboard: '{dashboard.get('title', 'Untitled Dashboard')}'",
        f"Description: {dashboard.get('description', 'No description available')}",
    ]

    if dashboard.get("dashboard_type"):
        prompt_parts.append(f"Dashboard Type: {dashboard['dashboard_type']}")

    # Add chart information
    if charts:
        prompt_parts.append(f"\nDashboard contains {len(charts)} charts:")
        for chart in charts[:10]:  # Limit to first 10 charts
            chart_info = (
                f"- {chart.get('title', 'Untitled Chart')} (Type: {chart.get('type', 'unknown')})"
            )
            if chart.get("schema", {}).get("columns"):
                chart_info += f" - Columns: {', '.join(chart['schema']['columns'][:5])}"

            # Add sample data info if available
            sample_data = chart.get("sample_data")
            if sample_data:
                if sample_data.get("rows"):
                    if sample_data.get("fallback"):
                        chart_info += (
                            f" - Sample data: {len(sample_data['rows'])} rows (raw table data)"
                        )
                    else:
                        chart_info += f" - Sample data: {len(sample_data['rows'])} rows available"
                elif sample_data.get("error"):
                    chart_info += f" - Data fetch error: {sample_data['error']}"

            prompt_parts.append(chart_info)

        if len(charts) > 10:
            prompt_parts.append(f"... and {len(charts) - 10} more charts")

    # Add filter information
    if filters:
        prompt_parts.append(f"\nAvailable filters ({len(filters)}):")
        for filter_obj in filters[:5]:
            prompt_parts.append(
                f"- {filter_obj.get('name', 'Unnamed Filter')} ({filter_obj.get('filter_type', 'unknown')})"
            )

    # Add data availability info with detailed status
    data_info = "\nData Access: "
    if summary.get("data_included"):
        data_info += (
            f"User has enabled data sharing (up to {summary.get('max_rows', 0)} rows per chart)"
        )

        # Count charts with actual data
        charts_with_data = sum(1 for chart in charts if chart.get("sample_data", {}).get("rows"))
        charts_with_errors = sum(1 for chart in charts if chart.get("sample_data", {}).get("error"))
        charts_with_fallback = sum(
            1 for chart in charts if chart.get("sample_data", {}).get("fallback")
        )

        if charts_with_data > 0:
            data_info += f"\n  - {charts_with_data} charts have sample data available"
            if charts_with_fallback > 0:
                data_info += f"\n  - {charts_with_fallback} charts using raw table data (chart config issues)"
        if charts_with_errors > 0:
            data_info += f"\n  - {charts_with_errors} charts had data fetch errors"
    else:
        data_info += "Only schema information is available (user has not enabled data sharing)"

    prompt_parts.append(data_info)

    # Add specific chart context if selected
    if selected_chart_id:
        selected_chart = next((c for c in charts if c.get("id") == selected_chart_id), None)
        if selected_chart:
            prompt_parts.append(
                f"\nUser is focusing on chart: '{selected_chart.get('title', 'Untitled Chart')}'"
            )
            if selected_chart.get("schema"):
                schema = selected_chart["schema"]
                if schema.get("columns"):
                    prompt_parts.append(f"Chart columns: {', '.join(schema['columns'])}")
                if schema.get("metrics"):
                    prompt_parts.append(f"Chart metrics: {', '.join(schema['metrics'])}")

    # Add specific instructions based on data availability
    if summary.get("data_included"):
        charts_with_data = sum(1 for chart in charts if chart.get("sample_data", {}).get("rows"))
        charts_with_errors = sum(1 for chart in charts if chart.get("sample_data", {}).get("error"))
        charts_with_fallback = sum(
            1 for chart in charts if chart.get("sample_data", {}).get("fallback")
        )

        if charts_with_data > 0:
            prompt_parts.extend(
                [
                    "\nYou can help users:",
                    "- Analyze actual data values and trends from the sample data provided",
                    "- Identify patterns, outliers, and insights in the real data",
                    "- Provide specific statistics and data-driven observations",
                    "- Suggest data-driven insights based on the chart configurations and actual values",
                    "- Explain what the actual data shows about business metrics",
                    "- Recommend filters or views based on data patterns",
                ]
            )

            if charts_with_fallback > 0:
                prompt_parts.append("- Some charts show raw table data due to configuration issues")

            if charts_with_errors > 0:
                prompt_parts.append(
                    "- Some charts had data fetch errors - explain based on schema/structure"
                )

            prompt_parts.extend(
                [
                    "",
                    "IMPORTANT: You have access to actual sample data for analysis. Use the real data values to provide specific, data-driven insights.",
                ]
            )
        else:
            prompt_parts.extend(
                [
                    "\nYou can help users:",
                    "- Understand the dashboard structure and chart configurations",
                    "- Explain what each chart is designed to show based on schema",
                    "- Suggest insights based on the chart configurations and column types",
                    "- Explain how to interpret visualizations",
                    "- Recommend filters or views that might be helpful",
                    "- Identify potential configuration issues that prevented data loading",
                    "",
                    "Note: Data sharing is enabled but there were errors fetching the actual data. Focus on schema and configuration analysis. Mention that charts may need configuration fixes.",
                ]
            )
    else:
        prompt_parts.extend(
            [
                "\nYou can help users:",
                "- Understand what data is shown in the dashboard based on schema",
                "- Explain the structure and purpose of each chart",
                "- Suggest insights based on the chart configurations and column types",
                "- Explain how to interpret visualizations",
                "- Recommend filters or views that might be helpful",
                "",
                "IMPORTANT: You only have schema information, not actual data values. If the user wants data-driven insights, let them know they can enable data sharing in the chat settings to get access to sample data.",
            ]
        )

    return "\n".join(prompt_parts)


@router.post("/{dashboard_id}/chat-settings")
@has_permission(["can_view_dashboards"])
def update_chat_settings(request, dashboard_id: int):
    """
    Update chat settings for a dashboard session.
    """
    try:
        data = json.loads(request.body)

        # Settings could be stored in user preferences or session
        settings = {
            "include_data": data.get("include_data", False),
            "max_rows": data.get("max_rows", 100),
            "provider_type": data.get("provider_type"),
            "auto_context": data.get("auto_context", True),
        }

        # For now, just validate and return the settings
        # In a full implementation, you might store these in user preferences

        return JsonResponse(
            {"dashboard_id": dashboard_id, "settings": settings, "updated_at": time.time()}
        )

    except Exception as e:
        logger.error(f"Error updating chat settings: {e}")
        return JsonResponse({"error": "Internal server error"}, status=500)


@router.post("/{dashboard_id}/debug")
@has_permission(["can_view_dashboards"])
def debug_dashboard_context(request, dashboard_id: int, payload: DashboardContextRequest):
    """
    Debug endpoint to see detailed context information for troubleshooting.
    Returns the full context with all debug information.
    """
    try:
        orguser_obj = request.orguser
        if not orguser_obj:
            return JsonResponse({"error": "Unauthorized"}, status=401)

        analyzer = DashboardContextAnalyzer(
            dashboard_id=dashboard_id,
            orguser_obj=orguser_obj,
            include_data=payload.include_data,
            max_rows=payload.max_rows,
        )

        # Get the context with detailed logging
        context = analyzer.get_dashboard_context()

        # Add extra debug info
        debug_info = {
            "dashboard_id": dashboard_id,
            "user_org": orguser_obj.org.slug,
            "include_data": payload.include_data,
            "max_rows": payload.max_rows,
            "context": context,
            "timestamp": time.time(),
        }

        # Add warehouse info for debugging
        from ddpui.models.org import OrgWarehouse

        org_warehouse = OrgWarehouse.objects.filter(org=orguser_obj.org).first()
        if org_warehouse:
            debug_info["warehouse"] = {
                "name": org_warehouse.name,
                "type": org_warehouse.wtype,
                "credentials_available": bool(org_warehouse.credentials),
            }
        else:
            debug_info["warehouse"] = None

        return JsonResponse(debug_info)

    except Exception as e:
        logger.error(f"Error in debug dashboard context: {e}")
        return JsonResponse(
            {"error": f"Debug error: {str(e)}", "dashboard_id": dashboard_id}, status=500
        )


@router.get("/{dashboard_id}/test-chart/{chart_id}")
@has_permission(["can_view_dashboards"])
def test_single_chart(request, dashboard_id: int, chart_id: int):
    """
    Test endpoint to debug a single chart's data fetching.
    """
    try:
        orguser_obj = request.orguser
        if not orguser_obj:
            return JsonResponse({"error": "Unauthorized"}, status=401)

        analyzer = DashboardContextAnalyzer(
            dashboard_id=dashboard_id,
            orguser_obj=orguser_obj,
            include_data=True,  # Always include data for testing
            max_rows=10,  # Small sample for testing
        )

        # Test just this chart
        result = analyzer._get_chart_data_context(chart_id)

        return JsonResponse(
            {
                "chart_id": chart_id,
                "dashboard_id": dashboard_id,
                "result": result,
                "timestamp": time.time(),
            }
        )

    except Exception as e:
        logger.error(f"Error testing chart {chart_id}: {e}")
        return JsonResponse({"error": f"Test error: {str(e)}", "chart_id": chart_id}, status=500)


@router.get("/{dashboard_id}/debug-chart-config/{chart_id}")
@has_permission(["can_view_dashboards"])
def debug_chart_config(request, dashboard_id: int, chart_id: int):
    """
    Debug endpoint to see chart configuration details.
    """
    try:
        orguser_obj = request.orguser
        if not orguser_obj:
            return JsonResponse({"error": "Unauthorized"}, status=401)

        from ddpui.models.visualization import Chart

        chart = Chart.objects.filter(id=chart_id, org=orguser_obj.org).first()
        if not chart:
            return JsonResponse({"error": "Chart not found"}, status=404)

        extra_config = chart.extra_config or {}

        debug_info = {
            "chart_id": chart_id,
            "dashboard_id": dashboard_id,
            "chart_details": {
                "title": chart.title,
                "chart_type": chart.chart_type,
                "computation_type": chart.computation_type,
                "schema_name": chart.schema_name,
                "table_name": chart.table_name,
            },
            "extra_config": extra_config,
            "config_analysis": {
                "has_metrics": bool(extra_config.get("metrics")),
                "has_aggregate_functions": bool(extra_config.get("aggregate_functions")),
                "has_dimension_col": bool(extra_config.get("dimension_col")),
                "has_columns": bool(extra_config.get("columns")),
                "metrics_count": len(extra_config.get("metrics", [])),
                "columns_count": len(extra_config.get("columns", [])),
            },
            "recommended_action": "unknown",
        }

        # Determine recommended action
        if chart.computation_type == "aggregated":
            if not extra_config.get("metrics") and not extra_config.get("aggregate_functions"):
                debug_info["recommended_action"] = "force_raw_mode_no_metrics"
            elif chart.chart_type.lower() in [
                "table",
                "pie",
                "doughnut",
                "map",
                "choropleth",
            ] and not extra_config.get("dimension_col"):
                if not extra_config.get("columns"):
                    debug_info["recommended_action"] = "force_raw_mode_no_dimension_data"
                else:
                    debug_info["recommended_action"] = "infer_dimension_col"
            else:
                debug_info["recommended_action"] = "use_as_configured"
        else:
            debug_info["recommended_action"] = "use_raw_mode"

        return JsonResponse(debug_info)

    except Exception as e:
        logger.error(f"Error debugging chart config {chart_id}: {e}")
        return JsonResponse({"error": f"Debug error: {str(e)}", "chart_id": chart_id}, status=500)

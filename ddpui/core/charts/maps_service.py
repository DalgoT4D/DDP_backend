"""Maps service module for handling map business logic"""

from typing import Optional, List, Dict, Any
from django.db import models
from ddpui.models.org import OrgWarehouse
from ddpui.models.georegion import GeoRegion
from ddpui.models.geojson import GeoJSON
from ddpui.core.charts import charts_service
from ddpui.schemas.chart_schema import ChartDataPayload
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui.maps")


def normalize_region_name(name):
    """Normalize region name for consistent matching"""
    if not name:
        return ""
    return str(name).strip().lower()


def get_available_regions(country_code: str, region_type: str = None) -> List[Dict]:
    """Get available regions for a country"""
    query = GeoRegion.objects.filter(country_code=country_code)
    if region_type:
        query = query.filter(type=region_type)

    regions = query.order_by("display_name")

    return [
        {
            "id": region.id,
            "name": region.name.strip().title(),  # Normalize to match map data
            "display_name": region.display_name.strip().title(),  # Normalize to match map data
            "type": region.type,
            "parent_id": region.parent_id,
            "country_code": region.country_code,
            "region_code": region.region_code,
        }
        for region in regions
    ]


def get_available_geojsons_for_region(region_id: int, org_id: int) -> List[Dict]:
    """Get available GeoJSONs for a specific region

    Returns both default GeoJSONs and org-specific custom GeoJSONs
    org_id is required as it comes from authenticated token
    """
    # Get both default GeoJSONs AND org-specific GeoJSONs for this region
    query = GeoJSON.objects.filter(region_id=region_id).filter(
        models.Q(is_default=True) | models.Q(org_id=org_id)
    )

    geojsons = query.order_by("-is_default", "name")

    return [
        {
            "id": g.id,
            "name": g.name,
            "display_name": f"{g.name} ({g.description or 'No description'})",
            "is_default": g.is_default,
            "layer_name": g.name,  # Using name as layer_name for now
            "properties_key": g.properties_key,
        }
        for g in geojsons
    ]


def get_child_regions(parent_region_id: int) -> List[Dict]:
    """Get child regions for a parent region"""
    children = GeoRegion.objects.filter(parent_id=parent_region_id).order_by("display_name")

    return [
        {
            "id": child.id,
            "name": child.name.strip().title(),  # Normalize to match map data
            "display_name": child.display_name.strip().title(),  # Normalize to match map data
            "type": child.type,
            "region_code": child.region_code,
        }
        for child in children
    ]


def build_map_query(payload: ChartDataPayload, drill_down_filters: List[Dict] = None):
    """Build query for map data with optional drill-down filters"""
    # Maps always use aggregated data grouped by geographic column

    # Create a new payload that mimics aggregated bar chart structure
    if payload.metrics:
        # Use new multiple metrics system
        map_payload = ChartDataPayload(
            chart_type="bar",  # Reuse bar chart query logic internally
            computation_type="aggregated",
            schema_name=payload.schema_name,
            table_name=payload.table_name,
            dimension_col=payload.geographic_column,
            metrics=payload.metrics,  # Use metrics array
            dashboard_filters=payload.dashboard_filters,  # Pass through dashboard filters
            extra_config=payload.extra_config,  # Pass through filters, pagination, sorting, and other config
        )
    else:
        # Fallback to legacy single metric system for backward compatibility
        map_payload = ChartDataPayload(
            chart_type="bar",  # Reuse bar chart query logic internally
            computation_type="aggregated",
            schema_name=payload.schema_name,
            table_name=payload.table_name,
            dimension_col=payload.geographic_column,
            metrics=payload.metrics,
            dashboard_filters=payload.dashboard_filters,  # Pass through dashboard filters
            extra_config=payload.extra_config,  # Pass through filters, pagination, sorting, and other config
        )

    # Build base query using the same service as regular charts (includes filtering, pagination, sorting)
    query = charts_service.build_chart_query(map_payload)

    # Add drill-down WHERE filters if provided
    if drill_down_filters:
        from sqlalchemy import and_

        conditions = []

        for filter_item in drill_down_filters:
            column_name = filter_item.get("column")
            column_value = filter_item.get("value")
            if column_name and column_value:
                # Add WHERE condition for this filter
                conditions.append(query.columns[column_name] == column_value)

        if conditions:
            query = query.where(and_(*conditions))

    return query


def build_drill_down_query_for_layer(
    payload: ChartDataPayload, layer_config: Dict, parent_selections: List[Dict] = None
):
    """Build query for a specific layer in drill-down hierarchy"""

    # Extract layer configuration
    geographic_column = layer_config.get("geographic_column")

    # Create drill-down filters from parent selections
    drill_down_filters = []
    if parent_selections:
        for selection in parent_selections:
            drill_down_filters.append(
                {"column": selection.get("column"), "value": selection.get("value")}
            )

    # Create modified payload for this layer
    layer_payload = ChartDataPayload(
        chart_type=payload.chart_type,
        computation_type=payload.computation_type,
        schema_name=payload.schema_name,
        table_name=payload.table_name,
        geographic_column=geographic_column,
        value_column=payload.value_column,
        metrics=payload.metrics,  # Pass through metrics for multiple metric support
        dashboard_filters=payload.dashboard_filters,  # Pass through dashboard filters
        extra_config=payload.extra_config,  # Pass through filters, pagination, sorting, and other config
    )

    return build_map_query(layer_payload, drill_down_filters)


def transform_data_for_map(
    results: List[Dict[str, Any]],
    geojson_data: Dict,
    geographic_column: str,
    value_column: str = None,
    customizations: Dict = None,
    metrics: List = None,
    selected_metric_index: int = 0,
) -> Dict[str, Any]:
    """Transform query results to map-specific format

    Args:
        results: Query results from database
        geojson_data: GeoJSON data for map visualization
        geographic_column: Column containing region names
        value_column: Legacy single value column (for backward compatibility)
        customizations: Chart customizations
        metrics: List of metrics (new multiple metrics system)
        selected_metric_index: Which metric to display (default: first metric)
    """

    customizations = customizations or {}

    # Handle multiple metrics system vs legacy single metric
    if metrics and len(metrics) > 0:
        # Use new multiple metrics system
        selected_metric = (
            metrics[selected_metric_index] if selected_metric_index < len(metrics) else metrics[0]
        )

        # Generate the column alias for the selected metric
        if (
            selected_metric.get("aggregation", "").lower() == "count"
            and selected_metric.get("column") is None
        ):
            if selected_metric.get("alias"):
                agg_col_name = f"count_all_{selected_metric['alias']}"
            else:
                agg_col_name = "count_all"
        else:
            if selected_metric.get("alias"):
                agg_col_name = selected_metric["alias"]
            else:
                agg_col_name = f"{selected_metric['aggregation']}_{selected_metric['column']}"

    # Create normalized lookup for user data
    data_lookup_normalized = {}
    original_name_mapping = {}  # Keep track of original names

    for row in results:
        region_name = row.get(geographic_column)
        value = row.get(agg_col_name, 0)
        if region_name:
            normalized_key = normalize_region_name(region_name)
            data_lookup_normalized[normalized_key] = value
            original_name_mapping[normalized_key] = str(region_name).strip()

    logger.info(f"Data lookup created with {len(data_lookup_normalized)} regions")
    logger.info(f"All normalized data keys: {list(data_lookup_normalized.keys())}")
    logger.info(f"All data values: {list(data_lookup_normalized.values())}")

    # Prepare data for ECharts map
    map_data = []
    matched_count = 0

    for feature in geojson_data.get("features", []):
        properties = feature.get("properties", {})
        geojson_region_name = properties.get("name", "")

        # Normalize the GeoJSON region name for matching
        normalized_geojson_name = normalize_region_name(geojson_region_name)

        # Look up value using normalized key
        value = data_lookup_normalized.get(normalized_geojson_name, 0)

        # Debug every region
        logger.info(
            f"Processing: '{geojson_region_name}' → normalized: '{normalized_geojson_name}' → value: {value}"
        )

        if value > 0:
            matched_count += 1

        # Use the original GeoJSON name for ECharts compatibility
        map_data.append({"name": geojson_region_name, "value": value})

    logger.info(f"Matched {matched_count} out of {len(map_data)} regions")

    values = [item["value"] for item in map_data if item["value"] > 0]
    min_value = min(values) if values else 0
    max_value = max(values) if values else 100

    # Prepare available metrics info for frontend
    available_metrics = []
    if metrics and len(metrics) > 0:
        for i, metric in enumerate(metrics):
            if metric.get("aggregation", "").lower() == "count" and metric.get("column") is None:
                display_name = metric.get("alias") or "Total Count"
            else:
                display_name = metric.get("alias") or f"{metric['aggregation']}({metric['column']})"

            available_metrics.append(
                {
                    "index": i,
                    "display_name": display_name,
                    "column": metric.get("column"),
                    "aggregation": metric.get("aggregation"),
                    "alias": metric.get("alias"),
                    "is_selected": i == selected_metric_index,
                }
            )

    return {
        "geojson": geojson_data,
        "data": map_data,
        "min_value": min_value,
        "max_value": max_value,
        "matched_regions": matched_count,
        "total_regions": len(map_data),
        "available_metrics": available_metrics,
        "selected_metric_index": selected_metric_index,
    }


def standardize_geojson_properties(geojson_data: dict, properties_key: str) -> dict:
    """Standardize GeoJSON properties to use 'name' key"""

    for feature in geojson_data.get("features", []):
        if "properties" in feature:
            # Copy the specified property to 'name' (keep original case for ECharts)
            if properties_key in feature["properties"]:
                feature["properties"]["name"] = feature["properties"][properties_key]

    return geojson_data

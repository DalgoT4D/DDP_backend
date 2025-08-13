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


def get_available_regions(country_code: str, region_type: str = None) -> List[Dict]:
    """Get available regions for a country"""
    query = GeoRegion.objects.filter(country_code=country_code)
    if region_type:
        query = query.filter(type=region_type)

    regions = query.order_by("display_name")

    return [
        {
            "id": region.id,
            "name": region.name,
            "display_name": region.display_name,
            "type": region.type,
            "parent_id": region.parent_id,
            "country_code": region.country_code,
            "region_code": region.region_code,
        }
        for region in regions
    ]


def get_available_geojsons_for_region(region_id: int, org_id: int = None) -> List[Dict]:
    """Get available GeoJSONs for a specific region"""
    # Get default GeoJSONs for this region
    query = GeoJSON.objects.filter(region_id=region_id)

    # Include org-specific GeoJSONs if org_id provided
    if org_id:
        query = query.filter(models.Q(is_default=True) | models.Q(org_id=org_id))
    else:
        query = query.filter(is_default=True)

    geojsons = query.order_by("-is_default", "version_name")

    return [
        {
            "id": g.id,
            "region_id": g.region_id,
            "version_name": g.version_name,
            "description": g.description,
            "is_default": g.is_default,
            "properties_key": g.properties_key,
            "file_size": g.file_size,
        }
        for g in geojsons
    ]


def get_child_regions(parent_region_id: int) -> List[Dict]:
    """Get child regions for a parent region"""
    children = GeoRegion.objects.filter(parent_id=parent_region_id).order_by("display_name")

    return [
        {
            "id": child.id,
            "name": child.name,
            "display_name": child.display_name,
            "type": child.type,
            "region_code": child.region_code,
        }
        for child in children
    ]


def build_map_query(payload: ChartDataPayload, drill_down_filters: List[Dict] = None):
    """Build query for map data with optional drill-down filters"""
    # Maps always use aggregated data grouped by geographic column

    # Create a new payload that mimics aggregated bar chart structure
    map_payload = ChartDataPayload(
        chart_type="bar",  # Reuse bar chart query logic internally
        computation_type="aggregated",
        schema_name=payload.schema_name,
        table_name=payload.table_name,
        dimension_col=payload.geographic_column,
        aggregate_col=payload.value_column,
        aggregate_func=payload.aggregate_func or "sum",
        limit=payload.limit or 1000,
        offset=payload.offset or 0,
    )

    # Build base query
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
        aggregate_func=payload.aggregate_func,
        limit=payload.limit,
        offset=payload.offset,
    )

    return build_map_query(layer_payload, drill_down_filters)


def transform_data_for_map(
    results: List[Dict[str, Any]],
    geojson_data: Dict,
    geographic_column: str,
    value_column: str,
    aggregate_func: str = "sum",
    customizations: Dict = None,
) -> Dict[str, Any]:
    """Transform query results to map-specific format"""

    customizations = customizations or {}

    # Get aggregate column name (consistent with charts_service)
    agg_col_name = f"{aggregate_func}_{value_column}" if value_column else "count_all"

    # Create lookup for user data
    data_lookup = {}
    for row in results:
        region_name = row.get(geographic_column)
        value = row.get(agg_col_name, 0)
        if region_name:
            data_lookup[str(region_name).strip()] = value

    logger.info(f"Data lookup created with {len(data_lookup)} regions")

    # Prepare data for ECharts map
    map_data = []
    matched_count = 0

    for feature in geojson_data.get("features", []):
        properties = feature.get("properties", {})
        region_name = properties.get("name", "")

        # Try exact match first, then case-insensitive
        value = data_lookup.get(region_name)
        if value is None:
            # Try case-insensitive match
            for data_region, data_value in data_lookup.items():
                if data_region.lower() == region_name.lower():
                    value = data_value
                    matched_count += 1
                    break
        else:
            matched_count += 1

        if value is None:
            value = 0

        map_data.append({"name": region_name, "value": value})

    logger.info(f"Matched {matched_count} out of {len(map_data)} regions")

    values = [item["value"] for item in map_data if item["value"] > 0]
    min_value = min(values) if values else 0
    max_value = max(values) if values else 100

    return {
        "geojson": geojson_data,
        "data": map_data,
        "min_value": min_value,
        "max_value": max_value,
        "matched_regions": matched_count,
        "total_regions": len(map_data),
    }


def standardize_geojson_properties(geojson_data: dict, properties_key: str) -> dict:
    """Standardize GeoJSON properties to use 'name' key"""

    for feature in geojson_data.get("features", []):
        if "properties" in feature:
            # Copy the specified property to 'name'
            if properties_key in feature["properties"]:
                feature["properties"]["name"] = feature["properties"][properties_key]

    return geojson_data

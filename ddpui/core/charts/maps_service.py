"""Maps service module for handling map business logic"""

from typing import Optional, List, Dict, Any
from ddpui.models.org import OrgWarehouse
from ddpui.models.map_layer import MapLayer
from ddpui.models.geojson import GeoJSON
from ddpui.core.charts import charts_service
from ddpui.schemas.chart_schema import ChartDataPayload
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui.maps")


def get_available_geojsons(country_code: str, layer_level: int, org_id: int) -> List[Dict]:
    """Get available GeoJSONs for a country and layer level"""
    layers = MapLayer.objects.filter(country_code=country_code, layer_level=layer_level)

    geojsons = []
    for layer in layers:
        # Get default GeoJSONs
        default_geojsons = GeoJSON.objects.filter(layer_id=layer.id, is_default=True)

        # Get org-specific GeoJSONs
        org_geojsons = GeoJSON.objects.filter(layer_id=layer.id, org_id=org_id, is_default=False)

        geojsons.extend(
            [
                {
                    "id": g.id,
                    "name": g.name,
                    "display_name": g.display_name,
                    "is_default": g.is_default,
                    "layer_name": layer.layer_name,
                    "properties_key": g.properties_key,
                }
                for g in list(default_geojsons) + list(org_geojsons)
            ]
        )

    return geojsons


def build_map_query(payload: ChartDataPayload):
    """Build query for map data - reuses existing chart query builder"""
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

    # Reuse existing query builder
    return charts_service.build_chart_query(map_payload)


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

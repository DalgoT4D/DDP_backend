"""GeoJSON schemas used by the map chart_type."""

from typing import Optional

from ninja import Schema


class GeoJSONListResponse(Schema):
    """Schema for GeoJSON list response."""

    id: int
    name: str
    display_name: str
    is_default: bool
    layer_name: str
    properties_key: str


class GeoJSONDetailResponse(Schema):
    """Schema for GeoJSON detail response."""

    id: int
    name: str
    display_name: str
    geojson_data: dict
    properties_key: str


class GeoJSONUpload(Schema):
    """Schema for uploading custom GeoJSON."""

    region_id: int
    name: str
    description: Optional[str] = None
    properties_key: str
    geojson_data: dict

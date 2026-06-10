"""Schema Tests for Chart schemas

Tests schema-specific functionality NOT tested by API tests:
1. ChartMetric nested schema validation
2. ChartDataPayload specific fields (map, metrics, pagination)
3. GeoJSON schemas
4. Schema serialization (.dict())
5. Edge cases (unicode, type coercion)
"""

import pytest
from datetime import datetime
from pydantic import ValidationError
from unittest.mock import MagicMock

from ddpui.schemas.chart_schemas import (
    ChartMetric,
    ChartCreate,
    ChartUpdate,
    ChartResponse,
    ChartDataPayload,
    ChartDataResponse,
    GeoJSONListResponse,
    GeoJSONUpload,
)


# ================================================================================
# Test ChartMetric Schema (nested schema - NOT tested by API tests)
# ================================================================================


class TestChartMetricSchema:
    """Tests for ChartMetric nested schema"""

    def test_chart_metric_valid_with_column(self):
        """Test valid metric with column name"""
        metric = ChartMetric(column="revenue", aggregation="sum", alias="Total Revenue")

        assert metric.column == "revenue"
        assert metric.aggregation == "sum"
        assert metric.alias == "Total Revenue"

    def test_chart_metric_valid_without_column(self):
        """Test valid metric without column (for COUNT(*))"""
        metric = ChartMetric(column=None, aggregation="count", alias="Row Count")

        assert metric.column is None
        assert metric.aggregation == "count"

    def test_chart_metric_missing_aggregation_rejected(self):
        """A metric with no aggregation and no column_expression is invalid."""
        with pytest.raises(ValidationError):
            ChartMetric(column="revenue")

    def test_chart_metric_sum_without_column_rejected(self):
        """Non-count aggregations need a column (count can stand alone for COUNT(*))."""
        with pytest.raises(ValidationError):
            ChartMetric(aggregation="sum")

    def test_chart_metric_column_expression_bypasses_rules(self):
        """An expression metric is valid without aggregation/column."""
        metric = ChartMetric(column_expression="SUM(a) / COUNT(DISTINCT id)")
        assert metric.column_expression.startswith("SUM(a)")

    def test_chart_metric_all_aggregation_types(self):
        """All six valid aggregations accepted."""
        aggregations = ["sum", "avg", "count", "min", "max", "count_distinct"]

        for agg in aggregations:
            metric = ChartMetric(column="value", aggregation=agg)
            assert metric.aggregation == agg

    def test_chart_metric_unknown_aggregation_rejected(self):
        with pytest.raises(ValidationError):
            ChartMetric(column="value", aggregation="median")


# ================================================================================
# Test ChartDataPayload specific fields (NOT fully tested by API tests)
# ================================================================================


class TestChartDataPayloadSchema:
    """Tests for ChartDataPayload specific fields"""

    def test_payload_default_pagination(self):
        """Test default pagination values"""
        payload = ChartDataPayload(
            chart_type="bar",
            schema_name="public",
            table_name="users",
        )

        assert payload.offset == 0
        assert payload.limit == 100

    def test_payload_custom_pagination(self):
        """Test custom pagination values"""
        payload = ChartDataPayload(
            chart_type="bar",
            schema_name="public",
            table_name="users",
            offset=50,
            limit=25,
        )

        assert payload.offset == 50
        assert payload.limit == 25

    def test_payload_with_map_fields(self):
        """Test payload with map-specific fields"""
        payload = ChartDataPayload(
            chart_type="map",
            schema_name="public",
            table_name="regional_sales",
            geographic_column="region",
            value_column="sales",
            selected_geojson_id=1,
        )

        assert payload.geographic_column == "region"
        assert payload.value_column == "sales"
        assert payload.selected_geojson_id == 1

    def test_payload_with_metrics(self):
        """Test payload with metrics array"""
        payload = ChartDataPayload(
            chart_type="bar",
            schema_name="public",
            table_name="sales",
            dimension_col="category",
            metrics=[ChartMetric(column="revenue", aggregation="sum")],
        )

        assert payload.dimension_col == "category"
        assert len(payload.metrics) == 1
        assert payload.metrics[0].aggregation == "sum"

    def test_payload_with_dashboard_filters(self):
        """Test payload with dashboard filters"""
        payload = ChartDataPayload(
            chart_type="bar",
            schema_name="public",
            table_name="users",
            dashboard_filters=[{"column": "status", "value": "active"}],
        )

        assert len(payload.dashboard_filters) == 1


# ================================================================================
# Test GeoJSON Schemas (NOT tested by API tests)
# ================================================================================


class TestGeoJSONSchemas:
    """Tests for GeoJSON related schemas"""

    def test_geojson_upload_valid(self):
        """Test valid GeoJSON upload"""
        upload = GeoJSONUpload(
            region_id=1,
            name="custom_regions",
            description="Custom regions for our org",
            properties_key="region_name",
            geojson_data={"type": "FeatureCollection", "features": []},
        )

        assert upload.region_id == 1
        assert upload.name == "custom_regions"
        assert upload.description == "Custom regions for our org"

    def test_geojson_upload_without_description(self):
        """Test GeoJSON upload without optional description"""
        upload = GeoJSONUpload(
            region_id=1,
            name="test",
            properties_key="name",
            geojson_data={},
        )

        assert upload.description is None

    def test_geojson_list_response(self):
        """Test GeoJSON list response"""
        response = GeoJSONListResponse(
            id=1,
            name="india_states",
            display_name="India States",
            is_default=True,
            layer_name="states",
            properties_key="name",
        )

        assert response.id == 1
        assert response.is_default is True


# ================================================================================
# Test Schema Serialization (NOT tested by API tests)
# ================================================================================


class TestSchemaSerialization:
    """Tests for schema serialization (.dict())"""

    def test_chart_create_to_dict(self):
        """Test ChartCreate can be converted to dict (extra_config is now typed per chart_type)."""
        chart = ChartCreate(
            title="Test",
            chart_type="bar",
            schema_name="public",
            table_name="users",
            extra_config={
                "dimension_column": "country",
                "metrics": [{"column": "revenue", "aggregation": "sum"}],
            },
        )

        data = chart.model_dump()

        assert isinstance(data, dict)
        assert data["title"] == "Test"
        # extra_config still serializes to a plain dict downstream
        assert data["extra_config"]["dimension_column"] == "country"
        assert data["extra_config"]["metrics"][0]["aggregation"] == "sum"

    def test_chart_update_to_dict_excludes_none(self):
        """Test ChartUpdate dict excludes None values when specified"""
        update = ChartUpdate(title="New Title")

        data = update.model_dump(exclude_none=True)

        assert "title" in data
        assert "description" not in data
        assert "chart_type" not in data

    def test_chart_data_response_to_dict(self):
        """Test ChartDataResponse can be converted to dict"""
        response = ChartDataResponse(
            data={"labels": ["A", "B"], "values": [10, 20]},
            echarts_config={"type": "bar"},
        )

        data = response.model_dump()

        assert data["data"]["labels"] == ["A", "B"]
        assert data["echarts_config"]["type"] == "bar"


# ================================================================================
# Test Edge Cases (NOT tested by API tests)
# ================================================================================


class TestEdgeCases:
    """Tests for edge cases and special values"""

    def test_chart_create_with_unicode_title(self):
        """Test chart creation with unicode characters in the title."""
        chart = ChartCreate(
            title="日本語チャート 📊",
            chart_type="bar",
            schema_name="public",
            table_name="users",
            extra_config={
                "dimension_column": "country",
                "metrics": [{"column": "revenue", "aggregation": "sum"}],
            },
        )

        assert chart.title == "日本語チャート 📊"

    def test_extra_config_with_nested_customizations(self):
        """customizations is an extra (allowed) field — deeply nested values pass through."""
        chart = ChartCreate(
            title="Nested",
            chart_type="bar",
            schema_name="public",
            table_name="users",
            extra_config={
                "dimension_column": "country",
                "metrics": [{"column": "revenue", "aggregation": "sum"}],
                "customizations": {
                    "filters": [
                        {
                            "conditions": [
                                {"field": "a", "op": "eq", "value": 1},
                                {"field": "b", "op": "gt", "value": 2},
                            ]
                        }
                    ]
                },
            },
        )

        dumped = chart.extra_config.model_dump()
        assert dumped["customizations"]["filters"][0]["conditions"][0]["field"] == "a"

    def test_chart_response_id_coerced_from_string(self):
        """Test that id can be coerced from string to int"""
        now = datetime.now()
        response = ChartResponse(
            id="123",  # String will be coerced to int
            title="Test",
            description=None,
            chart_type="bar",
            schema_name="public",
            table_name="users",
            extra_config={},
            created_at=now,
            updated_at=now,
        )

        assert response.id == 123
        assert isinstance(response.id, int)

    def test_chart_metric_with_empty_alias(self):
        """Test metric with empty string alias"""
        metric = ChartMetric(column="revenue", aggregation="sum", alias="")

        assert metric.alias == ""


# ================================================================================
# Test ChartCreate typed extra_config — per-chart-type dispatch + leniency
# ================================================================================


class TestChartCreateTypedExtraConfig:
    """The discriminated-union behaviour of ChartCreate.extra_config.

    Each chart_type maps to a sub-schema that enforces only the fields the
    downstream chart_validator requires; everything else (filters, sort,
    pagination, customizations, time_grain, etc.) passes through unchanged
    via extra='allow' so the existing UI payloads are not lossy.
    """

    # ── required-field enforcement ───────────────────────────────────────

    def test_bar_requires_dimension_column(self):
        with pytest.raises(ValidationError):
            ChartCreate(
                title="t",
                chart_type="bar",
                schema_name="public",
                table_name="users",
                extra_config={"metrics": [{"column": "revenue", "aggregation": "sum"}]},
            )

    def test_bar_requires_at_least_one_metric(self):
        with pytest.raises(ValidationError):
            ChartCreate(
                title="t",
                chart_type="bar",
                schema_name="public",
                table_name="users",
                extra_config={"dimension_column": "country", "metrics": []},
            )

    def test_pie_rejects_multiple_metrics(self):
        with pytest.raises(ValidationError):
            ChartCreate(
                title="t",
                chart_type="pie",
                schema_name="public",
                table_name="users",
                extra_config={
                    "dimension_column": "country",
                    "metrics": [
                        {"column": "a", "aggregation": "sum"},
                        {"column": "b", "aggregation": "sum"},
                    ],
                },
            )

    def test_number_rejects_multiple_metrics(self):
        with pytest.raises(ValidationError):
            ChartCreate(
                title="t",
                chart_type="number",
                schema_name="public",
                table_name="users",
                extra_config={
                    "metrics": [
                        {"column": "a", "aggregation": "sum"},
                        {"column": "b", "aggregation": "sum"},
                    ],
                },
            )

    def test_map_requires_geographic_and_geojson_id(self):
        """Map needs geographic_column + selected_geojson_id; value_column is
        Optional because COUNT(*) metrics don't carry a measured column (the
        UI omits value_column for count-based maps)."""
        with pytest.raises(ValidationError):
            ChartCreate(
                title="t",
                chart_type="map",
                schema_name="public",
                table_name="users",
                extra_config={"geographic_column": "state"},
            )

    def test_map_accepts_count_without_value_column(self):
        """A map with COUNT(*) metric and no value_column must validate."""
        chart = ChartCreate(
            title="t",
            chart_type="map",
            schema_name="public",
            table_name="regional_sales",
            extra_config={
                "geographic_column": "region",
                "selected_geojson_id": 1,
                "metrics": [{"column": None, "aggregation": "count"}],
            },
        )
        assert chart.extra_config.value_column is None

    # ── valid happy paths per chart_type ─────────────────────────────────

    def test_line_accepts_same_shape_as_bar(self):
        chart = ChartCreate(
            title="t",
            chart_type="line",
            schema_name="public",
            table_name="users",
            extra_config={
                "dimension_column": "month",
                "metrics": [{"column": "revenue", "aggregation": "sum"}],
            },
        )
        assert chart.extra_config.dimension_column == "month"

    def test_map_valid(self):
        chart = ChartCreate(
            title="t",
            chart_type="map",
            schema_name="public",
            table_name="regional_sales",
            extra_config={
                "geographic_column": "region",
                "value_column": "sales",
                "selected_geojson_id": 1,
            },
        )
        assert chart.extra_config.selected_geojson_id == 1

    def test_table_chart_is_lenient(self):
        """Table charts have varied shapes; the schema enforces no required fields.

        `dimensions` is a list of TableChartDimension objects (column +
        enable_drill_down) — distinct from `dimension_columns: List[str]`.
        """
        chart = ChartCreate(
            title="t",
            chart_type="table",
            schema_name="public",
            table_name="users",
            extra_config={
                "dimensions": [
                    {"column": "country", "enable_drill_down": True},
                    {"column": "state", "enable_drill_down": False},
                ],
                "metrics": [{"column": "revenue", "aggregation": "sum"}],
                "filters": [{"column": "active", "operator": "equals", "value": True}],
            },
        )
        dumped = chart.extra_config.model_dump()
        assert dumped["dimensions"][0] == {"column": "country", "enable_drill_down": True}
        assert dumped["filters"][0]["column"] == "active"

    # ── extras pass-through (the critical "don't break UI" guarantee) ────

    def test_extras_passthrough_on_bar(self):
        """All the extra fields the UI sends survive validation + model_dump()."""
        chart = ChartCreate(
            title="t",
            chart_type="bar",
            schema_name="public",
            table_name="users",
            extra_config={
                "dimension_column": "country",
                "extra_dimension_column": "year",
                "metrics": [{"column": "revenue", "aggregation": "sum"}],
                # filters/sort/pagination are declared on _ChartConfigBase;
                # customizations is now a typed BarChartCustomizations (extras pass through);
                # time_grain/x_axis_column/etc. are undeclared UI extras.
                "customizations": {"orientation": "vertical", "colors": ["#fff"]},
                "filters": [{"column": "active", "operator": "equals", "value": True}],
                "sort": [{"column": "country", "direction": "asc"}],
                "pagination": {"enabled": False, "page_size": 50},
                "time_grain": "month",
                "x_axis_column": "country",
                "y_axis_column": "revenue",
                "aggregate_function": "sum",
                "table_columns": ["country", "revenue"],
            },
        )

        dumped = chart.extra_config.model_dump()
        assert dumped["customizations"]["orientation"] == "vertical"
        assert dumped["customizations"]["colors"] == ["#fff"]
        assert dumped["filters"][0]["column"] == "active"
        assert dumped["sort"][0]["direction"] == "asc"
        assert dumped["pagination"] == {"enabled": False, "page_size": 50}
        assert dumped["time_grain"] == "month"
        assert dumped["x_axis_column"] == "country"
        assert dumped["y_axis_column"] == "revenue"
        assert dumped["aggregate_function"] == "sum"
        assert dumped["table_columns"] == ["country", "revenue"]

    def test_extras_survive_outer_model_dump(self):
        """charts_api.py persists payload.extra_config.model_dump() — must include extras."""
        chart = ChartCreate(
            title="t",
            chart_type="map",
            schema_name="public",
            table_name="regional_sales",
            extra_config={
                "geographic_column": "region",
                "value_column": "sales",
                "selected_geojson_id": 1,
                "customizations": {"colorScheme": "Blues", "theme": "dark"},
                "layers": [{"name": "states"}],
            },
        )
        outer = chart.model_dump()
        # Typed map customizations include the declared field + UI extras.
        assert outer["extra_config"]["customizations"]["colorScheme"] == "Blues"
        assert outer["extra_config"]["customizations"]["theme"] == "dark"
        assert outer["extra_config"]["layers"] == [{"name": "states"}]

    # ── invalid chart_type ───────────────────────────────────────────────

    def test_unknown_chart_type_rejected(self):
        with pytest.raises(ValidationError):
            ChartCreate(
                title="t",
                chart_type="heatmap",  # not in the Literal
                schema_name="public",
                table_name="users",
                extra_config={},
            )

    # ── non-dict extra_config rejected at schema layer ───────────────────

    @pytest.mark.parametrize("bad", [None, "string", 42, [1, 2, 3], 3.14, True])
    def test_extra_config_non_dict_rejected(self, bad):
        """ChartCreate must reject non-dict `extra_config` at validation time
        so the endpoint never reaches `.model_dump()` on a primitive."""
        with pytest.raises(ValidationError):
            ChartCreate(
                title="t",
                chart_type="bar",
                schema_name="public",
                table_name="users",
                extra_config=bad,
            )


# ================================================================================
# Test typed filters / sort / pagination on extra_config
# ================================================================================


class TestChartFilterSortPagination:
    """Filters / sort / pagination are declared on every chart_type config.

    These tests verify (a) the typed shapes accept what the UI sends,
    (b) round-trip through model_dump preserves the structure, (c) wrong
    operators / directions are rejected.
    """

    # Minimal valid bar config used as a base for these tests
    _base_bar = {
        "dimension_column": "country",
        "metrics": [{"column": "revenue", "aggregation": "sum"}],
    }

    def _build(self, **extra):
        return ChartCreate(
            title="t",
            chart_type="bar",
            schema_name="public",
            table_name="users",
            extra_config={**self._base_bar, **extra},
        )

    # ── filters ──────────────────────────────────────────────────────────

    def test_filters_accepts_ui_shape(self):
        chart = self._build(
            filters=[
                {"column": "active", "operator": "equals", "value": True},
                {"column": "region", "operator": "in", "value": ["EU", "NA"]},
                {"column": "deleted_at", "operator": "is_null"},
            ]
        )
        dumped = chart.extra_config.model_dump()
        assert dumped["filters"][0]["operator"] == "equals"
        assert dumped["filters"][1]["value"] == ["EU", "NA"]
        # is_null filter has no value — round-trips as None
        assert dumped["filters"][2]["operator"] == "is_null"

    def test_filter_rejects_unknown_operator(self):
        with pytest.raises(ValidationError):
            self._build(filters=[{"column": "active", "operator": "starts_with"}])

    def test_filter_requires_column_and_operator(self):
        with pytest.raises(ValidationError):
            self._build(filters=[{"operator": "equals", "value": 1}])

    def test_filter_extras_pass_through(self):
        """UI may add fields like value_type or conjunction; those should survive."""
        chart = self._build(
            filters=[
                {
                    "column": "amount",
                    "operator": "greater_than",
                    "value": 100,
                    "value_type": "number",  # extra
                }
            ]
        )
        dumped = chart.extra_config.model_dump()
        assert dumped["filters"][0]["value_type"] == "number"

    # ── sort ─────────────────────────────────────────────────────────────

    def test_sort_accepts_ui_shape(self):
        chart = self._build(
            sort=[
                {"column": "country", "direction": "asc"},
                {"column": "revenue", "direction": "desc"},
            ]
        )
        dumped = chart.extra_config.model_dump()
        assert dumped["sort"][0]["direction"] == "asc"
        assert dumped["sort"][1]["direction"] == "desc"

    def test_sort_rejects_invalid_direction(self):
        with pytest.raises(ValidationError):
            self._build(sort=[{"column": "x", "direction": "ascending"}])

    # ── pagination ───────────────────────────────────────────────────────

    def test_pagination_defaults(self):
        """When the UI sends {}, persisted defaults are enabled=False, page_size=50."""
        chart = self._build(pagination={})
        assert chart.extra_config.pagination.enabled is False
        assert chart.extra_config.pagination.page_size == 50

    def test_pagination_custom(self):
        chart = self._build(pagination={"enabled": True, "page_size": 25})
        dumped = chart.extra_config.model_dump()
        assert dumped["pagination"] == {"enabled": True, "page_size": 25}

    # ── all-empty case (the most common UI payload) ──────────────────────

    def test_empty_filters_sort_pagination_accepted(self):
        """The UI commonly sends `filters: [], sort: [], pagination: {...}` even
        when the user hasn't configured any. Must not error."""
        chart = self._build(filters=[], sort=[], pagination={"enabled": False, "page_size": 50})
        dumped = chart.extra_config.model_dump()
        assert dumped["filters"] == []
        assert dumped["sort"] == []
        assert dumped["pagination"]["page_size"] == 50

    def test_filters_sort_optional_when_absent(self):
        """If the UI omits filters/sort/pagination entirely, no error."""
        chart = self._build()
        assert chart.extra_config.filters is None
        assert chart.extra_config.sort is None
        assert chart.extra_config.pagination is None

    # ── inheritance: same fields on map / table / number ─────────────────

    def test_filters_on_map_chart(self):
        chart = ChartCreate(
            title="t",
            chart_type="map",
            schema_name="public",
            table_name="regional_sales",
            extra_config={
                "geographic_column": "region",
                "value_column": "sales",
                "selected_geojson_id": 1,
                "filters": [{"column": "year", "operator": "equals", "value": 2024}],
            },
        )
        assert chart.extra_config.filters[0].column == "year"

    def test_filters_on_table_chart(self):
        chart = ChartCreate(
            title="t",
            chart_type="table",
            schema_name="public",
            table_name="users",
            extra_config={
                "table_columns": ["id", "name"],
                "filters": [{"column": "active", "operator": "equals", "value": True}],
                "sort": [{"column": "id", "direction": "desc"}],
            },
        )
        assert chart.extra_config.filters[0].operator == "equals"
        assert chart.extra_config.sort[0].direction == "desc"


class TestRealWorldTableChartPayload:
    """Locks in the real chart-builder payload shape for table charts.

    Lifted verbatim from a production save to prevent regressions where the
    typed schema drifts away from what the UI actually sends.
    """

    PAYLOAD = {
        "title": "Chart - agg_pipeline_runs Feb 17, 4:35 PM",
        "chart_type": "table",
        "schema_name": "staging",
        "table_name": "agg_pipeline_runs",
        "extra_config": {
            "dimension_column": "granularity",
            "aggregate_function": "count",
            "customizations": {},
            "filters": [],
            "pagination": {"enabled": False, "page_size": 50},
            "sort": [],
            "table_columns": [
                "date_day",
                "granularity",
                "period_start",
                "period_end",
                "year",
                "month",
            ],
            "metrics": [{"alias": "Total Count", "column": None, "aggregation": "count"}],
            "dimensions": [
                {"column": "granularity", "enable_drill_down": False},
                {"column": "date_day", "enable_drill_down": False},
                {"column": "period_start", "enable_drill_down": False},
                {"column": "period_end", "enable_drill_down": False},
                {"column": "year", "enable_drill_down": False},
                {"column": "month", "enable_drill_down": False},
                {"column": "org_id", "enable_drill_down": False},
                {"column": "org_name", "enable_drill_down": False},
                {"column": "org_slug", "enable_drill_down": False},
                {"column": "work_queue_id", "enable_drill_down": False},
                {"column": "total_pipeline_runs", "enable_drill_down": False},
                {"column": "total_successful_runs", "enable_drill_down": False},
                {"column": "total_failed_runs", "enable_drill_down": False},
                {"column": "total_other_runs", "enable_drill_down": False},
            ],
            "dimension_columns": [
                "granularity",
                "date_day",
                "period_start",
                "period_end",
                "year",
                "month",
                "org_id",
                "org_name",
                "org_slug",
                "work_queue_id",
                "total_pipeline_runs",
                "total_successful_runs",
                "total_failed_runs",
                "total_other_runs",
            ],
        },
    }

    def test_payload_passes_validation(self):
        chart = ChartCreate(**self.PAYLOAD)
        assert chart.chart_type == "table"

    def test_payload_round_trips_through_model_dump(self):
        """The dict that charts_api persists must contain every key the UI sent."""
        chart = ChartCreate(**self.PAYLOAD)
        persisted = chart.extra_config.model_dump()

        for key in self.PAYLOAD["extra_config"]:
            assert key in persisted, f"`{key}` missing from persisted extra_config"

        assert len(persisted["dimensions"]) == 14
        assert persisted["dimensions"][0] == {
            "column": "granularity",
            "enable_drill_down": False,
        }
        assert persisted["dimension_columns"] == self.PAYLOAD["extra_config"]["dimension_columns"]
        assert persisted["pagination"] == {"enabled": False, "page_size": 50}
        assert persisted["aggregate_function"] == "count"
        assert persisted["dimension_column"] == "granularity"

    def test_table_dimension_rejects_missing_column(self):
        with pytest.raises(ValidationError):
            ChartCreate(
                title="t",
                chart_type="table",
                schema_name="public",
                table_name="users",
                extra_config={"dimensions": [{"enable_drill_down": True}]},
            )

    def test_dimension_columns_auto_derived_when_missing(self):
        """If a caller sends `dimensions` without `dimension_columns`, the
        validator backfills the mirror so the backend render path still
        sees a value."""
        chart = ChartCreate(
            title="t",
            chart_type="table",
            schema_name="public",
            table_name="users",
            extra_config={
                "dimensions": [
                    {"column": "country", "enable_drill_down": True},
                    {"column": "state", "enable_drill_down": False},
                ],
                # dimension_columns intentionally omitted
            },
        )
        assert chart.extra_config.dimension_columns == ["country", "state"]

    def test_dimension_columns_preserved_when_supplied(self):
        """If the caller supplies `dimension_columns`, we don't overwrite it."""
        chart = ChartCreate(
            title="t",
            chart_type="table",
            schema_name="public",
            table_name="users",
            extra_config={
                "dimensions": [{"column": "country", "enable_drill_down": False}],
                "dimension_columns": ["country", "extra_legacy_col"],
            },
        )
        assert chart.extra_config.dimension_columns == ["country", "extra_legacy_col"]


# ================================================================================
# Typed customizations — Number / Map charts (ported from ChartValidator rules)
# ================================================================================


class TestTypedCustomizations:
    """Number and Map charts have typed `customizations` sub-schemas that
    constrain the enums the old ChartValidator used to enforce.
    """

    # ── Number chart ─────────────────────────────────────────────────────

    def _number(self, **customizations):
        return ChartCreate(
            title="t",
            chart_type="number",
            schema_name="public",
            table_name="users",
            extra_config={
                "metrics": [{"column": "revenue", "aggregation": "sum"}],
                "customizations": customizations,
            },
        )

    def test_number_accepts_valid_format(self):
        chart = self._number(numberFormat="percentage", decimalPlaces=2)
        assert chart.extra_config.customizations.numberFormat == "percentage"
        assert chart.extra_config.customizations.decimalPlaces == 2

    def test_number_rejects_unknown_format(self):
        with pytest.raises(ValidationError):
            self._number(numberFormat="binary")

    def test_number_rejects_negative_decimal_places(self):
        with pytest.raises(ValidationError):
            self._number(decimalPlaces=-1)

    def test_number_rejects_decimal_places_above_10(self):
        with pytest.raises(ValidationError):
            self._number(decimalPlaces=11)

    def test_number_customizations_extras_pass_through(self):
        """UI may add fields like prefix/suffix; those should survive."""
        chart = self._number(numberFormat="currency", prefix="$", suffix="")
        dumped = chart.extra_config.customizations.model_dump()
        assert dumped["prefix"] == "$"

    def test_number_customizations_optional(self):
        chart = ChartCreate(
            title="t",
            chart_type="number",
            schema_name="public",
            table_name="users",
            extra_config={"metrics": [{"column": "x", "aggregation": "sum"}]},
        )
        assert chart.extra_config.customizations is None

    # ── Map chart ────────────────────────────────────────────────────────

    def _map(self, **customizations):
        return ChartCreate(
            title="t",
            chart_type="map",
            schema_name="public",
            table_name="regional_sales",
            extra_config={
                "geographic_column": "region",
                "value_column": "sales",
                "selected_geojson_id": 1,
                "customizations": customizations,
            },
        )

    def test_map_accepts_valid_color_scheme(self):
        chart = self._map(colorScheme="Blues")
        assert chart.extra_config.customizations.colorScheme == "Blues"

    def test_map_rejects_unknown_color_scheme(self):
        with pytest.raises(ValidationError):
            self._map(colorScheme="Viridis")

    def test_map_customizations_extras_pass_through(self):
        chart = self._map(colorScheme="Greens", strokeWidth=2)
        dumped = chart.extra_config.customizations.model_dump()
        assert dumped["strokeWidth"] == 2


# ================================================================================
# Test ChartUpdate typed extra_config — conditional per-type dispatch
# ================================================================================


class TestChartUpdateTypedExtraConfig:
    """ChartUpdate accepts any subset of fields. The per-type extra_config
    validation runs only when both `chart_type` and `extra_config` are sent.
    A pure metadata update (e.g. just a title change) must still work."""

    def test_metadata_only_update(self):
        """Updating just a title — no extra_config, no chart_type, no validation."""
        update = ChartUpdate(title="New Title")
        assert update.title == "New Title"
        assert update.extra_config is None
        assert update.chart_type is None

    def test_extra_config_without_chart_type_passes_through(self):
        """If chart_type isn't on the payload, we can't dispatch — accept raw dict."""
        update = ChartUpdate(extra_config={"anything": "goes", "even": [1, 2, 3]})
        # Still a dict, not a typed model.
        assert isinstance(update.extra_config, dict)
        assert update.extra_config["anything"] == "goes"

    def test_typed_validation_when_both_present(self):
        """The common chart-builder save path: chart_type + extra_config both sent."""
        update = ChartUpdate(
            chart_type="bar",
            extra_config={
                "dimension_column": "country",
                "metrics": [{"column": "revenue", "aggregation": "sum"}],
            },
        )
        # Typed instance, not a dict.
        assert update.extra_config.dimension_column == "country"

    def test_typed_validation_rejects_invalid(self):
        """Bar update without dimension_column fails just like a create."""
        with pytest.raises(ValidationError):
            ChartUpdate(
                chart_type="bar",
                extra_config={"metrics": [{"column": "revenue", "aggregation": "sum"}]},
            )

    def test_unknown_chart_type_rejected(self):
        with pytest.raises(ValidationError):
            ChartUpdate(chart_type="heatmap")

    @pytest.mark.parametrize("bad", ["string", 42, [1, 2, 3], 3.14, True])
    def test_extra_config_non_dict_rejected(self, bad):
        """ChartUpdate must reject non-dict (and non-None) `extra_config`
        regardless of whether chart_type is also sent — None remains valid
        because it means the partial update doesn't touch extra_config."""
        with pytest.raises(ValidationError):
            ChartUpdate(extra_config=bad)

    def test_filters_validated_on_update(self):
        """Filter operator validation runs the same way as on create."""
        with pytest.raises(ValidationError):
            ChartUpdate(
                chart_type="bar",
                extra_config={
                    "dimension_column": "country",
                    "metrics": [{"column": "revenue", "aggregation": "sum"}],
                    "filters": [{"column": "x", "operator": "starts_with"}],
                },
            )

    def test_update_to_dict_excludes_none(self):
        """Existing behaviour preserved — exclude_none drops unset fields."""
        update = ChartUpdate(title="New")
        data = update.model_dump(exclude_none=True)
        assert "title" in data
        assert "description" not in data
        assert "chart_type" not in data


# ================================================================================
# Real UI-payload round-trip: one per chart_type
# ================================================================================

from ddpui.tests.common.fixtures.chart_ui_payloads import CHART_UI_PAYLOADS


class TestRealUIPayloadRoundTrip:
    """For each chart_type, the captured webapp_v2 payload (incl. customizations)
    must (1) construct a ChartCreate without errors and (2) round-trip every
    typed customization value through model_dump(). When the UI shape drifts,
    update the fixture — these tests fail until you do."""

    @pytest.mark.parametrize("chart_type", list(CHART_UI_PAYLOADS.keys()))
    def test_ui_payload_accepted(self, chart_type):
        payload = CHART_UI_PAYLOADS[chart_type]
        chart = ChartCreate(**payload)
        assert chart.chart_type == chart_type

    @pytest.mark.parametrize("chart_type", list(CHART_UI_PAYLOADS.keys()))
    def test_customizations_round_trip(self, chart_type):
        payload = CHART_UI_PAYLOADS[chart_type]
        chart = ChartCreate(**payload)
        dumped = chart.extra_config.model_dump()
        sent_cust = payload["extra_config"]["customizations"]
        # Every key the UI sent must survive — value-preserving.
        for k, v in sent_cust.items():
            assert (
                dumped["customizations"][k] == v
            ), f"{chart_type}.customizations.{k} drifted: sent={v!r} got={dumped['customizations'][k]!r}"


# ================================================================================
# Enum-rejection sweep: every Literal on the typed customizations
# ================================================================================


def _bar(**cust):
    return dict(BAR_BASE, extra_config={**BAR_BASE["extra_config"], "customizations": cust})


def _line(**cust):
    return dict(LINE_BASE, extra_config={**LINE_BASE["extra_config"], "customizations": cust})


def _pie(**cust):
    return dict(PIE_BASE, extra_config={**PIE_BASE["extra_config"], "customizations": cust})


def _number(**cust):
    return dict(NUM_BASE, extra_config={**NUM_BASE["extra_config"], "customizations": cust})


def _map(**cust):
    return dict(MAP_BASE, extra_config={**MAP_BASE["extra_config"], "customizations": cust})


def _table(**cust):
    return dict(TABLE_BASE, extra_config={**TABLE_BASE["extra_config"], "customizations": cust})


# Minimal valid bases — just enough to satisfy required fields per type.
BAR_BASE = {
    "title": "t",
    "chart_type": "bar",
    "schema_name": "public",
    "table_name": "u",
    "extra_config": {
        "dimension_column": "x",
        "metrics": [{"column": "r", "aggregation": "sum"}],
    },
}
LINE_BASE = {**BAR_BASE, "chart_type": "line"}
PIE_BASE = {
    "title": "t",
    "chart_type": "pie",
    "schema_name": "public",
    "table_name": "u",
    "extra_config": {
        "dimension_column": "x",
        "metrics": [{"column": "r", "aggregation": "sum"}],
    },
}
NUM_BASE = {
    "title": "t",
    "chart_type": "number",
    "schema_name": "public",
    "table_name": "u",
    "extra_config": {"metrics": [{"column": "r", "aggregation": "sum"}]},
}
MAP_BASE = {
    "title": "t",
    "chart_type": "map",
    "schema_name": "public",
    "table_name": "u",
    "extra_config": {"geographic_column": "g", "selected_geojson_id": 1},
}
TABLE_BASE = {
    "title": "t",
    "chart_type": "table",
    "schema_name": "public",
    "table_name": "u",
    "extra_config": {},
}


class TestCustomizationEnumRejection:
    """Every Literal field on the typed customizations must reject unknown values.
    One bad-value test per enum, plus the cross-cutting numeric-range / nested cases."""

    # ── Bar ──────────────────────────────────────────────────────────────
    def test_bar_rejects_invalid_orientation(self):
        with pytest.raises(ValidationError):
            ChartCreate(**_bar(orientation="diagonal"))

    def test_bar_rejects_invalid_dataLabelPosition(self):
        # Bar's enum is "top|inside|insideBottom" — "outside" is Pie's.
        with pytest.raises(ValidationError):
            ChartCreate(**_bar(dataLabelPosition="outside"))

    def test_bar_rejects_invalid_axisLabelRotation(self):
        with pytest.raises(ValidationError):
            ChartCreate(**_bar(xAxisLabelRotation="90"))

    # ── Line ─────────────────────────────────────────────────────────────
    def test_line_rejects_invalid_lineStyle(self):
        with pytest.raises(ValidationError):
            ChartCreate(**_line(lineStyle="dashed"))

    def test_line_rejects_invalid_dataLabelPosition(self):
        # Line's enum is cardinal "top|bottom|left|right" — "inside" is Bar/Pie.
        with pytest.raises(ValidationError):
            ChartCreate(**_line(dataLabelPosition="inside"))

    # ── Pie ──────────────────────────────────────────────────────────────
    def test_pie_rejects_invalid_chartStyle(self):
        with pytest.raises(ValidationError):
            ChartCreate(**_pie(chartStyle="square"))

    def test_pie_rejects_invalid_labelFormat(self):
        with pytest.raises(ValidationError):
            ChartCreate(**_pie(labelFormat="percentage_only"))

    def test_pie_rejects_invalid_dataLabelPosition(self):
        # Pie's enum is "outside|inside" — "top" is Bar/Line's.
        with pytest.raises(ValidationError):
            ChartCreate(**_pie(dataLabelPosition="top"))

    # ── Shared on bar/line/pie ───────────────────────────────────────────
    def test_legendDisplay_rejects_unknown(self):
        with pytest.raises(ValidationError):
            ChartCreate(**_bar(legendDisplay="hidden"))

    def test_cardinal_legendPosition_rejects_corner_form(self):
        # Bar/line/pie use cardinal "top|bottom|left|right" — not corner-form.
        with pytest.raises(ValidationError):
            ChartCreate(**_pie(legendPosition="top-left"))

    # ── Number ───────────────────────────────────────────────────────────
    def test_number_rejects_invalid_numberSize(self):
        with pytest.raises(ValidationError):
            ChartCreate(**_number(numberSize="huge"))

    def test_number_rejects_invalid_numberFormat(self):
        with pytest.raises(ValidationError):
            ChartCreate(**_number(numberFormat="binary"))

    def test_number_rejects_decimalPlaces_out_of_range(self):
        with pytest.raises(ValidationError):
            ChartCreate(**_number(decimalPlaces=11))

    # ── Map ──────────────────────────────────────────────────────────────
    def test_map_rejects_invalid_colorScheme(self):
        with pytest.raises(ValidationError):
            ChartCreate(**_map(colorScheme="Viridis"))

    def test_map_rejects_cardinal_legendPosition(self):
        # Map uses corner-form "top-left|...|bottom-right" — cardinal is wrong.
        with pytest.raises(ValidationError):
            ChartCreate(**_map(legendPosition="top"))

    # ── Table (nested ColumnNumberFormat / ColumnDateFormat) ─────────────
    def test_table_rejects_invalid_nested_numberFormat(self):
        with pytest.raises(ValidationError):
            ChartCreate(**_table(columnFormatting={"col": {"numberFormat": "bogus"}}))

    def test_table_rejects_invalid_nested_dateFormat(self):
        with pytest.raises(ValidationError):
            ChartCreate(**_table(dateColumnFormatting={"col": {"dateFormat": "not_a_real_format"}}))


# ================================================================================
# Payloads that don't match the typed shape — must raise ValidationError
# ================================================================================


class TestPayloadShapeRejection:
    """Every per-chart-type required field must be enforced, and the top-level
    ChartCreate shape must reject missing required fields and wrong primitive
    types. These guard the contract: if the dispatch silently accepts a
    malformed payload, the typed schema has lost its purpose."""

    # ── Top-level required fields ────────────────────────────────────────

    @pytest.mark.parametrize(
        "missing",
        ["title", "chart_type", "schema_name", "table_name", "extra_config"],
    )
    def test_top_level_missing_required_field(self, missing):
        base = {
            "title": "t",
            "chart_type": "bar",
            "schema_name": "public",
            "table_name": "u",
            "extra_config": {
                "dimension_column": "x",
                "metrics": [{"column": "r", "aggregation": "sum"}],
            },
        }
        del base[missing]
        with pytest.raises(ValidationError):
            ChartCreate(**base)

    @pytest.mark.parametrize(
        "field,bad_value",
        [
            ("chart_type", 42),  # int instead of Literal[str]
            ("schema_name", None),  # None instead of str
            ("table_name", ["a"]),  # list instead of str
            ("title", {}),  # dict instead of str
        ],
    )
    def test_top_level_wrong_primitive_type(self, field, bad_value):
        base = {
            "title": "t",
            "chart_type": "bar",
            "schema_name": "public",
            "table_name": "u",
            "extra_config": {
                "dimension_column": "x",
                "metrics": [{"column": "r", "aggregation": "sum"}],
            },
        }
        base[field] = bad_value
        with pytest.raises(ValidationError):
            ChartCreate(**base)

    # ── Per-chart-type: each required field on extra_config ──────────────

    def test_line_requires_dimension_column(self):
        # Line inherits BarChartConfig — same required fields.
        with pytest.raises(ValidationError):
            ChartCreate(
                title="t",
                chart_type="line",
                schema_name="public",
                table_name="u",
                extra_config={"metrics": [{"column": "r", "aggregation": "sum"}]},
            )

    def test_pie_requires_dimension_column(self):
        with pytest.raises(ValidationError):
            ChartCreate(
                title="t",
                chart_type="pie",
                schema_name="public",
                table_name="u",
                extra_config={"metrics": [{"column": "r", "aggregation": "sum"}]},
            )

    def test_pie_requires_at_least_one_metric(self):
        with pytest.raises(ValidationError):
            ChartCreate(
                title="t",
                chart_type="pie",
                schema_name="public",
                table_name="u",
                extra_config={"dimension_column": "x", "metrics": []},
            )

    def test_number_requires_at_least_one_metric(self):
        with pytest.raises(ValidationError):
            ChartCreate(
                title="t",
                chart_type="number",
                schema_name="public",
                table_name="u",
                extra_config={"metrics": []},
            )

    def test_map_requires_geographic_column(self):
        with pytest.raises(ValidationError):
            ChartCreate(
                title="t",
                chart_type="map",
                schema_name="public",
                table_name="u",
                extra_config={"selected_geojson_id": 1},
            )

    def test_map_requires_selected_geojson_id(self):
        with pytest.raises(ValidationError):
            ChartCreate(
                title="t",
                chart_type="map",
                schema_name="public",
                table_name="u",
                extra_config={"geographic_column": "state"},
            )

    # ── Wrong shapes inside extra_config ─────────────────────────────────

    def test_metrics_must_be_a_list(self):
        with pytest.raises(ValidationError):
            ChartCreate(
                title="t",
                chart_type="bar",
                schema_name="public",
                table_name="u",
                extra_config={"dimension_column": "x", "metrics": "not a list"},
            )

    def test_filters_item_must_have_operator(self):
        # Nested ChartFilter missing required `operator`.
        with pytest.raises(ValidationError):
            ChartCreate(
                title="t",
                chart_type="bar",
                schema_name="public",
                table_name="u",
                extra_config={
                    "dimension_column": "x",
                    "metrics": [{"column": "r", "aggregation": "sum"}],
                    "filters": [{"column": "c"}],
                },
            )

    def test_table_dimension_item_must_have_column(self):
        with pytest.raises(ValidationError):
            ChartCreate(
                title="t",
                chart_type="table",
                schema_name="public",
                table_name="u",
                extra_config={"dimensions": [{"enable_drill_down": True}]},
            )

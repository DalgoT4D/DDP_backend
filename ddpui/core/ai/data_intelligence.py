"""
Data Intelligence Service for AI Chat Enhancement

This service builds comprehensive data catalogs and contexts for AI to understand
available data sources, their schemas, and business context.
"""

import logging
from typing import Dict, List, Any, Optional, Set, Tuple
from dataclasses import dataclass, field
from datetime import datetime
import json

from django.db import models
from django.utils import timezone

from sqlalchemy import text
from sqlalchemy import func, column, distinct, cast, Float, Date

from ddpui.models.org import Org, OrgWarehouse
from ddpui.models.dashboard import Dashboard
from ddpui.models.visualization import Chart
from ddpui.datainsights.warehouse.warehouse_factory import WarehouseFactory
from ddpui.datainsights.query_builder import AggQueryBuilder
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui.core.ai.data_intelligence")


@dataclass
class DataColumnInfo:
    """Information about a database column"""

    name: str
    data_type: str
    nullable: bool
    sample_values: List[Any] = field(default_factory=list)
    business_context: str = ""
    is_dimension: bool = False
    is_metric: bool = False
    translated_type: str = "string"  # For frontend consumption


@dataclass
class DataTableInfo:
    """Information about a database table"""

    schema_name: str
    table_name: str
    columns: List[DataColumnInfo] = field(default_factory=list)
    row_count_estimate: Optional[int] = None
    charts_using_table: List[int] = field(default_factory=list)  # Chart IDs
    business_description: str = ""
    last_analyzed: Optional[datetime] = None


@dataclass
class DataCatalog:
    """Complete data catalog for an organization"""

    org_id: int
    tables: Dict[str, DataTableInfo] = field(default_factory=dict)  # key: schema.table
    total_charts: int = 0
    total_tables: int = 0
    chart_table_mappings: Dict[int, str] = field(default_factory=dict)  # chart_id -> table_key
    generated_at: datetime = field(default_factory=timezone.now)


class DataIntelligenceService:
    """
    Service to analyze and catalog organization's data for AI consumption.

    This service builds comprehensive metadata about available tables, columns,
    and their business context to help AI understand what data is available
    and how to query it effectively.
    """

    def __init__(self):
        self.logger = CustomLogger("DataIntelligenceService")

    def get_org_data_catalog(self, org: Org, force_refresh: bool = False) -> DataCatalog:
        """
        Build comprehensive catalog of available data sources for an organization.

        Args:
            org: Organization to analyze
            force_refresh: If True, rebuild catalog from scratch

        Returns:
            DataCatalog with complete metadata about org's data
        """
        self.logger.info(f"Building data catalog for org {org.slug}")

        try:
            # Get warehouse connection
            org_warehouse = OrgWarehouse.objects.filter(org=org).first()
            if not org_warehouse:
                self.logger.warning(f"No warehouse configured for org {org.slug}")
                return DataCatalog(org_id=org.id)

            # Initialize catalog
            catalog = DataCatalog(org_id=org.id)

            # Get all charts to understand what tables are actually being used
            charts = Chart.objects.filter(org=org).select_related("org")
            catalog.total_charts = len(charts)

            # Build table inventory from charts
            table_chart_mapping = self._build_table_chart_mapping(charts)
            catalog.chart_table_mappings = {
                chart.id: f"{chart.schema_name}.{chart.table_name}"
                for chart in charts
                if chart.schema_name and chart.table_name
            }

            # Analyze each unique table
            warehouse_client = WarehouseFactory.get_warehouse_client(org_warehouse)

            for table_key, chart_ids in table_chart_mapping.items():
                try:
                    table_info = self._analyze_table(table_key, chart_ids, warehouse_client, charts)
                    catalog.tables[table_key] = table_info

                except Exception as table_error:
                    self.logger.error(f"Error analyzing table {table_key}: {table_error}")
                    # Create minimal table info to maintain catalog completeness
                    schema_name, table_name = table_key.split(".", 1)
                    catalog.tables[table_key] = DataTableInfo(
                        schema_name=schema_name,
                        table_name=table_name,
                        charts_using_table=chart_ids,
                        business_description=f"Table analysis failed: {str(table_error)}",
                    )

            catalog.total_tables = len(catalog.tables)

            self.logger.info(
                f"Data catalog built for org {org.slug}: {catalog.total_tables} tables, "
                f"{catalog.total_charts} charts"
            )

            return catalog

        except Exception as e:
            self.logger.error(f"Error building data catalog for org {org.slug}: {e}")
            # Return empty catalog rather than failing
            return DataCatalog(org_id=org.id)

    def _build_table_chart_mapping(self, charts) -> Dict[str, List[int]]:
        """Build mapping of tables to chart IDs that use them"""
        table_chart_mapping = {}

        for chart in charts:
            if not chart.schema_name or not chart.table_name:
                continue

            table_key = f"{chart.schema_name}.{chart.table_name}"
            if table_key not in table_chart_mapping:
                table_chart_mapping[table_key] = []
            table_chart_mapping[table_key].append(chart.id)

        return table_chart_mapping

    def _analyze_table(
        self, table_key: str, chart_ids: List[int], warehouse_client, all_charts
    ) -> DataTableInfo:
        """Analyze a single table and build comprehensive metadata"""
        schema_name, table_name = table_key.split(".", 1)

        self.logger.debug(f"Analyzing table {table_key}")

        # Create base table info
        table_info = DataTableInfo(
            schema_name=schema_name,
            table_name=table_name,
            charts_using_table=chart_ids,
            last_analyzed=timezone.now(),
        )

        try:
            # Get column metadata from warehouse
            column_list = warehouse_client.get_table_columns(schema_name, table_name)

            for col_data in column_list:
                if isinstance(col_data, dict):
                    column_info = DataColumnInfo(
                        name=col_data.get("name", "unknown"),
                        data_type=col_data.get("data_type", "unknown"),
                        nullable=col_data.get("nullable", True),
                        translated_type=col_data.get("translated_type", "string"),
                    )

                    # Enhance with business context
                    self._enrich_column_context(column_info, chart_ids, all_charts)

                    # Get sample values for context (limited and safe)
                    column_info.sample_values = self._get_safe_sample_values(
                        warehouse_client, schema_name, table_name, column_info.name
                    )

                    table_info.columns.append(column_info)

            # Get table row count estimate
            table_info.row_count_estimate = self._get_table_row_estimate(
                warehouse_client, schema_name, table_name
            )

            # Generate business description
            table_info.business_description = self._generate_table_business_description(
                table_info, chart_ids, all_charts
            )

        except Exception as e:
            self.logger.error(f"Error analyzing table structure for {table_key}: {e}")
            table_info.business_description = f"Analysis failed: {str(e)}"

        return table_info

    def _enrich_column_context(self, column_info: DataColumnInfo, chart_ids: List[int], all_charts):
        """Enrich column information with business context from chart usage"""
        # Determine if column is used as dimension or metric
        charts_using_table = [c for c in all_charts if c.id in chart_ids]

        for chart in charts_using_table:
            extra_config = chart.extra_config or {}

            # Check if used as dimension
            dimension_columns = [
                extra_config.get("dimension_column"),
                extra_config.get("extra_dimension_column"),
                extra_config.get("geographic_column"),
                extra_config.get("x_axis_column"),
            ]

            if column_info.name in [col for col in dimension_columns if col]:
                column_info.is_dimension = True

            # Check if used in metrics
            metrics = extra_config.get("metrics", [])
            for metric in metrics:
                if isinstance(metric, dict) and metric.get("column") == column_info.name:
                    column_info.is_metric = True

            # Check y_axis usage (often metrics)
            if column_info.name == extra_config.get("y_axis_column"):
                column_info.is_metric = True

        # Generate business context based on column name patterns
        column_info.business_context = self._generate_column_business_context(column_info)

    def _generate_column_business_context(self, column_info: DataColumnInfo) -> str:
        """Generate business context description for a column"""
        col_name_lower = column_info.name.lower()

        # Common business patterns
        if any(term in col_name_lower for term in ["revenue", "sales", "income", "earnings"]):
            return "Financial metric - likely represents monetary values"
        elif any(term in col_name_lower for term in ["count", "total", "sum", "quantity"]):
            return "Numeric metric - represents counts or quantities"
        elif any(term in col_name_lower for term in ["date", "time", "created", "updated"]):
            return "Temporal dimension - used for time-based analysis"
        elif any(term in col_name_lower for term in ["name", "title", "description"]):
            return "Text dimension - descriptive information"
        elif any(term in col_name_lower for term in ["id", "key", "code"]):
            return "Identifier - used for joins and grouping"
        elif any(term in col_name_lower for term in ["state", "country", "region", "location"]):
            return "Geographic dimension - used for location-based analysis"
        elif any(term in col_name_lower for term in ["category", "type", "status", "group"]):
            return "Categorical dimension - used for grouping and filtering"
        elif column_info.data_type.lower() in ["boolean", "bool"]:
            return "Boolean flag - represents yes/no conditions"
        elif (
            "decimal" in column_info.data_type.lower() or "numeric" in column_info.data_type.lower()
        ):
            return "Numeric data - can be used for calculations and aggregations"
        else:
            usage_type = (
                "dimension"
                if column_info.is_dimension
                else "metric"
                if column_info.is_metric
                else "data"
            )
            return f"Column used as {usage_type} in existing charts"

    def _get_safe_sample_values(
        self, warehouse_client, schema_name: str, table_name: str, column_name: str, limit: int = 5
    ) -> List[Any]:
        """Safely get sample values from a column"""
        try:
            # Updated this query to AggQueryBuilder

            # query = text(
            #     f"""
            #     SELECT DISTINCT "{column_name}"
            #     FROM "{schema_name}"."{table_name}"
            #     WHERE "{column_name}" IS NOT NULL
            #     LIMIT :limit_val
            # """
            # )

            # Build query using AggQueryBuilder
            query_builder = AggQueryBuilder()
            query_builder.fetch_from(table_name, schema_name)
            query_builder.add_column(column(column_name))
            query_builder.limit_rows(limit)
            query_builder.offset_rows(0)
            query_builder.where_clause(column(column_name).isnot(None))
            query_builder.build()

            results = warehouse_client.execute(query_builder)

            sample_values = []
            for row in results[:limit]:
                if isinstance(row, dict):
                    value = row.get(column_name)
                else:
                    value = row[0] if len(row) > 0 else None

                if value is not None:
                    # Convert to safe string representation for AI context
                    if isinstance(value, (int, float, str, bool)):
                        sample_values.append(value)
                    else:
                        sample_values.append(str(value)[:100])  # Truncate long values

            return sample_values[:limit]

        except Exception as e:
            self.logger.debug(
                f"Could not get sample values for {schema_name}.{table_name}.{column_name}: {e}"
            )
            return []

    def _get_table_row_estimate(
        self, warehouse_client, schema_name: str, table_name: str
    ) -> Optional[int]:
        """Get estimated row count for a table"""
        try:
            # Moved the below query to AggQueryBuilder
            # query = text(f'SELECT COUNT(*) FROM "{schema_name}"."{table_name}" LIMIT 1000000')
            # results = warehouse_client.execute(query)

            # Build query using AggQueryBuilder
            query_builder = AggQueryBuilder()
            query_builder.fetch_from(table_name, schema_name)
            query_builder.add_column(func.count())
            query_builder.limit_rows(1000000)
            query_builder.build()

            results = warehouse_client.execute(query_builder)

            if results and len(results) > 0:
                if isinstance(results[0], dict):
                    return list(results[0].values())[0]
                else:
                    return results[0][0] if len(results[0]) > 0 else None

            return None

        except Exception as e:
            self.logger.debug(f"Could not get row count for {schema_name}.{table_name}: {e}")
            return None

    def _generate_table_business_description(
        self, table_info: DataTableInfo, chart_ids: List[int], all_charts
    ) -> str:
        """Generate business-friendly description of what this table contains"""
        charts_using_table = [c for c in all_charts if c.id in chart_ids]

        # Start with basic info
        description_parts = [f"Table {table_info.schema_name}.{table_info.table_name}"]

        if table_info.row_count_estimate:
            if table_info.row_count_estimate > 1000000:
                description_parts.append(
                    f"(large table with {table_info.row_count_estimate:,} rows)"
                )
            else:
                description_parts.append(f"({table_info.row_count_estimate:,} rows)")

        # Add column summary
        total_cols = len(table_info.columns)
        dimension_cols = sum(1 for col in table_info.columns if col.is_dimension)
        metric_cols = sum(1 for col in table_info.columns if col.is_metric)

        description_parts.append(f"has {total_cols} columns")

        if dimension_cols > 0 or metric_cols > 0:
            details = []
            if dimension_cols > 0:
                details.append(f"{dimension_cols} dimensions")
            if metric_cols > 0:
                details.append(f"{metric_cols} metrics")
            description_parts.append(f"including {', '.join(details)}")

        # Add chart usage context
        if charts_using_table:
            chart_types = [chart.chart_type for chart in charts_using_table]
            unique_chart_types = list(set(chart_types))

            if len(unique_chart_types) == 1:
                description_parts.append(
                    f"Used in {len(charts_using_table)} {unique_chart_types[0]} chart(s)"
                )
            else:
                description_parts.append(
                    f"Used in {len(charts_using_table)} charts ({', '.join(unique_chart_types[:3])})"
                )

        # Add business context hints from column names
        business_indicators = []
        column_names = [col.name.lower() for col in table_info.columns]

        if any(term in " ".join(column_names) for term in ["revenue", "sales", "profit", "cost"]):
            business_indicators.append("financial data")
        if any(
            term in " ".join(column_names)
            for term in ["student", "school", "education", "enrollment"]
        ):
            business_indicators.append("education data")
        if any(term in " ".join(column_names) for term in ["customer", "user", "client"]):
            business_indicators.append("customer data")
        if any(term in " ".join(column_names) for term in ["product", "item", "inventory"]):
            business_indicators.append("product data")
        if any(
            term in " ".join(column_names) for term in ["state", "country", "region", "location"]
        ):
            business_indicators.append("geographic data")

        if business_indicators:
            description_parts.append(f"Contains {', '.join(business_indicators[:2])}")

        return ". ".join(description_parts) + "."

    def build_ai_data_context(
        self, org: Org, dashboard_id: Optional[int] = None, focus_tables: Optional[List[str]] = None
    ) -> str:
        """
        Build comprehensive data context for AI consumption.

        Args:
            org: Organization to analyze
            dashboard_id: Optional dashboard to focus on
            focus_tables: Optional list of table keys to focus on

        Returns:
            Rich text context describing available data for AI
        """
        self.logger.info(f"Building AI data context for org {org.slug}")

        # Get data catalog
        catalog = self.get_org_data_catalog(org)

        if not catalog.tables:
            return "No data tables are available for analysis."

        # Build context sections
        context_parts = [
            "=== DATA AVAILABLE FOR ANALYSIS ===\n",
            f"Organization: {org.name}",
            f"Total Tables: {catalog.total_tables}",
            f"Total Charts: {catalog.total_charts}",
            f"Analysis Generated: {catalog.generated_at.strftime('%Y-%m-%d %H:%M:%S')}\n",
        ]

        # Filter tables if focus is specified
        tables_to_describe = catalog.tables
        if focus_tables:
            tables_to_describe = {
                key: table for key, table in catalog.tables.items() if key in focus_tables
            }
        elif dashboard_id:
            # Focus on tables used by charts in this dashboard
            dashboard_tables = self._get_dashboard_tables(dashboard_id, catalog)
            if dashboard_tables:
                tables_to_describe = {
                    key: table for key, table in catalog.tables.items() if key in dashboard_tables
                }

        # Describe each table in detail
        context_parts.append("=== AVAILABLE DATA TABLES ===\n")

        for table_key, table_info in tables_to_describe.items():
            context_parts.extend(self._build_table_context_description(table_info))
            context_parts.append("")  # Empty line between tables

        # Add query guidelines
        context_parts.extend(self._build_query_guidelines(tables_to_describe))

        return "\n".join(context_parts)

    def _get_dashboard_tables(self, dashboard_id: int, catalog: DataCatalog) -> Set[str]:
        """Get set of table keys used by charts in a specific dashboard"""
        try:
            dashboard = Dashboard.objects.get(id=dashboard_id)
            dashboard_chart_ids = []

            # Extract chart IDs from dashboard components
            if dashboard.components:
                for component in dashboard.components.values():
                    if component.get("type") == "chart":
                        chart_id = component.get("config", {}).get("chartId")
                        if chart_id:
                            dashboard_chart_ids.append(chart_id)

            # Map chart IDs to table keys
            dashboard_tables = set()
            for chart_id, table_key in catalog.chart_table_mappings.items():
                if chart_id in dashboard_chart_ids:
                    dashboard_tables.add(table_key)

            return dashboard_tables

        except Dashboard.DoesNotExist:
            return set()
        except Exception as e:
            self.logger.error(f"Error getting dashboard tables for {dashboard_id}: {e}")
            return set()

    def _build_table_context_description(self, table_info: DataTableInfo) -> List[str]:
        """Build detailed description of a table for AI context"""
        lines = [
            f"TABLE: {table_info.schema_name}.{table_info.table_name}",
            f"Description: {table_info.business_description}",
        ]

        if table_info.row_count_estimate:
            lines.append(f"Estimated rows: {table_info.row_count_estimate:,}")

        # Group columns by type for better AI understanding
        dimension_cols = [col for col in table_info.columns if col.is_dimension]
        metric_cols = [col for col in table_info.columns if col.is_metric]
        other_cols = [
            col for col in table_info.columns if not col.is_dimension and not col.is_metric
        ]

        if dimension_cols:
            lines.append("\nDIMENSION COLUMNS (for grouping/filtering):")
            for col in dimension_cols[:10]:  # Limit to prevent context overflow
                sample_text = (
                    f" (samples: {', '.join(map(str, col.sample_values[:3]))})"
                    if col.sample_values
                    else ""
                )
                lines.append(f"  - {col.name} ({col.data_type}){sample_text}")
                if col.business_context:
                    lines.append(f"    Context: {col.business_context}")

        if metric_cols:
            lines.append("\nMETRIC COLUMNS (for calculations/aggregations):")
            for col in metric_cols[:10]:
                sample_text = (
                    f" (samples: {', '.join(map(str, col.sample_values[:3]))})"
                    if col.sample_values
                    else ""
                )
                lines.append(f"  - {col.name} ({col.data_type}){sample_text}")
                if col.business_context:
                    lines.append(f"    Context: {col.business_context}")

        if other_cols and len(other_cols) <= 20:  # Only show if manageable number
            lines.append("\nOTHER COLUMNS:")
            for col in other_cols[:15]:  # Limit to prevent context overflow
                sample_text = (
                    f" (samples: {', '.join(map(str, col.sample_values[:2]))})"
                    if col.sample_values
                    else ""
                )
                lines.append(f"  - {col.name} ({col.data_type}){sample_text}")
        elif other_cols:
            lines.append(f"\nOTHER COLUMNS: {len(other_cols)} additional columns available")

        return lines

    def _build_query_guidelines(self, tables: Dict[str, DataTableInfo]) -> List[str]:
        """Build guidelines for AI on how to query the available data"""
        lines = [
            "\n=== QUERY GUIDELINES ===",
            "",
            "AVAILABLE OPERATIONS:",
            "- SELECT queries only (no INSERT/UPDATE/DELETE)",
            "- Aggregations: COUNT, SUM, AVG, MIN, MAX",
            "- Grouping: GROUP BY with dimension columns",
            "- Filtering: WHERE with specific conditions",
            "- Ordering: ORDER BY for sorted results",
            "- Limiting: LIMIT for result size control",
            "",
            "SECURITY CONSTRAINTS:",
            "- All queries are parameterized for safety",
            "- Maximum 1000 rows per query result",
            "- 30-second timeout limit",
            "- Read-only access to data",
            "",
            "QUERY EXAMPLES FOR AVAILABLE DATA:",
        ]

        # Generate example queries based on available tables
        for table_key, table_info in list(tables.items())[:3]:  # Limit examples
            dimension_cols = [col.name for col in table_info.columns if col.is_dimension]
            metric_cols = [col.name for col in table_info.columns if col.is_metric]

            if dimension_cols and metric_cols:
                lines.append(
                    f"  - Count by {dimension_cols[0]}: "
                    f"SELECT {dimension_cols[0]}, COUNT(*) FROM {table_key} GROUP BY {dimension_cols[0]}"
                )

                if len(metric_cols) > 0:
                    lines.append(
                        f"  - Sum {metric_cols[0]} by {dimension_cols[0]}: "
                        f"SELECT {dimension_cols[0]}, SUM({metric_cols[0]}) FROM {table_key} GROUP BY {dimension_cols[0]}"
                    )

        return lines

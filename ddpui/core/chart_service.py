"""
Chart service module for creating and managing charts
"""
import json
import hashlib
import logging
from typing import Dict, Any, List, Optional, Union
from datetime import datetime, timedelta
from django.utils import timezone
from django.db import transaction
from django.core.exceptions import ValidationError
from sqlalchemy import text, func, column, select, table
from sqlalchemy.sql.functions import Function
from sqlalchemy.sql.expression import ColumnClause
from ddpui.models.chart import Chart, ChartSnapshot
from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser
from ddpui.datainsights.query_builder import AggQueryBuilder
from ddpui.datainsights.warehouse.warehouse_factory import WarehouseFactory
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("chart_service")


class ChartService:
    """Service class for chart operations"""

    def __init__(self, org: Org, user: OrgUser):
        self.org = org
        self.user = user
        from ddpui.models.org import OrgWarehouse

        warehouse = OrgWarehouse.objects.filter(org=org).first()
        if warehouse:
            try:
                from ddpui.utils import secretsmanager

                credentials = secretsmanager.retrieve_warehouse_credentials(warehouse)
                if credentials:
                    self.warehouse = WarehouseFactory.connect(credentials, warehouse.wtype)
                    # Test connection health
                    self._test_connection_health()
                else:
                    logger.error(f"No credentials found for warehouse {warehouse.id}")
                    self.warehouse = None
            except Exception as e:
                logger.error(f"Failed to connect to warehouse for org {org.slug}: {str(e)}")
                self.warehouse = None
        else:
            self.warehouse = None

    def create_chart(
        self,
        title: str,
        description: str,
        chart_type: str,
        schema_name: str,
        table_name: str,
        config: Dict[str, Any],
        is_public: bool = False,
    ) -> Chart:
        """Create a new chart"""
        logger.info(
            f"Creating chart '{title}' for org {self.org.slug} by user {self.user.user.email}"
        )

        try:
            # Check if warehouse exists
            if not self.warehouse:
                logger.error(f"No warehouse configured for organization {self.org.slug}")
                raise ValidationError("No warehouse configured for this organization")

            # Validate and sanitize inputs
            title = self._sanitize_string_input(title, 255)
            description = self._sanitize_string_input(description, 1000)

            if not title:
                logger.error("Chart title is required but was empty")
                raise ValidationError("Chart title is required")

            # Validate table and schema names
            self._validate_table_and_schema_names(schema_name, table_name)

            # Validate chart type (should be chart library)
            valid_chart_libraries = ["echarts", "nivo", "recharts"]
            if chart_type not in valid_chart_libraries:
                logger.error(f"Invalid chart type: {chart_type}")
                raise ValidationError(f"Invalid chart type: {chart_type}")

            with transaction.atomic():
                # Validate configuration
                self._validate_chart_config(config)

                # Create chart
                chart = Chart.create_chart(
                    org=self.org,
                    created_by=self.user,
                    title=title,
                    description=description,
                    chart_type=chart_type,
                    schema_name=schema_name,
                    table_name=table_name,
                    config=config,
                    is_public=is_public,
                )

                logger.info(f"Successfully created chart {chart.id} '{title}'")
                return chart

        except ValidationError:
            # Re-raise validation errors without wrapping
            raise
        except Exception as e:
            logger.error(f"Unexpected error creating chart: {str(e)}", exc_info=True)
            raise ValidationError(f"Failed to create chart: {str(e)}")

    def update_chart(
        self,
        chart_id: int,
        title: str = None,
        description: str = None,
        chart_type: str = None,
        schema_name: str = None,
        table_name: str = None,
        config: Dict[str, Any] = None,
        is_public: bool = None,
    ) -> Chart:
        """Update an existing chart"""
        try:
            chart = Chart.objects.get(id=chart_id, org=self.org)

            # Check permissions
            if not chart.can_edit(self.user):
                raise ValidationError("You don't have permission to edit this chart")

            if config:
                self._validate_chart_config(config)

            chart.update_chart(
                title=title,
                description=description,
                chart_type=chart_type,
                schema_name=schema_name,
                table_name=table_name,
                config=config,
                is_public=is_public,
            )

            # Clear related snapshots
            chart.snapshots.all().delete()

            return chart

        except Chart.DoesNotExist:
            raise ValidationError("Chart not found")
        except Exception as e:
            raise ValidationError(f"Failed to update chart: {str(e)}")

    def delete_chart(self, chart_id: int) -> bool:
        """Delete a chart"""
        try:
            chart = Chart.objects.get(id=chart_id, org=self.org)

            # Check permissions
            if not chart.can_edit(self.user):
                raise ValidationError("You don't have permission to delete this chart")

            chart.delete()
            return True

        except Chart.DoesNotExist:
            raise ValidationError("Chart not found")
        except Exception as e:
            raise ValidationError(f"Failed to delete chart: {str(e)}")

    def get_chart(self, chart_id: int) -> Chart:
        """Get a specific chart"""
        try:
            chart = Chart.objects.get(id=chart_id, org=self.org)

            # Check permissions
            if not chart.can_view(self.user):
                raise ValidationError("You don't have permission to view this chart")

            return chart

        except Chart.DoesNotExist:
            raise ValidationError("Chart not found")

    def get_charts(self, filters: Dict[str, Any] = None) -> List[Chart]:
        """Get charts with optional filters"""
        queryset = Chart.objects.filter(org=self.org)

        if filters:
            if "chart_type" in filters:
                queryset = queryset.filter(chart_type=filters["chart_type"])
            if "schema_name" in filters:
                queryset = queryset.filter(schema_name=filters["schema_name"])
            if "table_name" in filters:
                queryset = queryset.filter(table_name=filters["table_name"])
            if "created_by" in filters:
                queryset = queryset.filter(created_by=filters["created_by"])
            if "is_public" in filters:
                queryset = queryset.filter(is_public=filters["is_public"])

        # Filter by view permissions
        if (
            hasattr(self.user, "new_role")
            and self.user.new_role
            and self.user.new_role.slug != "account_manager"
        ):
            from django.db import models

            queryset = queryset.filter(models.Q(is_public=True) | models.Q(created_by=self.user))

        return list(queryset)

    def generate_chart_data(
        self,
        chart_type: str,
        computation_type: str,
        schema_name: str,
        table_name: str,
        xaxis: str = None,
        yaxis: str = None,
        dimensions: Union[str, List[str]] = None,
        aggregate_col: str = None,
        aggregate_func: str = None,
        aggregate_col_alias: str = None,
        dimension_col: str = None,
        offset: int = 0,
        limit: int = 100,
    ) -> Dict[str, Any]:
        """Generate chart data based on configuration"""
        try:
            # Validate parameters for SQL injection
            if xaxis and not self._is_valid_column_name(xaxis):
                raise ValidationError(f"Invalid xaxis column name: {xaxis}")
            if yaxis and not self._is_valid_column_name(yaxis):
                raise ValidationError(f"Invalid yaxis column name: {yaxis}")
            if aggregate_col and not self._is_valid_column_name(aggregate_col):
                raise ValidationError(f"Invalid aggregate column name: {aggregate_col}")
            if dimension_col and not self._is_valid_column_name(dimension_col):
                raise ValidationError(f"Invalid dimension column name: {dimension_col}")

            # Validate schema and table names
            self._validate_table_and_schema_names(schema_name, table_name)
            # Generate cache key
            cache_key = self._generate_cache_key(
                chart_type,
                computation_type,
                schema_name,
                table_name,
                xaxis,
                yaxis,
                dimensions,
                aggregate_col,
                aggregate_func,
                aggregate_col_alias,
                dimension_col,
                offset,
                limit,
            )

            # Check for cached data
            cached_data = self._get_cached_data(cache_key)
            if cached_data:
                return cached_data

            # Generate query based on computation type
            if computation_type == "raw":
                query_result = self._generate_raw_query(
                    schema_name, table_name, xaxis, yaxis, offset, limit
                )
            else:
                query_result = self._generate_aggregated_query(
                    schema_name,
                    table_name,
                    xaxis,
                    aggregate_col,
                    aggregate_func,
                    aggregate_col_alias,
                    dimension_col,
                    offset,
                    limit,
                )

            # Transform data for chart
            chart_data = self._transform_data_for_chart(
                query_result,
                chart_type,
                computation_type,
                xaxis,
                yaxis,
                aggregate_col_alias,
                dimension_col,
            )

            # Generate ECharts configuration
            echarts_config = self._generate_echarts_config(
                chart_data, chart_type, xaxis, yaxis, aggregate_col_alias
            )

            result = {
                "chart_config": echarts_config,
                "raw_data": chart_data,
                "metadata": {
                    "chart_type": chart_type,
                    "computation_type": computation_type,
                    "schema_name": schema_name,
                    "table_name": table_name,
                    "record_count": len(chart_data.get("data", [])),
                    "generated_at": timezone.now().isoformat(),
                },
            }

            # Cache the result
            self._cache_data(cache_key, result)

            return result

        except Exception as e:
            raise ValidationError(f"Failed to generate chart data: {str(e)}")

    def _validate_chart_config(self, config):
        """Validate chart configuration with comprehensive checks"""
        # Convert schema object to dict if needed
        if hasattr(config, "dict"):
            config = config.dict()
        elif hasattr(config, "__dict__"):
            config = config.__dict__

        # Basic required fields validation
        required_fields = ["chartType", "computation_type"]

        for field in required_fields:
            if field not in config:
                raise ValidationError(f"Missing required field: {field}")

        # Validate chart type
        valid_chart_types = [
            "bar",
            "line",
            "pie",
            "area",
            "scatter",
            "funnel",
            "heatmap",
            "radar",
            "number",
            "map",
        ]
        if config["chartType"] not in valid_chart_types:
            raise ValidationError(
                f"Invalid chart type: {config['chartType']}. Must be one of: {', '.join(valid_chart_types)}"
            )

        # Validate computation type
        valid_computation_types = ["raw", "aggregated"]
        if config["computation_type"] not in valid_computation_types:
            raise ValidationError(
                f"Invalid computation type: {config['computation_type']}. Must be one of: {', '.join(valid_computation_types)}"
            )

        # Validate fields based on computation type
        if config["computation_type"] == "raw":
            if not config.get("xAxis") or not config.get("yAxis"):
                raise ValidationError("xAxis and yAxis are required for raw computation")

            # Validate axis field names (basic SQL injection prevention)
            for axis_name, axis_value in [
                ("xAxis", config.get("xAxis")),
                ("yAxis", config.get("yAxis")),
            ]:
                if axis_value and not self._is_valid_column_name(axis_value):
                    raise ValidationError(f"Invalid {axis_name} column name: {axis_value}")
        else:
            if not config.get("aggregate_func"):
                raise ValidationError("aggregate_func is required for aggregated computation")

            # Validate aggregate function
            valid_agg_funcs = ["sum", "avg", "count", "min", "max", "stddev", "variance"]
            if config["aggregate_func"] not in valid_agg_funcs:
                raise ValidationError(
                    f"Invalid aggregate function: {config['aggregate_func']}. Must be one of: {', '.join(valid_agg_funcs)}"
                )

            # Validate aggregate column if provided
            if config.get("aggregate_col") and not self._is_valid_column_name(
                config["aggregate_col"]
            ):
                raise ValidationError(f"Invalid aggregate column name: {config['aggregate_col']}")

        # Validate dimensions if provided
        if config.get("dimensions"):
            dimensions = config["dimensions"]
            if isinstance(dimensions, str):
                dimensions = [dimensions]
            elif not isinstance(dimensions, list):
                raise ValidationError("Dimensions must be a string or list of strings")

            for dim in dimensions:
                if not self._is_valid_column_name(dim):
                    raise ValidationError(f"Invalid dimension column name: {dim}")

    def _is_valid_column_name(self, column_name: str) -> bool:
        """Validate column name to prevent SQL injection"""
        if not column_name or not isinstance(column_name, str):
            return False

        # Check for basic SQL injection patterns
        dangerous_patterns = [
            ";",
            "--",
            "/*",
            "*/",
            "union",
            "select",
            "drop",
            "delete",
            "insert",
            "update",
            "create",
            "alter",
            "exec",
            "execute",
            "sp_",
            "xp_",
        ]

        column_lower = column_name.lower()
        for pattern in dangerous_patterns:
            if pattern in column_lower:
                return False

        # Allow only alphanumeric characters, underscores, and dots (for schema.table.column)
        import re

        return bool(re.match(r"^[a-zA-Z0-9_\.]+$", column_name))

    def _validate_table_and_schema_names(self, schema_name: str, table_name: str):
        """Validate schema and table names"""
        if not self._is_valid_column_name(schema_name):
            raise ValidationError(f"Invalid schema name: {schema_name}")

        if not self._is_valid_column_name(table_name):
            raise ValidationError(f"Invalid table name: {table_name}")

    def _sanitize_string_input(self, value: str, max_length: int = 255) -> str:
        """Sanitize string input"""
        if not value:
            return ""

        # Trim whitespace and limit length
        sanitized = str(value).strip()[:max_length]

        # Remove any null bytes
        sanitized = sanitized.replace("\0", "")

        return sanitized

    def _generate_raw_query(
        self, schema_name: str, table_name: str, xaxis: str, yaxis: str, offset: int, limit: int
    ) -> List[Dict]:
        """Generate raw data query"""
        query = (
            select([column(xaxis).label("x"), column(yaxis).label("y")])
            .select_from(table(table_name, schema=schema_name))
            .offset(offset)
        )

        # Apply performance optimizations
        query = self._optimize_query_performance(query, limit)

        return self.warehouse.execute(query)

    def _generate_aggregated_query(
        self,
        schema_name: str,
        table_name: str,
        xaxis: str,
        aggregate_col: str,
        aggregate_func: str,
        aggregate_col_alias: str = None,
        dimension_col: str = None,
        offset: int = 0,
        limit: int = 100,
    ) -> List[Dict]:
        """Generate aggregated data query"""
        query_builder = AggQueryBuilder()

        # Add X-axis column
        query_builder.add_column(column(xaxis))

        # Add aggregate column
        if aggregate_func == "count":
            agg_col = func.count().label(aggregate_col_alias or "count")
        elif aggregate_func == "sum":
            agg_col = func.sum(column(aggregate_col)).label(
                aggregate_col_alias or f"sum_{aggregate_col}"
            )
        elif aggregate_func == "avg":
            agg_col = func.avg(column(aggregate_col)).label(
                aggregate_col_alias or f"avg_{aggregate_col}"
            )
        elif aggregate_func == "min":
            agg_col = func.min(column(aggregate_col)).label(
                aggregate_col_alias or f"min_{aggregate_col}"
            )
        elif aggregate_func == "max":
            agg_col = func.max(column(aggregate_col)).label(
                aggregate_col_alias or f"max_{aggregate_col}"
            )
        else:
            raise ValidationError(f"Unsupported aggregate function: {aggregate_func}")

        query_builder.add_column(agg_col)

        # Add dimension column if provided
        if dimension_col:
            query_builder.add_column(column(dimension_col))
            query_builder.group_cols_by(xaxis, dimension_col)
        else:
            query_builder.group_cols_by(xaxis)

        # Set table
        query_builder.fetch_from(table_name, schema_name)

        # Set limit and offset with performance optimization
        optimized_limit = min(limit, 1000) if limit else 1000
        query_builder.limit_rows(optimized_limit).offset_rows(offset)

        query = query_builder.build()
        return self.warehouse.execute(query)

    def _transform_data_for_chart(
        self,
        query_result: List[Dict],
        chart_type: str,
        computation_type: str,
        xaxis: str,
        yaxis: str = None,
        aggregate_col_alias: str = None,
        dimension_col: str = None,
    ) -> Dict:
        """Transform query result for chart visualization"""
        if not query_result:
            return {"data": [], "x-axis": [], "y-axis": []}

        data = []
        x_axis_data = []
        y_axis_data = []

        for row in query_result:
            if computation_type == "raw":
                x_value = row.get("x")
                y_value = row.get("y")
            else:
                x_value = row.get(xaxis)
                y_value = row.get(aggregate_col_alias or "count")

            x_axis_data.append(x_value)
            y_axis_data.append(y_value)

            data.append(
                {
                    "x": x_value,
                    "y": y_value,
                    "name": str(x_value),
                    "value": y_value,
                    **row,  # Include all original fields
                }
            )

        return {
            "data": data,
            "x-axis": x_axis_data,
            "y-axis": y_axis_data,
            "chart_type": chart_type,
            "computation_type": computation_type,
        }

    def _generate_echarts_config(
        self,
        chart_data: Dict,
        chart_type: str,
        xaxis: str = None,
        yaxis: str = None,
        aggregate_col_alias: str = None,
    ) -> Dict:
        """Generate ECharts configuration"""
        base_config = {
            "tooltip": {
                "trigger": "axis" if chart_type in ["line", "bar", "area"] else "item",
                "axisPointer": {"type": "cross" if chart_type in ["line", "area"] else "shadow"},
            },
            "legend": {"data": []},
            "grid": {"left": "3%", "right": "4%", "bottom": "3%", "containLabel": True},
            "animation": True,
            "animationDuration": 1000,
        }

        if chart_type in ["bar", "line", "area"]:
            base_config.update(
                {
                    "xAxis": {
                        "type": "category",
                        "data": chart_data.get("x-axis", []),
                        "name": xaxis or "X-Axis",
                        "nameLocation": "middle",
                        "nameGap": 30,
                    },
                    "yAxis": {
                        "type": "value",
                        "name": aggregate_col_alias or yaxis or "Y-Axis",
                        "nameLocation": "middle",
                        "nameGap": 50,
                    },
                    "series": [
                        {
                            "name": aggregate_col_alias or yaxis or "Series",
                            "type": chart_type,
                            "data": chart_data.get("y-axis", []),
                            "emphasis": {"focus": "series"},
                        }
                    ],
                }
            )

        elif chart_type == "pie":
            base_config.update(
                {
                    "series": [
                        {
                            "name": aggregate_col_alias or "Value",
                            "type": "pie",
                            "radius": "50%",
                            "data": [
                                {"value": item["y"], "name": item["name"]}
                                for item in chart_data.get("data", [])
                            ],
                            "emphasis": {
                                "itemStyle": {
                                    "shadowBlur": 10,
                                    "shadowOffsetX": 0,
                                    "shadowColor": "rgba(0, 0, 0, 0.5)",
                                }
                            },
                        }
                    ]
                }
            )

        elif chart_type == "scatter":
            base_config.update(
                {
                    "xAxis": {
                        "type": "value",
                        "name": xaxis or "X-Axis",
                        "nameLocation": "middle",
                        "nameGap": 30,
                    },
                    "yAxis": {
                        "type": "value",
                        "name": aggregate_col_alias or yaxis or "Y-Axis",
                        "nameLocation": "middle",
                        "nameGap": 50,
                    },
                    "series": [
                        {
                            "name": aggregate_col_alias or "Series",
                            "type": "scatter",
                            "data": [[item["x"], item["y"]] for item in chart_data.get("data", [])],
                            "emphasis": {"focus": "series"},
                        }
                    ],
                }
            )

        # Add color palette
        base_config["color"] = [
            "#5470c6",
            "#91cc75",
            "#fac858",
            "#ee6666",
            "#73c0de",
            "#3ba272",
            "#fc8452",
            "#9a60b4",
            "#ea7ccc",
        ]

        return base_config

    def _generate_cache_key(self, *args) -> str:
        """Generate cache key for chart data"""
        key_string = "|".join(str(arg) for arg in args)
        return hashlib.md5(key_string.encode()).hexdigest()

    def _get_cached_data(self, cache_key: str) -> Optional[Dict]:
        """Get cached chart data"""
        try:
            snapshot = ChartSnapshot.objects.get(data_hash=cache_key, expires_at__gt=timezone.now())
            return {
                "chart_config": snapshot.chart_config,
                "raw_data": snapshot.raw_data,
                "metadata": {
                    "cached": True,
                    "cached_at": snapshot.created_at.isoformat(),
                },
            }
        except ChartSnapshot.DoesNotExist:
            return None

    def _cache_data(self, cache_key: str, data: Dict):
        """Cache chart data with intelligent expiration"""
        try:
            # Determine cache duration based on data characteristics
            record_count = len(data.get("raw_data", {}).get("data", []))

            # Longer cache for smaller datasets (they change less frequently)
            if record_count < 100:
                cache_duration = timedelta(hours=4)
            elif record_count < 1000:
                cache_duration = timedelta(hours=2)
            else:
                cache_duration = timedelta(hours=1)

            expires_at = timezone.now() + cache_duration

            # Add cache metadata
            enhanced_data = {
                **data,
                "cache_metadata": {
                    "cached_at": timezone.now().isoformat(),
                    "cache_duration_hours": cache_duration.total_seconds() / 3600,
                    "record_count": record_count,
                    "org_id": self.org.id,
                },
            }

            ChartSnapshot.objects.create(
                chart=None,  # General cache, not tied to specific chart
                data_hash=cache_key,
                chart_config=enhanced_data["chart_config"],
                raw_data=enhanced_data["raw_data"],
                expires_at=expires_at,
            )

            logger.info(
                f"Cached chart data for org {self.org.slug}, key: {cache_key[:8]}..., duration: {cache_duration}"
            )

        except Exception as e:
            logger.error(f"Failed to cache chart data: {e}")

    def _test_connection_health(self):
        """Test warehouse connection health"""
        try:
            if self.warehouse:
                # Simple health check query
                from sqlalchemy import text

                result = self.warehouse.execute(text("SELECT 1"))
                if not result:
                    logger.warning(f"Warehouse health check failed for org {self.org.slug}")
        except Exception as e:
            logger.error(f"Warehouse health check error for org {self.org.slug}: {str(e)}")

    def _optimize_query_performance(self, query, limit: int = 100):
        """Optimize query performance with limits and indexes"""
        if limit and limit > 1000:
            logger.warning(f"Large query limit ({limit}) requested for org {self.org.slug}")
            limit = 1000  # Cap at 1000 for performance

        return query.limit(limit) if hasattr(query, "limit") else query

    def _get_connection_pool_stats(self):
        """Get database connection pool statistics"""
        try:
            if self.warehouse and hasattr(self.warehouse, "engine"):
                pool = self.warehouse.engine.pool
                return {
                    "pool_size": pool.size(),
                    "checked_in": pool.checkedin(),
                    "checked_out": pool.checkedout(),
                    "overflow": pool.overflow(),
                    "invalid": pool.invalid(),
                }
        except Exception as e:
            logger.error(f"Failed to get connection pool stats: {str(e)}")
            return None

    def invalidate_cache_for_table(self, schema_name: str, table_name: str):
        """Invalidate cache for all charts using a specific table"""
        try:
            # Find all cache entries that might be related to this table
            # This is a simplified approach - in production, you might want more sophisticated cache tagging
            ChartSnapshot.objects.filter(data_hash__contains=f"{schema_name}_{table_name}").delete()

            logger.info(f"Invalidated cache for table {schema_name}.{table_name}")
        except Exception as e:
            logger.error(f"Failed to invalidate cache for table {schema_name}.{table_name}: {e}")

    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics for monitoring"""
        try:
            total_snapshots = ChartSnapshot.objects.count()
            expired_snapshots = ChartSnapshot.objects.filter(expires_at__lt=timezone.now()).count()
            org_snapshots = ChartSnapshot.objects.filter(
                data_hash__contains=str(self.org.id)
            ).count()

            return {
                "total_snapshots": total_snapshots,
                "expired_snapshots": expired_snapshots,
                "org_snapshots": org_snapshots,
                "cache_hit_potential": (total_snapshots - expired_snapshots) / total_snapshots
                if total_snapshots > 0
                else 0,
            }
        except Exception as e:
            logger.error(f"Failed to get cache stats: {e}")
            return {}

    @staticmethod
    def clean_expired_snapshots():
        """Clean up expired chart snapshots"""
        try:
            deleted_count = ChartSnapshot.objects.filter(expires_at__lt=timezone.now()).delete()[0]
            logger.info(f"Cleaned up {deleted_count} expired chart snapshots")
        except Exception as e:
            logger.error(f"Failed to clean expired snapshots: {e}")

    @staticmethod
    def clean_old_snapshots(days_old: int = 7):
        """Clean up old chart snapshots beyond a certain age"""
        try:
            cutoff_date = timezone.now() - timedelta(days=days_old)
            deleted_count = ChartSnapshot.objects.filter(created_at__lt=cutoff_date).delete()[0]
            logger.info(
                f"Cleaned up {deleted_count} old chart snapshots (older than {days_old} days)"
            )
        except Exception as e:
            logger.error(f"Failed to clean old snapshots: {e}")

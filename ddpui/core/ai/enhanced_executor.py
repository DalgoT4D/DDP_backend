"""
Enhanced Dynamic Query Execution Engine (Layer 3)

Advanced query execution with caching, optimization, monitoring, and smart fallbacks.
"""

import time
import hashlib
import json
from typing import Dict, List, Any, Optional, Tuple, Union
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import threading
from collections import defaultdict

from django.utils import timezone
from django.core.cache import cache
from sqlalchemy import text

from ddpui.core.ai.query_executor import (
    DynamicQueryExecutor,
    QueryExecutionResult,
    ExecutionSafetyLimits,
)
from ddpui.core.ai.query_generator import QueryPlan, NaturalLanguageQueryService
from ddpui.core.ai.data_intelligence import DataIntelligenceService
from ddpui.models.org import Org
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui.core.ai.enhanced_executor")


@dataclass
class QueryCacheInfo:
    """Information about cached query results"""

    cache_key: str
    cached_at: datetime
    hit_count: int = 0
    org_id: int = 0
    question_hash: str = ""
    ttl_seconds: int = 3600  # 1 hour default


@dataclass
class QueryOptimization:
    """Query optimization suggestions and applied changes"""

    original_complexity: int
    optimized_complexity: int
    optimizations_applied: List[str] = field(default_factory=list)
    performance_gain_estimate: float = 0.0
    cache_eligible: bool = False


class EnhancedDynamicExecutor(DynamicQueryExecutor):
    """
    Enhanced version of the Dynamic Query Executor with advanced features:
    - Intelligent caching
    - Query optimization
    - Performance monitoring
    - Smart fallbacks
    - Adaptive rate limiting
    """

    def __init__(self):
        super().__init__()
        self.logger = CustomLogger("EnhancedDynamicExecutor")

        # Enhanced monitoring
        self._performance_metrics = defaultdict(list)
        self._query_patterns = defaultdict(int)
        self._optimization_stats = defaultdict(int)

        # Cache management
        self._cache_stats = {"hits": 0, "misses": 0, "evictions": 0}

        # Thread safety
        self._metrics_lock = threading.Lock()

        # Enhanced safety limits with adaptive adjustments
        self.adaptive_limits = {
            "base_limits": ExecutionSafetyLimits(),
            "current_multiplier": 1.0,
            "last_adjustment": timezone.now(),
        }

    def execute_natural_language_query_enhanced(
        self,
        question: str,
        org: Org,
        dashboard_id: Optional[int] = None,
        user_context: Optional[Dict[str, Any]] = None,
        enable_caching: bool = True,
        enable_optimization: bool = True,
    ) -> Tuple[QueryExecutionResult, Dict[str, Any]]:
        """
        Enhanced execution with caching, optimization, and detailed analytics.

        Returns:
            Tuple of (execution_result, analytics_data)
        """
        execution_start = time.time()
        analytics = {
            "cache_used": False,
            "optimizations_applied": [],
            "performance_metrics": {},
            "recommendations": [],
        }

        try:
            # Generate cache key for this question
            cache_key = self._generate_cache_key(question, org.id, dashboard_id)

            # Check cache first
            if enable_caching:
                cached_result = self._get_cached_result(cache_key, org.id)
                if cached_result:
                    analytics["cache_used"] = True
                    analytics["performance_metrics"]["cache_hit"] = True
                    analytics["performance_metrics"]["total_time_ms"] = int(
                        (time.time() - execution_start) * 1000
                    )
                    return cached_result, analytics

            # Adaptive rate limiting check
            rate_limit_result = self._adaptive_rate_limit_check(org.id, question)
            if not rate_limit_result[0]:
                result = QueryExecutionResult(success=False, error_message=rate_limit_result[1])
                return result, analytics

            # Execute with standard flow but capture optimization opportunities
            result = self.execute_natural_language_query(
                question=question, org=org, dashboard_id=dashboard_id, user_context=user_context
            )

            # Apply post-execution optimizations and analysis
            if result.success and enable_optimization:
                optimization_info = self._analyze_and_optimize_execution(result, question, org)
                analytics["optimizations_applied"] = optimization_info.optimizations_applied
                analytics["performance_metrics"]["optimization"] = {
                    "complexity_reduction": optimization_info.original_complexity
                    - optimization_info.optimized_complexity,
                    "performance_gain": optimization_info.performance_gain_estimate,
                    "cache_eligible": optimization_info.cache_eligible,
                }

                # Cache successful results if eligible
                if enable_caching and optimization_info.cache_eligible:
                    self._cache_result(cache_key, result, org.id, question)
                    analytics["performance_metrics"]["cached"] = True

            # Record performance metrics
            self._record_performance_metrics(question, result, org.id, analytics)

            # Generate recommendations
            analytics["recommendations"] = self._generate_recommendations(
                result, question, org, analytics
            )

            return result, analytics

        except Exception as e:
            self.logger.error(f"Error in enhanced execution: {e}")
            error_result = QueryExecutionResult(
                success=False,
                error_message=f"Enhanced execution failed: {str(e)}",
                execution_time_ms=int((time.time() - execution_start) * 1000),
            )
            return error_result, analytics

    def _generate_cache_key(self, question: str, org_id: int, dashboard_id: Optional[int]) -> str:
        """Generate a unique cache key for the query"""
        key_components = [
            question.lower().strip(),
            str(org_id),
            str(dashboard_id) if dashboard_id else "no_dashboard",
            "v2",  # Cache version for invalidation
        ]

        key_string = "|".join(key_components)
        return f"nlq_cache:{hashlib.md5(key_string.encode()).hexdigest()}"

    def _get_cached_result(self, cache_key: str, org_id: int) -> Optional[QueryExecutionResult]:
        """Get cached query result if available and valid"""
        try:
            cached_data = cache.get(cache_key)
            if cached_data:
                # Update cache stats
                with self._metrics_lock:
                    self._cache_stats["hits"] += 1

                # Deserialize and return result
                result_dict = json.loads(cached_data["result"])
                result = QueryExecutionResult(**result_dict)

                # Update cache info
                cache_info = cached_data.get("cache_info", {})
                cache_info["hit_count"] = cache_info.get("hit_count", 0) + 1

                self.logger.info(f"Cache hit for key {cache_key}, org {org_id}")
                return result
            else:
                with self._metrics_lock:
                    self._cache_stats["misses"] += 1
                return None

        except Exception as e:
            self.logger.error(f"Error retrieving cached result: {e}")
            return None

    def _cache_result(
        self, cache_key: str, result: QueryExecutionResult, org_id: int, question: str
    ):
        """Cache a successful query result"""
        try:
            # Only cache successful results with reasonable data size
            if (
                result.success
                and result.row_count <= 500
                and result.execution_time_ms <= 10000  # Don't cache very large results
            ):  # Don't cache slow queries
                # Serialize result (excluding non-serializable fields)
                cache_data = {
                    "result": json.dumps(
                        {
                            "success": result.success,
                            "data": result.data,
                            "columns": result.columns,
                            "row_count": result.row_count,
                            "execution_time_ms": result.execution_time_ms,
                            "warnings": result.warnings,
                            "executed_at": result.executed_at.isoformat(),
                            "source_tables": result.source_tables,
                        }
                    ),
                    "cache_info": QueryCacheInfo(
                        cache_key=cache_key,
                        cached_at=timezone.now(),
                        org_id=org_id,
                        question_hash=hashlib.md5(question.encode()).hexdigest(),
                    ).__dict__,
                }

                # Cache with TTL based on result characteristics
                ttl = self._calculate_cache_ttl(result)
                cache.set(cache_key, cache_data, timeout=ttl)

                self.logger.info(f"Cached result for key {cache_key}, TTL {ttl}s")

        except Exception as e:
            self.logger.error(f"Error caching result: {e}")

    def _calculate_cache_ttl(self, result: QueryExecutionResult) -> int:
        """Calculate appropriate cache TTL based on result characteristics"""
        base_ttl = 3600  # 1 hour base

        # Longer cache for aggregated results
        if result.row_count <= 50:
            base_ttl *= 2  # 2 hours for summary data

        # Shorter cache for large result sets
        if result.row_count > 200:
            base_ttl //= 2  # 30 minutes for large data

        # Shorter cache for slow queries (data might be changing)
        if result.execution_time_ms > 5000:
            base_ttl //= 2

        return max(base_ttl, 300)  # Minimum 5 minutes

    def _analyze_and_optimize_execution(
        self, result: QueryExecutionResult, question: str, org: Org
    ) -> QueryOptimization:
        """Analyze execution and identify optimization opportunities"""
        optimization = QueryOptimization(
            original_complexity=result.query_plan.validation_result.complexity_score
            if result.query_plan and result.query_plan.validation_result
            else 5,
            optimized_complexity=0,
        )

        try:
            # Analyze query patterns
            self._analyze_query_patterns(question, result)

            # Check if result is cache-eligible
            optimization.cache_eligible = (
                result.success
                and result.execution_time_ms >= 1000
                and result.row_count <= 1000  # Queries over 1 second benefit from caching
                and "error"  # Don't cache very large results
                not in question.lower()  # Don't cache error-testing queries
            )

            # Estimate performance gains
            if optimization.cache_eligible:
                optimization.optimizations_applied.append("Cache eligible for future requests")
                optimization.performance_gain_estimate = min(
                    result.execution_time_ms * 0.95, 5000
                )  # Up to 95% time savings

            # Check for query optimization opportunities
            if result.query_plan and result.query_plan.generated_sql:
                sql_optimizations = self._analyze_sql_optimization_opportunities(
                    result.query_plan.generated_sql
                )
                optimization.optimizations_applied.extend(sql_optimizations)

            # Calculate optimized complexity
            complexity_reduction = len(optimization.optimizations_applied) * 0.5
            optimization.optimized_complexity = max(
                optimization.original_complexity - complexity_reduction, 1
            )

        except Exception as e:
            self.logger.error(f"Error in optimization analysis: {e}")

        return optimization

    def _analyze_sql_optimization_opportunities(self, sql: str) -> List[str]:
        """Analyze SQL for optimization opportunities"""
        optimizations = []
        sql_upper = sql.upper()

        # Check for missing indexes (basic heuristics)
        if "WHERE" in sql_upper and "ORDER BY" in sql_upper:
            optimizations.append("Consider composite index on WHERE and ORDER BY columns")

        # Check for potential query rewriting
        if "GROUP BY" in sql_upper and "HAVING" in sql_upper:
            optimizations.append("HAVING clause could potentially be optimized with WHERE")

        # Check for aggregation efficiency
        if sql.count("SUM(") > 1 or sql.count("COUNT(") > 1:
            optimizations.append("Multiple aggregations could be combined")

        return optimizations

    def _analyze_query_patterns(self, question: str, result: QueryExecutionResult):
        """Analyze query patterns for future optimizations"""
        with self._metrics_lock:
            # Track question patterns
            question_pattern = self._extract_question_pattern(question)
            self._query_patterns[question_pattern] += 1

            # Track performance characteristics
            if result.success:
                self._performance_metrics["execution_times"].append(result.execution_time_ms)
                self._performance_metrics["row_counts"].append(result.row_count)

            # Keep metrics manageable
            for metric_list in self._performance_metrics.values():
                if isinstance(metric_list, list) and len(metric_list) > 1000:
                    metric_list[:] = metric_list[-500:]  # Keep last 500 entries

    def _extract_question_pattern(self, question: str) -> str:
        """Extract pattern from question for analysis"""
        question_lower = question.lower().strip()

        # Common query patterns
        if any(word in question_lower for word in ["how many", "count", "total"]):
            return "count_aggregation"
        elif any(word in question_lower for word in ["average", "avg", "mean"]):
            return "average_calculation"
        elif any(word in question_lower for word in ["top", "highest", "maximum", "best"]):
            return "top_n_ranking"
        elif any(word in question_lower for word in ["show", "list", "display"]):
            return "data_listing"
        elif any(word in question_lower for word in ["compare", "vs", "versus", "difference"]):
            return "comparison_analysis"
        else:
            return "general_query"

    def _adaptive_rate_limit_check(self, org_id: int, question: str) -> Tuple[bool, str]:
        """Adaptive rate limiting based on query complexity and org behavior"""
        base_check = self._check_rate_limits(org_id)

        if not base_check[0]:
            return base_check

        # Check for query complexity-based additional limits
        question_pattern = self._extract_question_pattern(question)

        # More restrictive limits for complex queries
        if question_pattern in ["comparison_analysis"] and len(question) > 100:
            # Check if this org has been making many complex queries
            recent_complex_queries = self._count_recent_complex_queries(org_id)
            if recent_complex_queries > 5:  # Max 5 complex queries per 10 minutes
                return (
                    False,
                    "Rate limit for complex queries exceeded. Please wait before making more complex analysis requests.",
                )

        return True, ""

    def _count_recent_complex_queries(self, org_id: int) -> int:
        """Count complex queries in recent history"""
        if org_id not in self._execution_history:
            return 0

        recent_time = timezone.now() - timedelta(minutes=10)
        complex_count = 0

        for execution in self._execution_history:
            if (
                execution.get("org_id") == org_id
                and execution.get("execution_success")
                and execution.get("ai_confidence", 0) < 0.7
            ):  # Low confidence = complex
                try:
                    executed_at = datetime.fromisoformat(execution.get("executed_at", ""))
                    if executed_at >= recent_time:
                        complex_count += 1
                except:
                    continue

        return complex_count

    def _record_performance_metrics(
        self, question: str, result: QueryExecutionResult, org_id: int, analytics: Dict[str, Any]
    ):
        """Record detailed performance metrics"""
        with self._metrics_lock:
            pattern = self._extract_question_pattern(question)

            metric_entry = {
                "timestamp": timezone.now().isoformat(),
                "org_id": org_id,
                "question_pattern": pattern,
                "success": result.success,
                "execution_time_ms": result.execution_time_ms,
                "row_count": result.row_count,
                "cache_used": analytics.get("cache_used", False),
                "optimizations_count": len(analytics.get("optimizations_applied", [])),
            }

            self._performance_metrics["detailed_metrics"].append(metric_entry)

    def _generate_recommendations(
        self, result: QueryExecutionResult, question: str, org: Org, analytics: Dict[str, Any]
    ) -> List[str]:
        """Generate intelligent recommendations based on execution analysis"""
        recommendations = []

        try:
            # Performance-based recommendations
            if result.success and result.execution_time_ms > 5000:
                recommendations.append(
                    "Consider adding database indexes for faster query performance"
                )

            if result.row_count > 500:
                recommendations.append(
                    "Large result set - consider adding filters to narrow down results"
                )

            # Pattern-based recommendations
            question_pattern = self._extract_question_pattern(question)

            if question_pattern == "count_aggregation" and result.row_count == 1:
                recommendations.append(
                    "This counting query could benefit from caching for repeated requests"
                )

            if question_pattern == "top_n_ranking" and not any(
                "limit" in opt.lower() for opt in analytics.get("optimizations_applied", [])
            ):
                recommendations.append(
                    "Consider using TOP/LIMIT clauses for ranking queries to improve performance"
                )

            # Cache-based recommendations
            if not analytics.get("cache_used") and result.execution_time_ms > 2000:
                recommendations.append("This query type could benefit from result caching")

            # Data quality recommendations
            if result.success and result.row_count == 0:
                recommendations.append(
                    "No data found - check if filters are too restrictive or data exists for the requested criteria"
                )

        except Exception as e:
            self.logger.error(f"Error generating recommendations: {e}")

        return recommendations[:3]  # Limit to top 3 recommendations

    def get_enhanced_execution_stats(self, org_id: Optional[int] = None) -> Dict[str, Any]:
        """Get comprehensive execution statistics including performance insights"""
        base_stats = self.get_execution_stats(org_id)

        with self._metrics_lock:
            enhanced_stats = {
                **base_stats,
                "cache_performance": self._cache_stats.copy(),
                "query_patterns": dict(self._query_patterns),
                "performance_insights": self._calculate_performance_insights(),
                "optimization_stats": dict(self._optimization_stats),
            }

        return enhanced_stats

    def _calculate_performance_insights(self) -> Dict[str, Any]:
        """Calculate performance insights from collected metrics"""
        insights = {}

        try:
            execution_times = self._performance_metrics.get("execution_times", [])
            if execution_times:
                insights["avg_execution_time_ms"] = sum(execution_times) / len(execution_times)
                insights["p95_execution_time_ms"] = (
                    sorted(execution_times)[int(len(execution_times) * 0.95)]
                    if len(execution_times) > 20
                    else max(execution_times)
                )

            row_counts = self._performance_metrics.get("row_counts", [])
            if row_counts:
                insights["avg_result_size"] = sum(row_counts) / len(row_counts)
                insights["max_result_size"] = max(row_counts)

            # Cache efficiency
            total_cache_requests = self._cache_stats["hits"] + self._cache_stats["misses"]
            if total_cache_requests > 0:
                insights["cache_hit_ratio"] = self._cache_stats["hits"] / total_cache_requests

        except Exception as e:
            self.logger.error(f"Error calculating performance insights: {e}")
            insights["calculation_error"] = str(e)

        return insights

    def clear_org_cache(self, org_id: int) -> Dict[str, int]:
        """Clear all cached results for an organization"""
        cleared_count = 0

        try:
            # This is a simplified version - in production you'd want a more efficient approach
            # such as using cache key patterns or a separate cache index
            cache_keys = []  # Would need to track cache keys per org

            for key in cache_keys:
                cache.delete(key)
                cleared_count += 1

            self.logger.info(f"Cleared {cleared_count} cache entries for org {org_id}")

        except Exception as e:
            self.logger.error(f"Error clearing cache for org {org_id}: {e}")

        return {"cleared_entries": cleared_count, "org_id": org_id, "timestamp": int(time.time())}

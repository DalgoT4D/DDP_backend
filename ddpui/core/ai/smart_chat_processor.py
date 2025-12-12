"""
Smart Chat Processor for Layer 4: Enhanced Dashboard Chat API Integration

Intelligently detects when chat messages contain data queries and routes them
through the natural language query system for enhanced responses.
"""

import re
import json
from typing import Dict, List, Any, Optional, Tuple, Union
from dataclasses import dataclass
from enum import Enum

from ddpui.core.ai.enhanced_executor import EnhancedDynamicExecutor
from ddpui.core.ai.interfaces import AIMessage
from ddpui.core.ai.factory import get_default_ai_provider
from ddpui.models.org import Org
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui.core.ai.smart_chat_processor")


class MessageIntent(Enum):
    """Classification of chat message intents"""

    DATA_QUERY = "data_query"  # Specific data questions that can be answered with SQL
    DASHBOARD_EXPLANATION = "dashboard_explanation"  # Questions about dashboard structure
    GENERAL_CONVERSATION = "general_conversation"  # General chat
    TECHNICAL_HELP = "technical_help"  # Help with using the platform
    BUSINESS_INSIGHT = "business_insight"  # High-level business analysis requests


@dataclass
class MessageAnalysis:
    """Analysis result of a chat message"""

    intent: MessageIntent
    confidence: float  # 0.0 - 1.0
    data_query_detected: bool
    suggested_sql_approach: str = ""
    key_entities: List[str] = None
    temporal_context: Optional[str] = None  # e.g., "2021", "last year", "this month"
    geographic_context: Optional[str] = None  # e.g., "Maharashtra", "India"
    metric_context: List[str] = None  # e.g., ["revenue", "count", "average"]

    def __post_init__(self):
        if self.key_entities is None:
            self.key_entities = []
        if self.metric_context is None:
            self.metric_context = []


@dataclass
class EnhancedChatResponse:
    """Enhanced chat response with optional data query results"""

    content: str
    intent_detected: MessageIntent
    data_results: Optional[Dict[str, Any]] = None
    query_executed: bool = False
    fallback_used: bool = False
    confidence_score: float = 0.0
    recommendations: List[str] = None

    def __post_init__(self):
        if self.recommendations is None:
            self.recommendations = []


class SmartChatProcessor:
    """
    Intelligent chat processor that enhances dashboard chat with data query capabilities.

    Analyzes incoming messages to detect data queries and automatically executes
    them through the natural language query system when appropriate.
    """

    def __init__(self):
        self.logger = CustomLogger("SmartChatProcessor")
        self.executor = EnhancedDynamicExecutor()
        self.ai_provider = None

        # Intent detection patterns
        self.data_query_patterns = [
            r"\b(?:how many|count|total|sum of|average|avg|maximum|max|minimum|min)\b",
            r"\b(?:show me|list|display|get|find)\s+.*(?:data|records|rows|entries)\b",
            r"\b(?:top|bottom|highest|lowest|best|worst)\s+\d*\s*\b",
            r"\b(?:in|for|during|from|between)\s+\d{4}\b",  # Years
            r"\b(?:revenue|sales|profit|cost|price|amount|value)\b",
            r"\b(?:students|customers|users|clients|products|orders|transactions)\b",
            r"\b(?:where|with|having|group by|order by)\b",
            r"\bcompare.*(?:to|with|vs|versus)\b",
        ]

        self.temporal_patterns = [
            r"\b(\d{4})\b",  # Years like 2021, 2023
            r"\b(last|this|next)\s+(year|month|week|quarter)\b",
            r"\b(january|february|march|april|may|june|july|august|september|october|november|december)\b",
            r"\b(Q1|Q2|Q3|Q4)\s*(\d{4})?\b",
        ]

        self.geographic_patterns = [
            r"\bin\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\b",  # "in Maharashtra", "in New York"
            r"\bfrom\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\b",
        ]

        self.metric_patterns = [
            r"\b(revenue|sales|profit|income|earnings|cost|expense|price|amount|value|fee|charge)\b",
            r"\b(count|total|sum|average|mean|median|percentage|ratio|rate)\b",
            r"\b(students|enrollment|attendance|performance|score|grade)\b",
            r"\b(customers|users|clients|visitors|subscribers|members)\b",
        ]

    def analyze_message(
        self, message: str, dashboard_context: Optional[Dict[str, Any]] = None
    ) -> MessageAnalysis:
        """
        Analyze a chat message to determine intent and extract key information.

        Args:
            message: The user's chat message
            dashboard_context: Optional context about the current dashboard

        Returns:
            MessageAnalysis with detected intent and extracted entities
        """
        self.logger.info(f"Analyzing message: {message[:100]}...")

        message_lower = message.lower().strip()

        # Check for data query patterns
        data_query_matches = sum(
            1
            for pattern in self.data_query_patterns
            if re.search(pattern, message_lower, re.IGNORECASE)
        )

        data_query_confidence = min(data_query_matches / len(self.data_query_patterns), 1.0)

        # Extract entities
        temporal_context = self._extract_temporal_context(message)
        geographic_context = self._extract_geographic_context(message)
        metric_context = self._extract_metric_context(message)
        key_entities = self._extract_key_entities(message, dashboard_context)

        # Determine intent based on patterns and context
        intent, confidence = self._classify_intent(
            message_lower, data_query_confidence, key_entities, dashboard_context
        )

        return MessageAnalysis(
            intent=intent,
            confidence=confidence,
            data_query_detected=data_query_confidence > 0.3,
            key_entities=key_entities,
            temporal_context=temporal_context,
            geographic_context=geographic_context,
            metric_context=metric_context,
            suggested_sql_approach=self._suggest_sql_approach(intent, key_entities, metric_context),
        )

    def process_enhanced_chat_message(
        self,
        message: str,
        org: Org,
        dashboard_id: Optional[int] = None,
        dashboard_context: Optional[Dict[str, Any]] = None,
        user_context: Optional[Dict[str, Any]] = None,
        enable_data_query: bool = True,
    ) -> EnhancedChatResponse:
        """
        Process a chat message with enhanced capabilities including automatic data querying.

        Args:
            message: User's chat message
            org: Organization context
            dashboard_id: Current dashboard ID
            dashboard_context: Dashboard context for AI
            user_context: Additional user context
            enable_data_query: Whether to execute data queries automatically

        Returns:
            EnhancedChatResponse with content and optional data results
        """
        try:
            # First, attempt a direct answer from the already-available dashboard context data.
            # This can handle simple fact-style questions (e.g. "What is the population of Karnataka")
            # without relying on NLQ/SQL generation.
            direct_answer = self._attempt_direct_lookup_from_context(message, dashboard_context)
            if direct_answer:
                return direct_answer

            # Analyze the message intent
            analysis = self.analyze_message(message, dashboard_context)

            self.logger.info(
                f"Message intent: {analysis.intent.value}, confidence: {analysis.confidence}"
            )

            # Route based on detected intent
            if (
                analysis.intent == MessageIntent.DATA_QUERY
                and analysis.data_query_detected
                and enable_data_query
                and analysis.confidence > 0.5
            ):
                return self._handle_data_query_message(
                    message, analysis, org, dashboard_id, dashboard_context, user_context
                )

            elif analysis.intent == MessageIntent.DASHBOARD_EXPLANATION:
                return self._handle_dashboard_explanation_message(
                    message, analysis, dashboard_context
                )

            else:
                return self._handle_general_chat_message(message, analysis, dashboard_context)

        except Exception as e:
            self.logger.error(f"Error processing enhanced chat message: {e}")
            return EnhancedChatResponse(
                content=f"I encountered an error processing your message. Please try rephrasing your question.",
                intent_detected=MessageIntent.GENERAL_CONVERSATION,
                fallback_used=True,
            )

    def _handle_data_query_message(
        self,
        message: str,
        analysis: MessageAnalysis,
        org: Org,
        dashboard_id: Optional[int],
        dashboard_context: Optional[Dict[str, Any]],
        user_context: Optional[Dict[str, Any]],
    ) -> EnhancedChatResponse:
        """Handle messages that contain data queries"""
        try:
            # Execute the natural language query
            execution_result, analytics = self.executor.execute_natural_language_query_enhanced(
                question=message,
                org=org,
                dashboard_id=dashboard_id,
                user_context=user_context,
                enable_caching=True,
                enable_optimization=True,
            )

            if execution_result.success and execution_result.data:
                # Generate enhanced response with data insights
                response_content = self._generate_data_response_content(
                    message, execution_result, analysis, analytics
                )

                return EnhancedChatResponse(
                    content=response_content,
                    intent_detected=MessageIntent.DATA_QUERY,
                    data_results={
                        "query_results": execution_result.data,
                        "columns": execution_result.columns,
                        "row_count": execution_result.row_count,
                        "execution_time_ms": execution_result.execution_time_ms,
                        "query_explanation": execution_result.query_plan.explanation
                        if execution_result.query_plan
                        else "",
                        "analytics": analytics,
                    },
                    query_executed=True,
                    confidence_score=execution_result.query_plan.confidence_score
                    if execution_result.query_plan
                    else 0.8,
                    recommendations=analytics.get("recommendations", []),
                )
            else:
                # Query failed - provide helpful fallback
                return self._handle_failed_data_query(
                    message, execution_result, analysis, dashboard_context
                )

        except Exception as e:
            self.logger.error(f"Error handling data query message: {e}")
            return self._handle_failed_data_query(message, None, analysis, dashboard_context)

    def _handle_dashboard_explanation_message(
        self, message: str, analysis: MessageAnalysis, dashboard_context: Optional[Dict[str, Any]]
    ) -> EnhancedChatResponse:
        """Handle messages asking for dashboard explanations"""
        try:
            # If we have structured dashboard context, build a deterministic,
            # data-grounded explanation instead of asking the model to infer one.
            if dashboard_context:
                summary_text = self._build_dashboard_summary_from_context(dashboard_context)
                return EnhancedChatResponse(
                    content=summary_text,
                    intent_detected=MessageIntent.DASHBOARD_EXPLANATION,
                    confidence_score=0.95,
                )

            # Fallback: use standard AI chat with high-level instructions
            if not self.ai_provider:
                self.ai_provider = get_default_ai_provider()

            system_prompt = self._build_dashboard_explanation_prompt(dashboard_context, analysis)

            ai_messages = [
                AIMessage(role="system", content=system_prompt),
                AIMessage(role="user", content=message),
            ]

            response = self.ai_provider.chat_completion(
                messages=ai_messages, temperature=0.3, max_tokens=600
            )

            return EnhancedChatResponse(
                content=response.content,
                intent_detected=MessageIntent.DASHBOARD_EXPLANATION,
                confidence_score=0.8,
            )

        except Exception as e:
            self.logger.error(f"Error handling dashboard explanation: {e}")

            # Always return a useful, data-grounded summary even on error
            fallback_text = "This dashboard shows multiple charts summarizing your key metrics."
            try:
                if dashboard_context and isinstance(dashboard_context, dict):
                    fallback_text = self._build_dashboard_summary_from_context(dashboard_context)
            except Exception as inner_e:
                self.logger.error(f"Fallback dashboard summary failed: {inner_e}")

            return EnhancedChatResponse(
                content=fallback_text,
                intent_detected=MessageIntent.DASHBOARD_EXPLANATION,
                fallback_used=True,
            )

    def _handle_general_chat_message(
        self, message: str, analysis: MessageAnalysis, dashboard_context: Optional[Dict[str, Any]]
    ) -> EnhancedChatResponse:
        """Handle general conversation messages"""
        try:
            # Use standard AI chat with light context
            if not self.ai_provider:
                self.ai_provider = get_default_ai_provider()

            system_prompt = self._build_general_chat_prompt(dashboard_context)

            ai_messages = [
                AIMessage(role="system", content=system_prompt),
                AIMessage(role="user", content=message),
            ]

            response = self.ai_provider.chat_completion(
                messages=ai_messages, temperature=0.7, max_tokens=800
            )

            # Add helpful suggestions for data queries if appropriate
            suggestions = self._generate_query_suggestions(analysis, dashboard_context)

            content = response.content
            if suggestions:
                content += "\n\n" + "\n".join(suggestions)

            return EnhancedChatResponse(
                content=content, intent_detected=analysis.intent, recommendations=suggestions
            )

        except Exception as e:
            self.logger.error(f"Error handling general chat: {e}")
            return EnhancedChatResponse(
                content="I'm here to help you analyze your dashboard and data. What would you like to explore?",
                intent_detected=MessageIntent.GENERAL_CONVERSATION,
                fallback_used=True,
            )

    def _handle_failed_data_query(
        self,
        message: str,
        execution_result: Optional[Any],
        analysis: MessageAnalysis,
        dashboard_context: Optional[Dict[str, Any]],
    ) -> EnhancedChatResponse:
        """Handle cases where data query execution failed"""
        # First, attempt a direct lookup from the already-available dashboard context data.
        # This allows answering simple questions like "What is the population of Karnataka"
        # directly from chart/sample_data rows without relying on NLQ generation.
        direct_answer = self._attempt_direct_lookup_from_context(message, dashboard_context)
        if direct_answer:
            return direct_answer

        error_msg = execution_result.error_message if execution_result else "Unknown error"

        # Provide helpful fallback based on the error type
        if "rate limit" in error_msg.lower():
            content = "I'm receiving many requests right now. Please wait a moment before asking data questions again."
        elif "no data" in error_msg.lower():
            content = f"I couldn't find data to answer '{message}'. This might be because:\n• The requested data doesn't exist in your dashboard\n• The question needs to be more specific\n• Data sharing needs to be enabled in settings"
        elif "validation failed" in error_msg.lower():
            content = f"I had trouble understanding how to query your data for '{message}'. Could you rephrase your question more specifically?"
        else:
            content = f"I couldn't execute a data query for your question. Let me help you understand what data is available in this dashboard instead."

        # Add suggestions for better queries
        suggestions = self._generate_query_improvement_suggestions(
            message, analysis, dashboard_context
        )
        if suggestions:
            content += "\n\nHere are some specific questions I can help with:\n" + "\n".join(
                f"• {s}" for s in suggestions
            )

        return EnhancedChatResponse(
            content=content,
            intent_detected=MessageIntent.DATA_QUERY,
            query_executed=False,
            fallback_used=True,
            recommendations=suggestions,
        )

    def _attempt_direct_lookup_from_context(
        self, message: str, dashboard_context: Optional[Dict[str, Any]]
    ) -> Optional[EnhancedChatResponse]:
        """
        Try to answer simple fact-style questions directly from dashboard sample data,
        without relying on NLQ/SQL generation.

        This is especially useful when chart data has already been materialized and
        the question refers to a specific entity present in that data (for example,
        "What is the population of Karnataka").
        """
        try:
            if not dashboard_context:
                return None

            charts = dashboard_context.get("charts", [])
            if not charts:
                return None

            # Extract candidate entity tokens from the question (e.g. "Karnataka")
            import re

            entity_candidates = set(
                re.findall(r"\b[A-Z][a-zA-Z]+\b", message)
            )  # simple proper-noun heuristic
            if not entity_candidates:
                return None

            # Preferred numeric column name hints
            numeric_preference_keywords = ["population", "count", "total", "value", "metric"]

            for chart in charts:
                sample_data = chart.get("sample_data") or {}
                rows = sample_data.get("rows") or []
                raw_columns = sample_data.get("columns") or []

                if not rows or not raw_columns:
                    continue

                # Normalise columns to a list of column names; sample_data.columns may
                # be a list of dicts like {"name": "<col>", "type": "..."}.
                column_names: List[str] = []
                for col in raw_columns:
                    if isinstance(col, dict) and col.get("name"):
                        column_names.append(str(col.get("name")))
                    else:
                        column_names.append(str(col))

                if not column_names:
                    continue

                # For each candidate entity (e.g. "Karnataka"), try to aggregate over all
                # matching rows in this chart.
                for ent in entity_candidates:
                    matching_rows = []

                    for row in rows:
                        for col_name in column_names:
                            cell_value = row.get(col_name)
                            if isinstance(cell_value, str) and ent.lower() in cell_value.lower():
                                matching_rows.append(row)
                                break

                    if not matching_rows:
                        continue

                    # Identify numeric columns that appear in the matching rows
                    numeric_cols: List[str] = []
                    for col_name in column_names:
                        if any(isinstance(r.get(col_name), (int, float)) for r in matching_rows):
                            numeric_cols.append(col_name)

                    if not numeric_cols:
                        continue

                    # Prefer numeric columns whose name suggests a metric like population/total
                    preferred_numeric_col = None
                    for col_name in numeric_cols:
                        lower_name = str(col_name).lower()
                        if any(kw in lower_name for kw in numeric_preference_keywords):
                            preferred_numeric_col = col_name
                            break

                    if not preferred_numeric_col:
                        # Fall back to the first numeric column if there is exactly one
                        if len(numeric_cols) == 1:
                            preferred_numeric_col = numeric_cols[0]
                        else:
                            continue  # ambiguous, skip

                    # Aggregate across all matching rows
                    total_value = 0.0
                    for r in matching_rows:
                        v = r.get(preferred_numeric_col)
                        if isinstance(v, (int, float)):
                            total_value += float(v)

                    # If we couldn't accumulate anything numeric, skip
                    if total_value == 0 and not any(
                        isinstance(r.get(preferred_numeric_col), (int, float))
                        for r in matching_rows
                    ):
                        continue

                    # Try to infer a source table if available
                    schema = chart.get("schema") or {}
                    schema_name = schema.get("schema_name")
                    table_name = schema.get("table_name")
                    source_line = ""
                    if schema_name and table_name:
                        source_line = f"\n\nSource: {schema_name}.{table_name}"

                    # Build a concise, user-friendly answer
                    answer_text = (
                        f"Based on your dashboard data, the total {preferred_numeric_col} for {ent} is **{int(total_value):,}**."
                        f"{source_line}"
                    )

                    return EnhancedChatResponse(
                        content=answer_text,
                        intent_detected=MessageIntent.DATA_QUERY,
                        data_results={
                            "query_results": matching_rows,
                            "columns": column_names,
                            "row_count": len(matching_rows),
                            "source": "direct_context_lookup_aggregate",
                        },
                        query_executed=False,
                        confidence_score=0.9,
                    )

                # If no row-level entity match, but the chart title itself contains the entity
                # and there is a single numeric column, treat this as an aggregate metric
                # card (e.g. "Karnataka Population" showing one number).
                chart_title = str(chart.get("title", ""))
                matched_entity_from_title = None
                for ent in entity_candidates:
                    if ent.lower() in chart_title.lower():
                        matched_entity_from_title = ent
                        break

                if matched_entity_from_title and rows:
                    first_row = rows[0]
                    numeric_cols = [
                        col_name
                        for col_name in column_names
                        if isinstance(first_row.get(col_name), (int, float))
                    ]
                    if not numeric_cols:
                        continue

                    preferred_numeric_col = None
                    for col_name in numeric_cols:
                        lower_name = str(col_name).lower()
                        if any(kw in lower_name for kw in numeric_preference_keywords):
                            preferred_numeric_col = col_name
                            break

                    if not preferred_numeric_col:
                        if len(numeric_cols) == 1:
                            preferred_numeric_col = numeric_cols[0]
                        else:
                            continue

                    value = first_row.get(preferred_numeric_col)
                    if not isinstance(value, (int, float)):
                        continue

                    schema = chart.get("schema") or {}
                    schema_name = schema.get("schema_name")
                    table_name = schema.get("table_name")
                    source_line = ""
                    if schema_name and table_name:
                        source_line = f"\n\nSource: {schema_name}.{table_name}"

                    answer_text = (
                        f"Based on your dashboard data, the {preferred_numeric_col} for {matched_entity_from_title} is **{value:,}**."
                        f"{source_line}"
                    )

                    return EnhancedChatResponse(
                        content=answer_text,
                        intent_detected=MessageIntent.DATA_QUERY,
                        data_results={
                            "query_results": [first_row],
                            "columns": column_names,
                            "row_count": 1,
                            "source": "direct_context_lookup_title",
                        },
                        query_executed=False,
                        confidence_score=0.85,
                    )

            return None

        except Exception as e:
            self.logger.error(f"Error in direct context lookup: {e}")
            return None

    def _generate_data_response_content(
        self,
        original_question: str,
        execution_result: Any,
        analysis: MessageAnalysis,
        analytics: Dict[str, Any],
    ) -> str:
        """Generate human-friendly response content based on query results"""
        try:
            data = execution_result.data
            row_count = execution_result.row_count

            # Start with a direct answer
            if row_count == 0:
                content = f"I found no data matching your query about '{original_question}'."
            elif row_count == 1 and len(execution_result.columns) == 1:
                # Single value result
                value = data[0][execution_result.columns[0]]
                content = f"Based on your data, the answer is: **{value:,}**"
            else:
                # Multiple rows/columns
                content = (
                    f"I found {row_count:,} results for your query about '{original_question}'."
                )

                # Add summary insights
                if row_count <= 10:
                    content += " Here's what the data shows:\n\n"
                    content += self._format_small_result_set(data, execution_result.columns)
                else:
                    content += " Here are the key insights:\n\n"
                    content += self._format_large_result_set(data, execution_result.columns)

            # Add performance info if relevant
            if analytics.get("cache_used"):
                content += f"\n\n*This result was retrieved from cache for faster response.*"
            elif execution_result.execution_time_ms > 2000:
                content += f"\n\n*Query completed in {execution_result.execution_time_ms/1000:.1f} seconds.*"

            # Add helpful context
            if execution_result.query_plan:
                confidence = execution_result.query_plan.confidence_score
                if confidence < 0.7:
                    content += f"\n\n*Note: This analysis has moderate confidence ({confidence:.1f}). You may want to verify the results.*"

            # Add explicit data source information so users can trace back
            source_tables = getattr(execution_result, "source_tables", None) or []
            if source_tables:
                # Deduplicate while preserving order
                seen = set()
                unique_sources = []
                for tbl in source_tables:
                    if tbl and tbl not in seen:
                        seen.add(tbl)
                        unique_sources.append(tbl)

                if unique_sources:
                    if len(unique_sources) == 1:
                        source_line = f"Source: {unique_sources[0]}"
                    else:
                        source_line = "Source: " + ", ".join(unique_sources)
                    content += f"\n\n{source_line}"

            return content

        except Exception as e:
            self.logger.error(f"Error generating data response content: {e}")
            return f"I successfully queried your data and found {execution_result.row_count} results, but had trouble formatting the response. The raw data is available in the detailed results."

    def _format_small_result_set(self, data: List[Dict], columns: List[str]) -> str:
        """Format small result sets (≤10 rows) as readable text"""
        formatted_lines = []

        for i, row in enumerate(data[:10], 1):
            if len(columns) == 1:
                # Single column - just show the value
                value = row[columns[0]]
                formatted_lines.append(f"{i}. {value}")
            elif len(columns) == 2:
                # Two columns - show as key-value pairs
                col1, col2 = columns[0], columns[1]
                formatted_lines.append(f"• **{row[col1]}**: {row[col2]}")
            else:
                # Multiple columns - show key fields
                key_fields = []
                for col in columns[:3]:  # Show first 3 columns
                    value = row[col]
                    if value is not None:
                        key_fields.append(f"{col}: {value}")
                formatted_lines.append(f"{i}. {', '.join(key_fields)}")

        return "\n".join(formatted_lines)

    def _format_large_result_set(self, data: List[Dict], columns: List[str]) -> str:
        """Format large result sets (>10 rows) with summary insights"""
        formatted_lines = []

        # Show top few results
        formatted_lines.append("**Top results:**")
        for i, row in enumerate(data[:5], 1):
            if len(columns) >= 2:
                col1, col2 = columns[0], columns[1]
                formatted_lines.append(f"{i}. **{row[col1]}**: {row[col2]}")
            else:
                formatted_lines.append(f"{i}. {row[columns[0]]}")

        if len(data) > 5:
            formatted_lines.append(f"... and {len(data) - 5} more results")

        # Add summary statistics if numeric data
        try:
            numeric_columns = []
            for col in columns:
                if all(
                    isinstance(row.get(col), (int, float))
                    for row in data[:10]
                    if row.get(col) is not None
                ):
                    numeric_columns.append(col)

            if numeric_columns:
                formatted_lines.append("\n**Summary statistics:**")
                for col in numeric_columns[:2]:  # Limit to 2 columns
                    values = [
                        row[col]
                        for row in data
                        if row.get(col) is not None and isinstance(row[col], (int, float))
                    ]
                    if values:
                        total = sum(values)
                        avg = total / len(values)
                        formatted_lines.append(
                            f"• {col}: Total = {total:,.0f}, Average = {avg:,.1f}"
                        )
        except:
            pass  # Skip statistics if calculation fails

        return "\n".join(formatted_lines)

    def _classify_intent(
        self,
        message_lower: str,
        data_query_confidence: float,
        key_entities: List[str],
        dashboard_context: Optional[Dict[str, Any]],
    ) -> Tuple[MessageIntent, float]:
        """Classify the intent of the message"""

        # Explicitly treat high-level dashboard questions as dashboard explanation,
        # even if they don't exactly match the regex patterns below.
        if "dashboard" in message_lower:
            if any(
                kw in message_lower
                for kw in ["tell me", "explain", "describe", "about", "overview", "summary"]
            ):
                return MessageIntent.DASHBOARD_EXPLANATION, 0.9

        # Strong data query indicators
        if data_query_confidence > 0.4:
            return MessageIntent.DATA_QUERY, min(data_query_confidence + 0.2, 0.9)

        # Dashboard explanation patterns
        explanation_patterns = [
            r"\b(?:what is|what does|explain|describe|tell me about)\b.*\b(?:chart|dashboard|graph|visualization)\b",
            r"\b(?:how does|how do|what happens)\b.*\b(?:this|dashboard|chart)\b",
            r"\b(?:what can|what should|how can)\s+i\s+(?:see|view|analyze|use)\b",
        ]

        explanation_matches = sum(
            1
            for pattern in explanation_patterns
            if re.search(pattern, message_lower, re.IGNORECASE)
        )

        if explanation_matches > 0:
            return MessageIntent.DASHBOARD_EXPLANATION, 0.7

        # Technical help patterns
        help_patterns = [
            r"\b(?:how to|help me|i need help|i don\'t know how)\b",
            r"\b(?:tutorial|guide|instruction|documentation)\b",
            r"\b(?:filter|sort|export|download|share)\b",
        ]

        help_matches = sum(
            1 for pattern in help_patterns if re.search(pattern, message_lower, re.IGNORECASE)
        )

        if help_matches > 0:
            return MessageIntent.TECHNICAL_HELP, 0.6

        # Business insight patterns
        insight_patterns = [
            r"\b(?:insight|analysis|trend|pattern|recommendation|suggestion)\b",
            r"\b(?:improve|optimize|increase|decrease|grow|strategy)\b",
            r"\b(?:business|performance|roi|kpi|metric)\b",
        ]

        insight_matches = sum(
            1 for pattern in insight_patterns if re.search(pattern, message_lower, re.IGNORECASE)
        )

        if insight_matches > 0:
            return MessageIntent.BUSINESS_INSIGHT, 0.6

        # Default to general conversation
        return MessageIntent.GENERAL_CONVERSATION, 0.5

    def _extract_temporal_context(self, message: str) -> Optional[str]:
        """Extract temporal context from the message"""
        for pattern in self.temporal_patterns:
            match = re.search(pattern, message, re.IGNORECASE)
            if match:
                return match.group().strip()
        return None

    def _extract_geographic_context(self, message: str) -> Optional[str]:
        """Extract geographic context from the message"""
        for pattern in self.geographic_patterns:
            match = re.search(pattern, message, re.IGNORECASE)
            if match:
                return match.group(1).strip()
        return None

    def _extract_metric_context(self, message: str) -> List[str]:
        """Extract metric-related terms from the message"""
        metrics = []
        for pattern in self.metric_patterns:
            matches = re.findall(pattern, message, re.IGNORECASE)
            metrics.extend(matches)
        return list(set(metrics))  # Remove duplicates

    def _extract_key_entities(
        self, message: str, dashboard_context: Optional[Dict[str, Any]]
    ) -> List[str]:
        """Extract key entities mentioned in the message"""
        entities = []

        # Extract from dashboard context if available
        if dashboard_context:
            charts = dashboard_context.get("charts", [])
            for chart in charts:
                chart_title = chart.get("title", "").lower()
                if chart_title and chart_title in message.lower():
                    entities.append(chart_title)

        # Extract common business entities
        business_entities = [
            r"\b(customer|client|user|student|employee|product|order|transaction|sale|purchase|invoice|payment)\w*\b"
        ]

        for pattern in business_entities:
            matches = re.findall(pattern, message, re.IGNORECASE)
            entities.extend(matches)

        return list(set(entities))  # Remove duplicates

    def _suggest_sql_approach(
        self, intent: MessageIntent, entities: List[str], metrics: List[str]
    ) -> str:
        """Suggest an SQL approach based on detected intent and entities"""
        if intent == MessageIntent.DATA_QUERY:
            if any(metric in ["count", "total"] for metric in metrics):
                return "COUNT aggregation with GROUP BY"
            elif any(metric in ["average", "mean"] for metric in metrics):
                return "AVG aggregation with GROUP BY"
            elif any(metric in ["revenue", "sales", "amount"] for metric in metrics):
                return "SUM aggregation with GROUP BY"
            else:
                return "SELECT with filtering and ordering"
        return ""

    def _build_dashboard_explanation_prompt(
        self, dashboard_context: Optional[Dict[str, Any]], analysis: MessageAnalysis
    ) -> str:
        """Build system prompt for dashboard explanation messages"""
        base_instructions = [
            "You are a dashboard expert helping users understand their own data visualizations.",
            "",
            "HARD CONSTRAINTS:",
            "- Use ONLY the information that is explicitly available in the dashboard context, the query results, and this conversation.",
            "- Do NOT use any external knowledge, the public internet, or general facts about the world (for example: typical populations of states or countries).",
            '- Do NOT guess, infer, or "fill in" missing values from outside knowledge. If the data needed to answer a question is not present in the dashboard context, clearly say that the data is not available.',
            '- When data is missing, respond with a message like: "Based on the data available in this dashboard, I cannot determine this. The necessary data is not present."',
            "",
            "YOUR GOAL:",
            "- Explain what THIS specific dashboard contains and how it can be used.",
            "- Focus on the actual charts, metrics, and filters that are present in the dashboard context below.",
            "- Describe business insights that can be derived ONLY from the provided charts and fields.",
            "- Suggest concrete follow-up questions the user can ask about their own data (not external benchmarks or public statistics).",
        ]

        # If no structured context is available, fall back to base instructions only
        if not dashboard_context:
            return "\n".join(base_instructions)

        dashboard = dashboard_context.get("dashboard", {}) or {}
        charts = dashboard_context.get("charts", []) or []
        filters = dashboard_context.get("filters", []) or []
        summary = dashboard_context.get("summary", {}) or {}

        context_lines: List[str] = []

        # Dashboard summary
        context_lines.append("")
        context_lines.append("DASHBOARD OVERVIEW:")
        context_lines.append(f"- Title: {dashboard.get('title', 'Untitled Dashboard')}")
        description = dashboard.get("description") or ""
        if description:
            context_lines.append(f"- Description: {description}")
        context_lines.append(
            f"- Type: {dashboard.get('dashboard_type', 'unknown')} | Target screen size: {dashboard.get('target_screen_size', 'unspecified')}"
        )
        context_lines.append(f"- Total charts: {summary.get('total_charts', len(charts))}")

        # High-level chart list
        if charts:
            context_lines.append("")
            context_lines.append("CHARTS IN THIS DASHBOARD:")
            for idx, chart in enumerate(charts[:10], start=1):
                title = chart.get("title", f"Chart {idx}")
                chart_type = chart.get("type", "unknown")
                schema = chart.get("schema") or {}
                schema_name = schema.get("schema_name")
                table_name = schema.get("table_name")
                metrics = schema.get("metrics") or []
                dimensions = schema.get("dimensions") or []

                line = f"{idx}. {title} — type: {chart_type}"
                if metrics:
                    line += f" | metrics: {', '.join(metrics)}"
                if dimensions:
                    line += f" | dimensions: {', '.join(dimensions)}"
                if schema_name and table_name:
                    line += f" | source table: {schema_name}.{table_name}"
                context_lines.append(line)

        # Filters
        if filters:
            context_lines.append("")
            context_lines.append("AVAILABLE FILTERS:")
            for f in filters[:10]:
                name = f.get("name", "Unnamed filter")
                ftype = f.get("filter_type", "unknown")
                context_lines.append(f"- {name} (type: {ftype})")

        context_lines.append("")
        context_lines.append(
            'When the user asks you to "tell me more about the dashboard" or similar, you MUST:'
        )
        context_lines.append(
            "- Start with a 1–2 sentence plain-language summary of what this dashboard helps them understand."
        )
        context_lines.append(
            "- Then mention a few key charts by name and briefly explain what each one focuses on (e.g., which metric and which dimension)."
        )
        context_lines.append(
            "- Optionally point out any important filters that change how the data is viewed."
        )
        context_lines.append(
            "- Keep your explanation concrete and grounded in the specific charts and fields listed above."
        )

        return "\n".join(base_instructions + context_lines)

    def _build_dashboard_summary_from_context(self, dashboard_context: Dict[str, Any]) -> str:
        """
        Build a succinct, deterministic explanation of the current dashboard
        using only the structured context (no model reasoning).
        This function is defensive and should never raise; on any issue it
        still returns a chart-based summary instead of a generic sentence.
        """
        try:
            if not isinstance(dashboard_context, dict):
                return "This dashboard has several charts that highlight different metrics for your data."

            dashboard = dashboard_context.get("dashboard") or {}
            if not isinstance(dashboard, dict):
                dashboard = {}

            charts = dashboard_context.get("charts") or []
            if not isinstance(charts, list):
                charts = []

            filters = dashboard_context.get("filters") or []
            if not isinstance(filters, list):
                filters = []

            summary = dashboard_context.get("summary") or {}
            if not isinstance(summary, dict):
                summary = {}

            lines: List[str] = []

            title = str(dashboard.get("title") or "this dashboard")
            description = str(dashboard.get("description") or "").strip()
            total_charts = summary.get("total_charts")
            if not isinstance(total_charts, int):
                total_charts = len(charts)

            # Intro
            if description:
                lines.append(
                    f'This dashboard, "{title}", gives you an overview of your data: {description}'
                )
            else:
                lines.append(
                    f'This dashboard, "{title}", gives you an overview of key metrics in your data.'
                )

            if total_charts > 0:
                lines.append(
                    f'It contains {total_charts} chart{"" if total_charts == 1 else "s"} that highlight different aspects of your data.'
                )

            # Chart-by-chart summary
            if charts:
                lines.append("")
                lines.append("Here is what each chart focuses on:")
                for idx, chart in enumerate(charts, start=1):
                    if not isinstance(chart, dict):
                        continue

                    chart_title = str(chart.get("title") or f"Chart {idx}")
                    chart_type = str(chart.get("type") or "chart")
                    schema = chart.get("schema") or {}
                    if not isinstance(schema, dict):
                        schema = {}

                    metrics = schema.get("metrics") or []
                    if not isinstance(metrics, list):
                        metrics = [str(metrics)]

                    dimensions = schema.get("dimensions") or []
                    if not isinstance(dimensions, list):
                        dimensions = [str(dimensions)]

                    schema_name = schema.get("schema_name")
                    table_name = schema.get("table_name")

                    parts: List[str] = []
                    parts.append(f"- {chart_title}: a {chart_type} visualization")

                    if metrics and dimensions:
                        parts.append(
                            f"showing {', '.join(str(m) for m in metrics)} broken down by {', '.join(str(d) for d in dimensions)}"
                        )
                    elif metrics:
                        parts.append(
                            f"highlighting the metric{'' if len(metrics) == 1 else 's'} {', '.join(str(m) for m in metrics)}"
                        )
                    elif dimensions:
                        parts.append(
                            f"comparing values across {', '.join(str(d) for d in dimensions)}"
                        )

                    if schema_name and table_name:
                        parts.append(f"using data from {schema_name}.{table_name}")

                    lines.append(" ".join(parts).strip())

            # Filters
            if filters:
                lines.append("")
                lines.append("You can refine the view using the available filters, for example:")
                for f in filters[:3]:
                    if not isinstance(f, dict):
                        continue
                    name = str(f.get("name") or "Unnamed filter")
                    ftype = str(f.get("filter_type") or "filter")
                    lines.append(f"- {name} ({ftype})")

            return "\n".join(lines).strip()

        except Exception as e:
            self.logger.error(f"Error building dashboard summary from context: {e}")
            # Fallback: at least mention chart titles if available
            try:
                charts = dashboard_context.get("charts") or []
                titles = [
                    str(c.get("title")) for c in charts if isinstance(c, dict) and c.get("title")
                ]
                if titles:
                    return "This dashboard contains the following charts:\n" + "\n".join(
                        f"- {t}" for t in titles
                    )
            except Exception:
                pass

            return (
                "This dashboard has several charts that highlight different metrics for your data."
            )

    def _build_general_chat_prompt(self, dashboard_context: Optional[Dict[str, Any]]) -> str:
        """Build system prompt for general chat messages"""
        return """You are a helpful data analysis assistant for dashboard users.

HARD CONSTRAINTS (APPLY TO ALL QUESTIONS):
- You MUST use ONLY the data, schemas, and query results explicitly provided in the dashboard context and this conversation as your source of truth.
- You MUST NOT use the public internet, external data sources, or general world knowledge (including your own training-time knowledge) to answer questions.
- If a question cannot be answered purely from the fields, values, charts, filters, or structures present in the current dashboard context, you MUST clearly say that the information is not available in the current dashboard data.
- In those cases, DO NOT guess, approximate, or fill in values or explanations from outside knowledge.
- Never invent states, regions, entities, metrics, or numbers that are not present in the dashboard context or explicitly mentioned by the user.

WHAT YOU MAY ANSWER:
- Questions that clearly refer to this dashboard's charts, tables, filters, columns, or metrics.
- Questions that interpret or summarize patterns visible in the dashboard data that has been provided.
- Clarifying questions about what is present in the dashboard data (for example: which columns exist, what a chart is showing, how values in this dataset relate to each other).

WHAT YOU MUST NOT ANSWER:
- General knowledge questions that are not grounded in this dashboard's data (for example: capital cities, world populations, definitions of generic concepts, explanations of algorithms, or market benchmarks).
- Generic explanations of statistical or business concepts that are not tied directly to specific columns, measures, or charts in this dashboard.

IF A QUESTION IS OUT OF SCOPE:
- If the user asks anything that cannot be answered strictly from the current dashboard data, respond with a clear data-scoped message such as:
  "Based on the data available in this dashboard, I cannot answer that question because the necessary information is not present in the dashboard data."
- Prefer refusing with this kind of message over attempting to be helpful using outside knowledge.

Be concise, business-focused, and always grounded ONLY in the dashboard data when forming your answers."""

    def _generate_query_suggestions(
        self, analysis: MessageAnalysis, dashboard_context: Optional[Dict[str, Any]]
    ) -> List[str]:
        """Generate helpful query suggestions based on message analysis"""
        suggestions = []

        if analysis.temporal_context:
            suggestions.append(
                f"Try asking: 'How many customers did we have in {analysis.temporal_context}?'"
            )

        if analysis.metric_context:
            for metric in analysis.metric_context[:2]:
                suggestions.append(f"You can ask: 'What was the total {metric} by region?'")

        # Add dashboard-specific suggestions if available
        if dashboard_context and dashboard_context.get("charts"):
            chart_titles = [chart.get("title", "") for chart in dashboard_context["charts"][:3]]
            for title in chart_titles:
                if title:
                    suggestions.append(f"Ask about: '{title} data breakdown'")

        return suggestions[:3]  # Limit to 3 suggestions

    def _generate_query_improvement_suggestions(
        self, message: str, analysis: MessageAnalysis, dashboard_context: Optional[Dict[str, Any]]
    ) -> List[str]:
        """Generate suggestions for improving failed queries"""
        suggestions = []

        # Suggest being more specific
        if len(message.split()) < 5:
            suggestions.append(
                "Try being more specific - include time periods, regions, or specific metrics"
            )

        # Suggest using available data
        if dashboard_context and dashboard_context.get("charts"):
            chart_count = len(dashboard_context["charts"])
            suggestions.append(f"Ask about the {chart_count} charts available in this dashboard")

        # Suggest common patterns
        suggestions.extend(
            [
                "How many [items] in [time period]?",
                "What is the total [metric] by [dimension]?",
                "Show me the top 10 [items] by [metric]",
            ]
        )

        return suggestions[:4]

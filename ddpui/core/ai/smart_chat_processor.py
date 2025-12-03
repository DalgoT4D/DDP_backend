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
            # Use standard AI chat with rich dashboard context
            if not self.ai_provider:
                self.ai_provider = get_default_ai_provider()

            system_prompt = self._build_dashboard_explanation_prompt(dashboard_context, analysis)

            ai_messages = [
                AIMessage(role="system", content=system_prompt),
                AIMessage(role="user", content=message),
            ]

            response = self.ai_provider.chat_completion(
                messages=ai_messages, temperature=0.3, max_tokens=1000
            )

            return EnhancedChatResponse(
                content=response.content,
                intent_detected=MessageIntent.DASHBOARD_EXPLANATION,
                confidence_score=0.8,
            )

        except Exception as e:
            self.logger.error(f"Error handling dashboard explanation: {e}")
            return EnhancedChatResponse(
                content="I can help explain this dashboard. Could you be more specific about what you'd like to know?",
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
        return """You are a dashboard expert helping users understand their own data visualizations.

HARD CONSTRAINTS:
- Use ONLY the information that is explicitly available in the dashboard context, the query results, and this conversation.
- Do NOT use any external knowledge, the public internet, or general facts about the world (for example: typical populations of states or countries).
- Do NOT guess, infer, or "fill in" missing values from outside knowledge. If the data needed to answer a question is not present in the dashboard context, clearly say that the data is not available.
- When data is missing, respond with a message like: "Based on the data available in this dashboard, I cannot determine this. The necessary data is not present."

Focus on:
- Explaining what each chart (in the provided dashboard context) shows and why it's useful
- Describing the business insights that can be derived ONLY from the provided charts and fields
- Guiding users on how to interact with the dashboard
- Suggesting specific questions they can ask about their own data (not external benchmarks or public statistics)

Be clear, helpful, and business-focused in your explanations while staying strictly grounded in the provided data."""

    def _build_general_chat_prompt(self, dashboard_context: Optional[Dict[str, Any]]) -> str:
        """Build system prompt for general chat messages"""
        return """You are a helpful data analysis assistant for dashboard users.

HARD CONSTRAINTS:
- When answering questions about specific values, rankings, comparisons, or facts (for example: "which state has the highest population" or "what is the revenue for Maharashtra"), you MUST rely ONLY on the data provided in the dashboard context or explicit query results.
- Do NOT use the public internet, external data sources, or general world knowledge to answer such questions.
- If the dashboard context does not contain the data needed to answer a question, say so clearly and DO NOT guess. Use wording such as: "Based on the data available in this dashboard, I cannot determine that because the necessary data is not present."
- Never invent states, regions, entities, metrics, or numbers that are not present in the dashboard context or in the user's own message.

You can help with:
- General questions about data analysis concepts
- Understanding business metrics in a generic, educational way
- Suggesting ways to explore the data that IS present in the current dashboard
- Explaining data visualization concepts

Be conversational but informative. For any question that depends on concrete data values from the dashboard, stay strictly grounded in the provided data and prefer saying that data is not available over guessing."""

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

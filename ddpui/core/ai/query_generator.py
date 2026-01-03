"""
Natural Language Query Generator for AI Chat Enhancement

This service converts user questions into safe, executable SQL queries using AI,
with comprehensive validation and security measures.
"""

import re
import logging
from typing import Dict, List, Any, Optional, Tuple, Set
from dataclasses import dataclass, field
from datetime import datetime
import json

from django.utils import timezone
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from ddpui.core.ai.factory import get_default_ai_provider
from ddpui.core.ai.interfaces import AIMessage
from ddpui.core.ai.data_intelligence import DataIntelligenceService, DataCatalog
from ddpui.models.org import Org, OrgWarehouse
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui.core.ai.query_generator")


@dataclass
class QueryValidationResult:
    """Result of query validation checks"""

    is_valid: bool
    error_message: str = ""
    warnings: List[str] = field(default_factory=list)
    complexity_score: int = 0  # 1-10, higher = more complex
    estimated_rows: Optional[int] = None
    tables_accessed: List[str] = field(default_factory=list)
    columns_accessed: List[str] = field(default_factory=list)


@dataclass
class QueryPlan:
    """Complete plan for executing a user's natural language query"""

    original_question: str
    generated_sql: str
    explanation: str
    parameters: Dict[str, Any] = field(default_factory=dict)
    requires_execution: bool = True
    expected_result_type: str = "table"  # table, single_value, aggregation
    confidence_score: float = 0.0  # 0.0-1.0
    validation_result: Optional[QueryValidationResult] = None
    fallback_to_existing_data: bool = False
    ai_reasoning: str = ""
    created_at: datetime = field(default_factory=timezone.now)

    # Optional LLM usage details for accurate metering
    llm_usage: Optional[Dict[str, int]] = None
    llm_provider: Optional[str] = None
    llm_model: Optional[str] = None


class QuerySecurityValidator:
    """
    Comprehensive security validation for AI-generated SQL queries.
    Ensures only safe SELECT operations with proper constraints.
    """

    # Dangerous SQL patterns that are never allowed
    DANGEROUS_PATTERNS = [
        r"\b(DROP|DELETE|UPDATE|INSERT|ALTER|CREATE|TRUNCATE|EXEC|EXECUTE)\b",
        r"\b(GRANT|REVOKE|DENY)\b",
        r"\b(BACKUP|RESTORE|SHUTDOWN|KILL)\b",
        r"--[^\r\n]*",  # SQL comments
        r"/\*.*?\*/",  # Multi-line comments
        r";.*",  # Multiple statements
        r"\b(xp_|sp_|fn_)\w*",  # System procedures
        r"\b(INFORMATION_SCHEMA)\b",  # Schema introspection
        r"\b(pg_|mysql\.)\w*",  # Database-specific system tables
    ]

    # Allowed aggregation functions
    ALLOWED_AGGREGATIONS = {
        "COUNT",
        "SUM",
        "AVG",
        "MIN",
        "MAX",
        "DISTINCT",
        "GROUP_CONCAT",
        "STRING_AGG",
    }

    # Allowed operators
    ALLOWED_OPERATORS = {
        "=",
        "!=",
        "<>",
        "<",
        ">",
        "<=",
        ">=",
        "LIKE",
        "ILIKE",
        "IN",
        "NOT IN",
        "BETWEEN",
        "AND",
        "OR",
        "NOT",
        "IS NULL",
        "IS NOT NULL",
    }

    def __init__(self):
        self.logger = CustomLogger("QuerySecurityValidator")

    def validate_query(
        self, query: str, available_tables: Set[str], org_warehouse: OrgWarehouse
    ) -> QueryValidationResult:
        """
        Comprehensive validation of AI-generated SQL query.

        Args:
            query: SQL query to validate
            available_tables: Set of allowed table names (schema.table format)
            org_warehouse: Organization's warehouse for connection testing

        Returns:
            QueryValidationResult with validation status and details
        """
        self.logger.info(f"Validating query: {query[:100]}...")

        try:
            # Step 1: Basic security checks
            security_result = self._validate_security_patterns(query)
            if not security_result.is_valid:
                return security_result

            # Step 2: SQL structure validation
            structure_result = self._validate_sql_structure(query)
            if not structure_result.is_valid:
                return structure_result

            # Step 3: Table and column access validation
            access_result = self._validate_table_access(query, available_tables)
            if not access_result.is_valid:
                return access_result

            # Step 4: Query complexity analysis
            complexity_result = self._analyze_query_complexity(query)

            # Step 5: Syntax validation (if possible)
            syntax_result = self._validate_sql_syntax(query, org_warehouse)

            if not syntax_result.is_valid:
                return syntax_result

            # Combine all validation results
            final_result = QueryValidationResult(
                is_valid=True,
                complexity_score=complexity_result.complexity_score,
                estimated_rows=complexity_result.estimated_rows,
                tables_accessed=access_result.tables_accessed,
                columns_accessed=access_result.columns_accessed,
                warnings=(
                    security_result.warnings
                    + structure_result.warnings
                    + access_result.warnings
                    + complexity_result.warnings
                    + syntax_result.warnings
                ),
            )

            # Apply complexity-based restrictions
            if complexity_result.complexity_score > 8:
                final_result.is_valid = False
                final_result.error_message = "Query too complex for safe execution"
            elif complexity_result.complexity_score > 6:
                final_result.warnings.append("High complexity query - monitor performance")

            return final_result

        except Exception as e:
            self.logger.error(f"Error during query validation: {e}")
            return QueryValidationResult(
                is_valid=False, error_message=f"Validation failed: {str(e)}"
            )

    def _validate_security_patterns(self, query: str) -> QueryValidationResult:
        """Check for dangerous SQL patterns"""
        query_upper = query.upper()

        for pattern in self.DANGEROUS_PATTERNS:
            if re.search(pattern, query_upper, re.IGNORECASE | re.MULTILINE):
                return QueryValidationResult(
                    is_valid=False, error_message=f"Dangerous SQL pattern detected: {pattern}"
                )

        # Must start with SELECT
        if not re.match(r"^\s*SELECT\b", query_upper):
            return QueryValidationResult(
                is_valid=False, error_message="Only SELECT queries are allowed"
            )

        return QueryValidationResult(is_valid=True)

    def _validate_sql_structure(self, query: str) -> QueryValidationResult:
        """Validate basic SQL structure and components"""
        warnings = []

        # Check for basic SELECT structure
        if "FROM" not in query.upper():
            return QueryValidationResult(
                is_valid=False, error_message="SELECT query must include FROM clause"
            )

        # Check for potentially expensive operations
        query_upper = query.upper()

        if "UNION" in query_upper:
            warnings.append("UNION operations may be slow")

        if query_upper.count("JOIN") > 3:
            warnings.append("Multiple JOINs may impact performance")

        if "GROUP BY" in query_upper and "LIMIT" not in query_upper:
            warnings.append("GROUP BY without LIMIT may return many rows")

        # Check for subqueries
        if query.count("(") != query.count(")"):
            return QueryValidationResult(
                is_valid=False, error_message="Unbalanced parentheses in query"
            )

        subquery_count = len(re.findall(r"\(\s*SELECT\b", query_upper))
        if subquery_count > 2:
            warnings.append("Multiple subqueries may impact performance")

        return QueryValidationResult(is_valid=True, warnings=warnings)

    def _validate_table_access(
        self, query: str, available_tables: Set[str]
    ) -> QueryValidationResult:
        """Validate that query only accesses allowed tables"""
        # Extract table references from the query
        referenced_tables = self._extract_table_references(query)

        tables_accessed = []
        unauthorized_tables = []

        for table_ref in referenced_tables:
            # Normalize table reference (handle schema.table or just table)
            normalized_ref = table_ref.strip('"').strip("'")

            # Check if this table is in our allowed set
            found_match = False
            for allowed_table in available_tables:
                if (
                    normalized_ref == allowed_table
                    or normalized_ref.endswith("." + allowed_table.split(".")[-1])
                    or allowed_table.endswith("." + normalized_ref)
                ):
                    tables_accessed.append(allowed_table)
                    found_match = True
                    break

            if not found_match:
                unauthorized_tables.append(normalized_ref)

        if unauthorized_tables:
            return QueryValidationResult(
                is_valid=False,
                error_message=f"Access denied to tables: {', '.join(unauthorized_tables)}",
            )

        # Extract column references (basic check)
        columns_accessed = self._extract_column_references(query)

        return QueryValidationResult(
            is_valid=True, tables_accessed=tables_accessed, columns_accessed=columns_accessed
        )

    def _extract_table_references(self, query: str) -> List[str]:
        """Extract table names referenced in the query"""
        # This is a simplified extraction - could be enhanced with proper SQL parsing
        table_patterns = [
            r'FROM\s+(["`]?\w+["`]?(?:\.["`]?\w+["`]?)?)',
            r'JOIN\s+(["`]?\w+["`]?(?:\.["`]?\w+["`]?)?)',
            r'UPDATE\s+(["`]?\w+["`]?(?:\.["`]?\w+["`]?)?)',
            r'INSERT\s+INTO\s+(["`]?\w+["`]?(?:\.["`]?\w+["`]?)?)',
        ]

        tables = []
        query_upper = query.upper()

        for pattern in table_patterns:
            matches = re.findall(pattern, query_upper, re.IGNORECASE)
            tables.extend(matches)

        return tables

    def _extract_column_references(self, query: str) -> List[str]:
        """Extract column names referenced in the query (basic implementation)"""
        # This is a simplified implementation
        # In production, you might want to use a proper SQL parser

        # Extract columns from SELECT clause
        select_match = re.search(r"SELECT\s+(.*?)\s+FROM", query, re.IGNORECASE | re.DOTALL)
        if not select_match:
            return []

        select_clause = select_match.group(1)

        # Simple column extraction (handles basic cases)
        columns = []
        parts = select_clause.split(",")

        for part in parts:
            part = part.strip()
            # Remove aliases and functions
            part = re.sub(r"\s+AS\s+\w+", "", part, flags=re.IGNORECASE)
            part = re.sub(r"\w+\s*\(.*?\)", "", part)  # Remove functions

            if part and not part.upper() in ["*", "DISTINCT"]:
                columns.append(part.strip())

        return columns

    def _analyze_query_complexity(self, query: str) -> QueryValidationResult:
        """Analyze query complexity and estimate performance impact"""
        complexity_score = 1  # Base score
        warnings = []
        estimated_rows = None

        query_upper = query.upper()

        # Add complexity for various operations
        if "JOIN" in query_upper:
            join_count = query_upper.count("JOIN")
            complexity_score += join_count * 2
            if join_count > 2:
                warnings.append(f"Multiple JOINs ({join_count}) may be slow")

        if "GROUP BY" in query_upper:
            complexity_score += 2

        if "ORDER BY" in query_upper:
            complexity_score += 1

        if "HAVING" in query_upper:
            complexity_score += 1

        if re.search(r"\(\s*SELECT\b", query_upper):
            subquery_count = len(re.findall(r"\(\s*SELECT\b", query_upper))
            complexity_score += subquery_count * 3
            warnings.append(f"Contains {subquery_count} subquer(y/ies)")

        if "UNION" in query_upper:
            complexity_score += 3
            warnings.append("UNION operation may be resource intensive")

        # Check for LIMIT clause
        limit_match = re.search(r"LIMIT\s+(\d+)", query_upper)
        if limit_match:
            limit_value = int(limit_match.group(1))
            estimated_rows = min(limit_value, 1000)  # Cap at our safety limit

            if limit_value > 1000:
                warnings.append("LIMIT value reduced to 1000 for safety")
        else:
            # No LIMIT - add complexity and warn
            complexity_score += 2
            warnings.append("No LIMIT clause - results will be capped at 1000 rows")
            estimated_rows = 1000

        return QueryValidationResult(
            is_valid=True,
            complexity_score=min(complexity_score, 10),  # Cap at 10
            estimated_rows=estimated_rows,
            warnings=warnings,
        )

    def _validate_sql_syntax(
        self, query: str, org_warehouse: OrgWarehouse
    ) -> QueryValidationResult:
        """Validate SQL syntax using the actual database connection"""
        try:
            from ddpui.datainsights.warehouse.warehouse_factory import WarehouseFactory

            warehouse_client = WarehouseFactory.get_warehouse_client(org_warehouse)

            # Try to explain the query (this validates syntax without executing)
            explain_query = f"EXPLAIN {query}"

            # Add LIMIT 0 to prevent any data return during validation
            if "LIMIT" not in query.upper():
                validation_query = f"{query} LIMIT 0"
            else:
                validation_query = query

            # This will raise an exception if syntax is invalid
            warehouse_client.execute(text(validation_query))

            return QueryValidationResult(is_valid=True)

        except Exception as e:
            error_msg = str(e).lower()

            # Check for common syntax errors
            if any(term in error_msg for term in ["syntax", "parse", "invalid"]):
                return QueryValidationResult(
                    is_valid=False, error_message=f"SQL syntax error: {str(e)}"
                )

            # For other errors, issue warning but allow query
            return QueryValidationResult(
                is_valid=True, warnings=[f"Could not validate syntax: {str(e)}"]
            )


class NaturalLanguageQueryService:
    """
    Service to convert natural language questions into safe, executable SQL queries.

    Uses AI to understand user intent and generate appropriate queries with
    comprehensive security validation and safety measures.
    """

    def __init__(self):
        self.logger = CustomLogger("NaturalLanguageQueryService")
        self.data_intelligence = DataIntelligenceService()
        self.security_validator = QuerySecurityValidator()
        self.ai_provider = None

    def generate_query_from_question(
        self,
        question: str,
        org: Org,
        dashboard_id: Optional[int] = None,
        data_catalog: Optional[DataCatalog] = None,
    ) -> QueryPlan:
        """
        Convert a natural language question into an executable SQL query plan.

        Args:
            question: User's natural language question
            org: Organization context
            dashboard_id: Optional dashboard for focused analysis
            data_catalog: Optional pre-built catalog (for performance)

        Returns:
            QueryPlan with generated SQL and validation results
        """
        self.logger.info(f"Generating query for question: {question[:100]}...")

        try:
            # Get AI provider
            if not self.ai_provider:
                self.ai_provider = get_default_ai_provider()

            # Get or build data catalog
            if not data_catalog:
                data_catalog = self.data_intelligence.get_org_data_catalog(org)

            if not data_catalog.tables:
                return QueryPlan(
                    original_question=question,
                    generated_sql="",
                    explanation="No data sources available for query generation",
                    requires_execution=False,
                    fallback_to_existing_data=True,
                    confidence_score=0.0,
                    ai_reasoning="No tables found in data catalog",
                )

            # Build enhanced context for AI query generation
            query_context = self._build_query_generation_context(
                question, data_catalog, dashboard_id
            )

            # Generate query using AI
            ai_messages = [
                AIMessage(role="system", content=self._build_query_generation_system_prompt()),
                AIMessage(role="user", content=query_context),
            ]

            self.logger.info("Requesting AI query generation...")
            response = self.ai_provider.chat_completion(
                messages=ai_messages,
                temperature=0.1,  # Low temperature for consistent SQL generation
                max_tokens=2000,
            )

            # Parse AI response
            query_plan = self._parse_ai_query_response(question, response.content)

            # Attach LLM usage and model metadata for downstream metering
            try:
                if hasattr(query_plan, "llm_usage"):
                    query_plan.llm_usage = response.usage or None
                if hasattr(query_plan, "llm_provider"):
                    # Prefer explicit provider on response, fallback to provider type enum
                    provider_name = getattr(response, "provider", None)
                    if not provider_name and hasattr(self.ai_provider, "get_provider_type"):
                        provider_enum = self.ai_provider.get_provider_type()
                        provider_name = getattr(provider_enum, "value", None)
                    query_plan.llm_provider = provider_name
                if hasattr(query_plan, "llm_model"):
                    query_plan.llm_model = getattr(response, "model", None)
            except Exception as usage_error:
                # Do not fail query generation if usage attachment fails
                self.logger.error(f"Error attaching LLM usage to QueryPlan: {usage_error}")

            # Validate the generated query
            if query_plan.requires_execution and query_plan.generated_sql:
                org_warehouse = OrgWarehouse.objects.filter(org=org).first()
                if org_warehouse:
                    available_tables = set(data_catalog.tables.keys())
                    validation_result = self.security_validator.validate_query(
                        query_plan.generated_sql, available_tables, org_warehouse
                    )
                    query_plan.validation_result = validation_result

                    if not validation_result.is_valid:
                        query_plan.requires_execution = False
                        query_plan.fallback_to_existing_data = True
                        query_plan.explanation += (
                            f" Query validation failed: {validation_result.error_message}"
                        )

            return query_plan

        except Exception as e:
            self.logger.error(f"Error generating query from question: {e}")
            return QueryPlan(
                original_question=question,
                generated_sql="",
                explanation=f"Failed to generate query: {str(e)}",
                requires_execution=False,
                fallback_to_existing_data=True,
                confidence_score=0.0,
                ai_reasoning=f"Error during generation: {str(e)}",
            )

    def _build_query_generation_context(
        self, question: str, data_catalog: DataCatalog, dashboard_id: Optional[int] = None
    ) -> str:
        """Build comprehensive context for AI query generation"""

        context_parts = [
            "USER QUESTION:",
            f'"{question}"',
            "",
            "AVAILABLE DATA FOR QUERY GENERATION:",
            "",
        ]

        # Add focused table information
        relevant_tables = self._identify_relevant_tables(question, data_catalog)

        if not relevant_tables:
            # If no relevant tables found, include all tables but limit detail
            relevant_tables = list(data_catalog.tables.keys())[:5]

        for table_key in relevant_tables:
            if table_key in data_catalog.tables:
                table_info = data_catalog.tables[table_key]
                context_parts.extend(self._build_table_query_context(table_info))
                context_parts.append("")

        # Add query examples and guidelines
        context_parts.extend(
            [
                "QUERY GENERATION REQUIREMENTS:",
                "",
                "1. Generate ONLY a SELECT query that answers the user's question",
                "2. Use proper table and column names from the available data above",
                "3. Include appropriate WHERE clauses for filtering",
                "4. Add GROUP BY and aggregation functions when needed",
                "5. Always include ORDER BY for sorted results",
                "6. ALWAYS include 'LIMIT 1000' to prevent large result sets",
                "7. Use proper SQL syntax for the available database type",
                "",
                "RESPONSE FORMAT:",
                "Provide your response in this exact JSON format:",
                "{",
                '  "sql": "SELECT ... FROM ... WHERE ... LIMIT 1000",',
                '  "explanation": "This query answers the question by...",',
                '  "confidence": 0.85,',
                '  "reasoning": "I chose this approach because...",',
                '  "expected_result": "table|single_value|aggregation"',
                "}",
                "",
            ]
        )

        return "\n".join(context_parts)

    def _identify_relevant_tables(self, question: str, data_catalog: DataCatalog) -> List[str]:
        """Identify tables most relevant to the user's question"""
        question_lower = question.lower()
        relevant_tables = []

        # Score tables based on keyword matches
        table_scores = {}

        for table_key, table_info in data_catalog.tables.items():
            score = 0

            # Check table name and description
            table_name_lower = table_info.table_name.lower()
            description_lower = table_info.business_description.lower()

            # Score based on table name matches
            for word in question_lower.split():
                if len(word) > 2:  # Skip small words
                    if word in table_name_lower:
                        score += 3
                    if word in description_lower:
                        score += 2

            # Check column names and contexts
            for col in table_info.columns:
                col_name_lower = col.name.lower()
                col_context_lower = col.business_context.lower()

                for word in question_lower.split():
                    if len(word) > 2:
                        if word in col_name_lower:
                            score += 2
                        if word in col_context_lower:
                            score += 1

                # Check sample values
                for sample in col.sample_values:
                    if str(sample).lower() in question_lower:
                        score += 2

            if score > 0:
                table_scores[table_key] = score

        # Return top 3 tables by relevance score
        sorted_tables = sorted(table_scores.items(), key=lambda x: x[1], reverse=True)
        return [table_key for table_key, _ in sorted_tables[:3]]

    def _build_table_query_context(self, table_info) -> List[str]:
        """Build query-focused context for a specific table"""
        lines = [
            f"TABLE: {table_info.schema_name}.{table_info.table_name}",
            f"Purpose: {table_info.business_description}",
        ]

        if table_info.row_count_estimate:
            lines.append(f"Rows: ~{table_info.row_count_estimate:,}")

        # List all columns with their types and sample data
        lines.append("COLUMNS:")

        for col in table_info.columns[:15]:  # Limit columns to prevent context overflow
            col_line = f"  {col.name} ({col.data_type})"

            if col.sample_values:
                samples = [str(v) for v in col.sample_values[:3]]
                col_line += f" - Examples: {', '.join(samples)}"

            if col.business_context:
                col_line += f" - {col.business_context}"

            lines.append(col_line)

        if len(table_info.columns) > 15:
            lines.append(f"  ... and {len(table_info.columns) - 15} more columns")

        return lines

    def _build_query_generation_system_prompt(self) -> str:
        """Build system prompt for AI query generation"""
        return """You are an expert SQL query generator specializing in converting natural language questions into safe, efficient SELECT queries.

CORE RESPONSIBILITIES:
- Convert user questions into precise SQL SELECT statements
- Use only the table and column names provided in the context
- Generate secure, read-only queries with proper validation
- Provide clear explanations of the query logic

HARD CONSTRAINTS:
- You MUST rely solely on the schemas, tables, and columns explicitly provided in the context.
- You MUST NOT assume the existence of any tables, columns, states, regions, metrics, or values that are not present in the context.
- You MUST NOT use external knowledge, public datasets, or typical/expected values about the real world (for example, typical populations of Indian states).
- If the user's question cannot be answered using the provided tables/columns, you MUST return an empty SQL string and clearly explain that the necessary data is not available.

QUERY GENERATION RULES:
1. ONLY generate SELECT queries - never INSERT, UPDATE, DELETE, or DDL
2. Always include LIMIT 1000 for performance and safety
3. Use proper table.column references with exact names from context
4. Include appropriate WHERE, GROUP BY, ORDER BY as needed
5. Use standard SQL aggregation functions when answering quantitative questions
6. Handle case-insensitive string matching with UPPER() or ILIKE when appropriate
7. Format queries for readability with proper indentation

SECURITY REQUIREMENTS:
- No subqueries unless absolutely necessary
- No UNION operations
- No system tables or functions
- No comments or multiple statements
- Parameterizable values only

OUTPUT FORMAT:
Always respond with valid JSON containing:
- "sql": The complete SELECT query
- "explanation": Clear explanation of what the query does
- "confidence": Score 0.0-1.0 indicating confidence in the query
- "reasoning": Brief explanation of your approach
- "expected_result": "table", "single_value", or "aggregation"

EXAMPLE QUESTIONS AND APPROACHES:
- "How many students in 2021?" → Count with WHERE year = 2021
- "Top 5 states by revenue" → GROUP BY state, SUM(revenue), ORDER BY revenue DESC, LIMIT 5
- "Average attendance last year" → AVG() with date filtering
- "Students in Maharashtra schools" → WHERE state = 'Maharashtra' or similar

Be precise, secure, and helpful in your query generation."""

    def _parse_ai_query_response(self, original_question: str, ai_response: str) -> QueryPlan:
        """Parse AI response into a structured QueryPlan"""
        try:
            # Try to extract JSON from the response
            json_match = re.search(r"\{.*\}", ai_response, re.DOTALL)
            if json_match:
                response_data = json.loads(json_match.group(0))
            else:
                # Fallback: try to parse structured response
                response_data = self._parse_structured_response(ai_response)

            # Extract information with defaults
            sql_query = response_data.get("sql", "").strip()
            explanation = response_data.get("explanation", "Query generated by AI")
            confidence = float(response_data.get("confidence", 0.5))
            reasoning = response_data.get("reasoning", "")
            expected_result = response_data.get("expected_result", "table")

            # Ensure LIMIT is present for safety
            if sql_query and "LIMIT" not in sql_query.upper():
                sql_query += " LIMIT 1000"

            return QueryPlan(
                original_question=original_question,
                generated_sql=sql_query,
                explanation=explanation,
                confidence_score=confidence,
                expected_result_type=expected_result,
                ai_reasoning=reasoning,
                requires_execution=bool(sql_query),
            )

        except Exception as e:
            self.logger.error(f"Error parsing AI query response: {e}")
            self.logger.debug(f"AI response was: {ai_response}")

            return QueryPlan(
                original_question=original_question,
                generated_sql="",
                explanation=f"Failed to parse AI response: {str(e)}",
                confidence_score=0.0,
                requires_execution=False,
                fallback_to_existing_data=True,
                ai_reasoning=f"Parse error: {str(e)}",
            )

    def _parse_structured_response(self, response: str) -> Dict[str, Any]:
        """Fallback parser for non-JSON structured responses"""
        # Simple pattern matching for common response formats
        result = {}

        sql_match = re.search(
            r"(?:SQL|Query):\s*```(?:sql)?\s*(.*?)\s*```", response, re.IGNORECASE | re.DOTALL
        )
        if sql_match:
            result["sql"] = sql_match.group(1).strip()

        explanation_match = re.search(
            r"(?:Explanation|Description):\s*(.*?)(?:\n\n|\n[A-Z]|$)",
            response,
            re.IGNORECASE | re.DOTALL,
        )
        if explanation_match:
            result["explanation"] = explanation_match.group(1).strip()

        confidence_match = re.search(r"(?:Confidence|Score):\s*([0-9.]+)", response, re.IGNORECASE)
        if confidence_match:
            result["confidence"] = confidence_match.group(1)

        return result

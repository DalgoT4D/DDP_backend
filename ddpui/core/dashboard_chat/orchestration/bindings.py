"""Method wiring for the dashboard chat runtime class."""

from . import conversation as conversation_methods
from . import message_stack as message_methods
from . import nodes as node_methods
from . import presentation as presentation_methods
from . import retrieval as retrieval_methods
from . import session_snapshot as snapshot_methods
from . import source_identifiers as source_identifier_methods
from . import sql_execution as sql_execution_methods
from . import sql_parsing as sql_parsing_methods
from . import tool_handlers as tool_handler_methods
from . import tool_loop as tool_loop_methods


def bind_dashboard_chat_runtime_methods(runtime_cls) -> None:
    """Attach graph helper modules onto the runtime class."""
    runtime_cls._node_load_context = node_methods._node_load_context
    runtime_cls._node_route_intent = node_methods._node_route_intent
    runtime_cls._node_handle_small_talk = node_methods._node_handle_small_talk
    runtime_cls._node_handle_irrelevant = node_methods._node_handle_irrelevant
    runtime_cls._node_handle_needs_clarification = node_methods._node_handle_needs_clarification
    runtime_cls._node_handle_query_with_sql = node_methods._node_handle_query_with_sql
    runtime_cls._node_handle_query_without_sql = node_methods._node_handle_query_without_sql
    runtime_cls._node_handle_follow_up_sql = node_methods._node_handle_follow_up_sql
    runtime_cls._node_handle_follow_up_context = node_methods._node_handle_follow_up_context
    runtime_cls._run_intent_tool_loop = node_methods._run_intent_tool_loop
    runtime_cls._node_finalize_response = node_methods._node_finalize_response
    runtime_cls._route_after_intent = staticmethod(node_methods._route_after_intent)

    runtime_cls._build_new_query_messages = message_methods._build_new_query_messages
    runtime_cls._build_follow_up_messages = message_methods._build_follow_up_messages
    runtime_cls._normalize_conversation_history = staticmethod(
        message_methods._normalize_conversation_history
    )

    runtime_cls._execute_tool_loop = tool_loop_methods._execute_tool_loop
    runtime_cls._execute_tool_call = tool_loop_methods._execute_tool_call
    runtime_cls._build_tool_loop_result = tool_loop_methods._build_tool_loop_result

    runtime_cls._handle_retrieve_docs_tool = tool_handler_methods._handle_retrieve_docs_tool
    runtime_cls._handle_get_schema_snippets_tool = (
        tool_handler_methods._handle_get_schema_snippets_tool
    )
    runtime_cls._handle_search_dbt_models_tool = (
        tool_handler_methods._handle_search_dbt_models_tool
    )
    runtime_cls._handle_get_dbt_model_info_tool = (
        tool_handler_methods._handle_get_dbt_model_info_tool
    )
    runtime_cls._handle_get_distinct_values_tool = (
        tool_handler_methods._handle_get_distinct_values_tool
    )
    runtime_cls._handle_list_tables_by_keyword_tool = (
        tool_handler_methods._handle_list_tables_by_keyword_tool
    )
    runtime_cls._handle_check_table_row_count_tool = (
        tool_handler_methods._handle_check_table_row_count_tool
    )
    runtime_cls._get_turn_warehouse_tools = tool_handler_methods._get_turn_warehouse_tools
    runtime_cls._get_cached_schema_snippets = tool_handler_methods._get_cached_schema_snippets
    runtime_cls._seed_distinct_cache_from_previous_sql = (
        tool_handler_methods._seed_distinct_cache_from_previous_sql
    )
    runtime_cls._dbt_resources_by_unique_id = staticmethod(
        tool_handler_methods._dbt_resources_by_unique_id
    )
    runtime_cls._get_cached_query_embedding = tool_handler_methods._get_cached_query_embedding

    runtime_cls._extract_conversation_context = classmethod(
        conversation_methods._extract_conversation_context
    )
    runtime_cls._extract_chart_ids_from_payload = staticmethod(
        conversation_methods._extract_chart_ids_from_payload
    )
    runtime_cls._build_follow_up_context_prompt = classmethod(
        conversation_methods._build_follow_up_context_prompt
    )
    runtime_cls._detect_sql_modification_type = staticmethod(
        conversation_methods._detect_sql_modification_type
    )
    runtime_cls._extract_requested_follow_up_dimension = staticmethod(
        conversation_methods._extract_requested_follow_up_dimension
    )
    runtime_cls._extract_metrics_from_sql = staticmethod(
        conversation_methods._extract_metrics_from_sql
    )
    runtime_cls._extract_dimensions_from_sql = staticmethod(
        conversation_methods._extract_dimensions_from_sql
    )
    runtime_cls._extract_filters_from_sql = staticmethod(
        conversation_methods._extract_filters_from_sql
    )

    runtime_cls._retrieve_vector_documents = retrieval_methods._retrieve_vector_documents
    runtime_cls._filter_allowlisted_dbt_results = staticmethod(
        retrieval_methods._filter_allowlisted_dbt_results
    )
    runtime_cls._dedupe_retrieved_documents = staticmethod(
        retrieval_methods._dedupe_retrieved_documents
    )
    runtime_cls._build_citations = retrieval_methods._build_citations
    runtime_cls._citation_title = staticmethod(retrieval_methods._citation_title)
    runtime_cls._compact_snippet = staticmethod(retrieval_methods._compact_snippet)
    runtime_cls._build_tool_document_payload = retrieval_methods._build_tool_document_payload
    runtime_cls._build_chart_tool_metadata = classmethod(
        retrieval_methods._build_chart_tool_metadata
    )
    runtime_cls._prototype_doc_type = staticmethod(retrieval_methods._prototype_doc_type)
    runtime_cls._chart_metric_columns = classmethod(
        retrieval_methods._chart_metric_columns
    )
    runtime_cls._chart_dimension_columns = classmethod(
        retrieval_methods._chart_dimension_columns
    )
    runtime_cls._chart_time_column = classmethod(retrieval_methods._chart_time_column)
    runtime_cls._looks_like_time_dimension = staticmethod(
        retrieval_methods._looks_like_time_dimension
    )
    runtime_cls._chart_id_from_source_identifier = staticmethod(
        source_identifier_methods.chart_id_from_source_identifier
    )
    runtime_cls._unique_id_from_source_identifier = staticmethod(
        source_identifier_methods.unique_id_from_source_identifier
    )

    runtime_cls._load_session_snapshot = snapshot_methods._load_session_snapshot
    runtime_cls._build_session_snapshot = snapshot_methods._build_session_snapshot
    runtime_cls._persist_session_schema_cache = snapshot_methods._persist_session_schema_cache
    runtime_cls._persist_session_distinct_cache = snapshot_methods._persist_session_distinct_cache

    runtime_cls._validate_sql_allowlist = sql_execution_methods._validate_sql_allowlist
    runtime_cls._run_sql_with_distinct_guard = (
        sql_execution_methods._run_sql_with_distinct_guard
    )
    runtime_cls._missing_columns_in_primary_table = (
        sql_execution_methods._missing_columns_in_primary_table
    )
    runtime_cls._structured_sql_execution_error = (
        sql_execution_methods._structured_sql_execution_error
    )
    runtime_cls._validate_follow_up_dimension_usage = (
        sql_execution_methods._validate_follow_up_dimension_usage
    )
    runtime_cls._missing_distinct = sql_execution_methods._missing_distinct
    runtime_cls._normalize_distinct_value = staticmethod(
        sql_execution_methods._normalize_distinct_value
    )
    runtime_cls._has_validated_distinct_value = classmethod(
        sql_execution_methods._has_validated_distinct_value
    )
    runtime_cls._is_text_type = staticmethod(sql_execution_methods._is_text_type)
    runtime_cls._record_validated_distinct_values = (
        sql_execution_methods._record_validated_distinct_values
    )
    runtime_cls._record_validated_filters_from_sql = (
        sql_execution_methods._record_validated_filters_from_sql
    )

    runtime_cls._primary_table_name = staticmethod(sql_parsing_methods._primary_table_name)
    runtime_cls._table_references = classmethod(sql_parsing_methods._table_references)
    runtime_cls._resolve_table_qualifier = classmethod(
        sql_parsing_methods._resolve_table_qualifier
    )
    runtime_cls._table_columns = staticmethod(sql_parsing_methods._table_columns)
    runtime_cls._tables_with_column = classmethod(sql_parsing_methods._tables_with_column)
    runtime_cls._resolve_identifier_table = classmethod(
        sql_parsing_methods._resolve_identifier_table
    )
    runtime_cls._referenced_sql_identifier_refs = classmethod(
        sql_parsing_methods._referenced_sql_identifier_refs
    )
    runtime_cls._select_aliases = staticmethod(sql_parsing_methods._select_aliases)
    runtime_cls._extract_identifier_refs_from_sql_segment = staticmethod(
        sql_parsing_methods._extract_identifier_refs_from_sql_segment
    )
    runtime_cls._best_table_for_missing_columns = staticmethod(
        sql_parsing_methods._best_table_for_missing_columns
    )
    runtime_cls._extract_text_filter_values = staticmethod(
        sql_parsing_methods._extract_text_filter_values
    )
    runtime_cls._find_tables_with_column = staticmethod(
        sql_parsing_methods._find_tables_with_column
    )
    runtime_cls._structural_dimensions_from_sql = classmethod(
        sql_parsing_methods._structural_dimensions_from_sql
    )
    runtime_cls._normalize_dimension_name = staticmethod(
        sql_parsing_methods._normalize_dimension_name
    )

    runtime_cls._serialize_tool_result = staticmethod(
        presentation_methods._serialize_tool_result
    )
    runtime_cls._summarize_tool_call = presentation_methods._summarize_tool_call
    runtime_cls._max_turns_message = presentation_methods._max_turns_message
    runtime_cls._compose_final_answer_text = presentation_methods._compose_final_answer_text
    runtime_cls._determine_response_format = staticmethod(
        presentation_methods._determine_response_format
    )
    runtime_cls._sql_result_columns = staticmethod(
        presentation_methods._sql_result_columns
    )
    runtime_cls._build_usage_summary = presentation_methods._build_usage_summary
    runtime_cls._compose_small_talk_response = (
        presentation_methods._compose_small_talk_response
    )
    runtime_cls._build_fast_path_intent = staticmethod(
        presentation_methods._build_fast_path_intent
    )
    runtime_cls._build_fast_path_small_talk_response = staticmethod(
        presentation_methods._build_fast_path_small_talk_response
    )
    runtime_cls._clarification_fallback = staticmethod(
        presentation_methods._clarification_fallback
    )
    runtime_cls._fallback_answer_text = staticmethod(
        presentation_methods._fallback_answer_text
    )
    runtime_cls._single_row_summary = staticmethod(
        presentation_methods._single_row_summary
    )
    runtime_cls._humanize_column_name = staticmethod(
        presentation_methods._humanize_column_name
    )
    runtime_cls._normalize_sql_results_for_answer = classmethod(
        presentation_methods._normalize_sql_results_for_answer
    )
    runtime_cls._normalize_sql_value_for_answer = classmethod(
        presentation_methods._normalize_sql_value_for_answer
    )
    runtime_cls._format_numeric_answer_value = classmethod(
        presentation_methods._format_numeric_answer_value
    )
    runtime_cls._parse_numeric_string = staticmethod(
        presentation_methods._parse_numeric_string
    )
    runtime_cls._looks_like_rate_metric = staticmethod(
        presentation_methods._looks_like_rate_metric
    )

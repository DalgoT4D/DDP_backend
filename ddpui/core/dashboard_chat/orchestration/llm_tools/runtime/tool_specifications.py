"""OpenAI tool specifications used by the dashboard chat graph."""

DASHBOARD_CHAT_TOOL_SPECIFICATIONS = [
    {
        "type": "function",
        "function": {
            "name": "get_chart_table_metadata",
            "description": (
                "Fetch the chart registry entries and enriched metadata for chart tables. "
                "Use this first for charts that look relevant from the inline chart registry."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "chart_ids": {
                        "type": "array",
                        "items": {"type": "integer"},
                        "description": "Chart ids from the inline chart registry",
                    },
                    "chart_titles": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Exact chart titles from the inline chart registry",
                    },
                    "table_names": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Preferred chart tables to inspect",
                    },
                },
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "search_metadata",
            "description": (
                "Search the enriched metadata artifact for candidate tables using entity, measure, "
                "grain, time, question-type, and output-shape hints. Use this only after checking "
                "whether the chart tables can fully answer the question."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "question_terms": {
                        "type": "array",
                        "items": {"type": "string"},
                    },
                    "entity_terms": {
                        "type": "array",
                        "items": {"type": "string"},
                    },
                    "measure_terms": {
                        "type": "array",
                        "items": {"type": "string"},
                    },
                    "grain_terms": {
                        "type": "array",
                        "items": {"type": "string"},
                    },
                    "time_terms": {
                        "type": "array",
                        "items": {"type": "string"},
                    },
                    "question_type": {"type": "string"},
                    "required_output_shape": {"type": "string"},
                    "limit": {"type": "integer", "minimum": 1, "maximum": 20, "default": 8},
                },
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_table_metadata",
            "description": "Get full enriched metadata for specific allowlisted tables.",
            "parameters": {
                "type": "object",
                "properties": {
                    "tables": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Fully-qualified allowlisted table names",
                    }
                },
                "required": ["tables"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_column_metadata",
            "description": (
                "Inspect ranked relevant columns within specific tables when you already know the "
                "likely table set. Pass broad concept terms such as entity, name, grade, stage, "
                "measure, threshold, score, percentage, or topic; the tool returns the best "
                "matching columns rather than requiring one column to match every term."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "tables": {
                        "type": "array",
                        "items": {"type": "string"},
                    },
                    "column_terms": {
                        "type": "array",
                        "items": {"type": "string"},
                    },
                    "limit": {"type": "integer", "minimum": 1, "maximum": 120, "default": 80},
                },
                "required": ["tables"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "search_columns_by_name",
            "description": (
                "Find every allowlisted table that contains a specific column name or close column-name match."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "column_name": {"type": "string"},
                    "limit": {"type": "integer", "minimum": 1, "maximum": 50, "default": 20},
                },
                "required": ["column_name"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_join_paths",
            "description": (
                "Get candidate join paths between the currently relevant tables. Use when the question "
                "needs fields that do not exist together on one table."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "tables": {
                        "type": "array",
                        "items": {"type": "string"},
                    },
                    "missing_fields": {
                        "type": "array",
                        "items": {"type": "string"},
                    },
                },
                "required": ["tables"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_table_statistics",
            "description": (
                "Get precomputed table statistics such as row counts, time coverage, and distinct counts."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "tables": {
                        "type": "array",
                        "items": {"type": "string"},
                    }
                },
                "required": ["tables"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_related_tables",
            "description": (
                "From a known starting table set, find related allowlisted tables through the metadata join graph."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "tables": {
                        "type": "array",
                        "items": {"type": "string"},
                    },
                    "entity_terms": {
                        "type": "array",
                        "items": {"type": "string"},
                    },
                    "measure_terms": {
                        "type": "array",
                        "items": {"type": "string"},
                    },
                },
                "required": ["tables"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "resolve_time_scope",
            "description": (
                "Resolve phrases like Q1/Q2/Q3/Q4, this year, this quarter, or fiscal years into explicit dates."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "question_text": {"type": "string"},
                },
                "required": ["question_text"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "read_full_metadata",
            "description": (
                "Read the full enriched metadata artifact for the current dashboard. This is an extreme last resort "
                "when narrower metadata tools still do not provide enough clarity."
            ),
            "parameters": {
                "type": "object",
                "properties": {},
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_schema_snippets",
            "description": "Get warehouse column information for exact tables you intend to query.",
            "parameters": {
                "type": "object",
                "properties": {
                    "tables": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Fully-qualified table names (schema.table)",
                    }
                },
                "required": ["tables"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_distinct_values",
            "description": "Get distinct values for a column before filtering on text columns.",
            "parameters": {
                "type": "object",
                "properties": {
                    "table": {"type": "string", "description": "Fully-qualified table name"},
                    "column": {"type": "string", "description": "Column name"},
                    "limit": {"type": "integer", "minimum": 1, "maximum": 200, "default": 50},
                },
                "required": ["table", "column"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "set_sql_query_plan",
            "description": (
                "Record the intended SQL metric, grain, stage scope, cohort filters, null handling, "
                "and chosen tables before executing complex growth, ranking, threshold, or name-list SQL."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "metric_intent": {"type": "string"},
                    "entity_grain": {"type": "string"},
                    "comparison_axes": {"type": "array", "items": {"type": "string"}},
                    "stage_scope": {"type": "string"},
                    "cohort_filter_stage": {"type": "string"},
                    "required_measure_columns": {"type": "array", "items": {"type": "string"}},
                    "null_handling": {"type": "string"},
                    "disallowed_assumptions": {"type": "array", "items": {"type": "string"}},
                    "candidate_tables": {"type": "array", "items": {"type": "string"}},
                    "chosen_tables": {"type": "array", "items": {"type": "string"}},
                    "why_chosen_tables_answer_directly": {"type": "string"},
                },
                "required": [
                    "metric_intent",
                    "entity_grain",
                    "stage_scope",
                    "cohort_filter_stage",
                    "required_measure_columns",
                    "null_handling",
                    "chosen_tables",
                    "why_chosen_tables_answer_directly",
                ],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "run_sql_query",
            "description": "Execute a read-only SQL query on the database.",
            "parameters": {
                "type": "object",
                "properties": {"sql": {"type": "string", "description": "SELECT query to execute"}},
                "required": ["sql"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "check_table_row_count",
            "description": "Get the total number of rows in a table to check if it has data.",
            "parameters": {
                "type": "object",
                "properties": {
                    "table": {
                        "type": "string",
                        "description": "Fully-qualified table name (schema.table)",
                    }
                },
                "required": ["table"],
            },
        },
    },
]

"""OpenAI tool specifications used by the dashboard chat graph."""

DASHBOARD_CHAT_TOOL_SPECIFICATIONS = [
    {
        "type": "function",
        "function": {
            "name": "retrieve_docs",
            "description": "Search for relevant charts, datasets, dbt models, or context sections.",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "Search query"},
                    "types": {
                        "type": "array",
                        "items": {
                            "type": "string",
                            "enum": ["chart", "dataset", "context", "dbt_model"],
                        },
                        "description": "Document types to search",
                    },
                    "limit": {"type": "integer", "minimum": 1, "maximum": 20, "default": 8},
                },
                "required": ["query"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_schema_snippets",
            "description": "Get column information for database tables.",
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
            "name": "search_dbt_models",
            "description": "Search dbt models by keyword to find relevant data models.",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Search query for model names/descriptions",
                    },
                    "limit": {"type": "integer", "minimum": 1, "maximum": 20, "default": 8},
                },
                "required": ["query"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_dbt_model_info",
            "description": "Get detailed information about a specific dbt model.",
            "parameters": {
                "type": "object",
                "properties": {
                    "model_name": {
                        "type": "string",
                        "description": "Model name or schema.table",
                    }
                },
                "required": ["model_name"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_distinct_values",
            "description": "Get distinct values for a column (required before filtering on text columns).",
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
            "name": "list_tables_by_keyword",
            "description": "Find tables whose name or columns match a keyword (no hard-coding).",
            "parameters": {
                "type": "object",
                "properties": {
                    "keyword": {
                        "type": "string",
                        "description": "Keyword such as donor, funding, student",
                    },
                    "limit": {"type": "integer", "minimum": 1, "maximum": 50, "default": 15},
                },
                "required": ["keyword"],
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

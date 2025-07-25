# Chart API Documentation

## Overview

The Chart API provides endpoints for creating, managing, and visualizing data charts within the Dalgo platform. It supports multiple chart types (bar, pie, line) with both raw and aggregated data computations.

## API Endpoints

### 1. List Charts
**GET** `/api/charts/`

Lists all charts belonging to the user's organization.

**Permissions:** `can_view_chart`

**Response:** Array of Chart objects

---

### 2. Get Chart
**GET** `/api/charts/{chart_id}/`

Retrieves a specific chart by ID.

**Permissions:** `can_view_chart`

**Parameters:**
- `chart_id` (int): Chart unique identifier

**Response:** Chart object

---

### 3. Create Chart
**POST** `/api/charts/`

Creates a new chart with the specified configuration.

**Permissions:** `can_create_chart`

**Request Body:**
```json
{
  "title": "string (required, max 255)",
  "description": "string (optional, max 1000)",
  "chart_type": "bar|pie|line (required)",
  "computation_type": "raw|aggregated (required)",
  "schema_name": "string (required)",
  "table_name": "string (required)",
  "x_axis_column": "string (for raw)",
  "y_axis_column": "string (for raw)",
  "dimension_column": "string (for aggregated)",
  "aggregate_column": "string (for aggregated)",
  "aggregate_function": "sum|avg|count|min|max (for aggregated)",
  "extra_dimension_column": "string (optional)",
  "customizations": {
    "orientation": "horizontal|vertical",
    "stacked": "boolean",
    "showDataLabels": "boolean",
    "xAxisTitle": "string",
    "yAxisTitle": "string",
    "donut": "boolean (for pie)",
    "smooth": "boolean (for line)"
  }
}
```

**Response:** Created Chart object

---

### 4. Update Chart
**PUT** `/api/charts/{chart_id}/`

Updates an existing chart. Only provided fields are updated.

**Permissions:** `can_edit_chart`

**Parameters:**
- `chart_id` (int): Chart unique identifier

**Request Body:**
```json
{
  "title": "string (optional)",
  "description": "string (optional)",
  "customizations": "object (optional)",
  "is_favorite": "boolean (optional)"
}
```

**Response:** Updated Chart object

---

### 5. Delete Chart
**DELETE** `/api/charts/{chart_id}/`

Permanently deletes a chart.

**Permissions:** `can_delete_chart`

**Parameters:**
- `chart_id` (int): Chart unique identifier

**Response:** `{"success": true}`

---

### 6. Get Chart Data
**POST** `/api/charts/chart-data/`

Generates chart data and ECharts configuration based on the provided parameters.

**Permissions:** `can_view_warehouse_data`

**Request Body:**
```json
{
  "chart_type": "bar|pie|line",
  "computation_type": "raw|aggregated",
  "schema_name": "string",
  "table_name": "string",
  "x_axis": "string (for raw)",
  "y_axis": "string (for raw)",
  "dimension_col": "string (for aggregated)",
  "aggregate_col": "string (for aggregated)",
  "aggregate_func": "sum|avg|count|min|max",
  "extra_dimension": "string (optional)",
  "customizations": "object (optional)",
  "offset": "integer (default: 0)",
  "limit": "integer (default: 100, max: 10000)"
}
```

**Response:**
```json
{
  "data": {
    "xAxisData": ["..."],
    "series": [{...}],
    "legend": ["..."]
  },
  "echarts_config": {
    "title": {...},
    "tooltip": {...},
    "xAxis": {...},
    "yAxis": {...},
    "series": [{...}]
  }
}
```

---

### 7. Get Data Preview
**POST** `/api/charts/chart-data-preview/`

Provides a paginated preview of the data for chart configuration.

**Permissions:** `can_view_warehouse_data`

**Request Body:** Same as Get Chart Data endpoint

**Response:**
```json
{
  "columns": ["column1", "column2"],
  "column_types": {"column1": "varchar", "column2": "integer"},
  "data": [{"column1": "value1", "column2": 123}],
  "total_rows": 1000,
  "page": 1,
  "page_size": 50
}
```

## Error Responses

All endpoints return standard HTTP error codes:

- `400 Bad Request`: Invalid input data or query configuration
- `403 Forbidden`: User lacks required permissions
- `404 Not Found`: Resource not found
- `409 Conflict`: Resource already exists
- `500 Internal Server Error`: Server-side error

Error response format:
```json
{
  "detail": "Error message describing the issue"
}
```

## Data Validation

### SQL Identifiers
- Must start with a letter
- Can contain only letters, numbers, and underscores
- Maximum lengths:
  - Schema names: 128 characters
  - Table names: 128 characters
  - Column names: 128 characters

### Chart Configuration
- Chart types: `bar`, `pie`, `line`
- Computation types: `raw`, `aggregated`
- Aggregation functions: `sum`, `avg`, `count`, `min`, `max`

### Input Sanitization
All SQL identifiers are validated against injection patterns and properly escaped before query execution.

## Caching

Chart data results are cached based on a SHA256 hash of the query configuration. Cache entries expire after 1 hour by default.

## Performance Considerations

- Query results are limited to 10,000 rows by default
- Pagination is recommended for large datasets
- Use aggregated queries for better performance with large tables

## Security

- All endpoints require authentication via JWT tokens
- Schema access is validated for each request
- SQL injection prevention through input validation and parameterized queries
- Organization-level data isolation ensures users only access their own data
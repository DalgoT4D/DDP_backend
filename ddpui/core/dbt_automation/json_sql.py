from ddpui.utils.warehouse.client.warehouse_interface import WarehouseType


def json_extract_expression(wtype: str, column: str, key: str) -> str:
    if wtype == WarehouseType.POSTGRES.value:
        safe_key = key.replace("'", "''")
        return f"({column}::json ->> '{safe_key}')"
    if wtype == WarehouseType.BIGQUERY.value:
        safe_key = key.replace("'", "\\'")
        return f"JSON_VALUE({column}, '$.\"{safe_key}\"')"

    raise ValueError(f"Unsupported warehouse type: {wtype}")

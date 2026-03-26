"""Helpers for parsing dashboard-chat vector source identifiers."""


def chart_id_from_source_identifier(source_identifier: str) -> int | None:
    """Extract chart ids from dashboard export source identifiers."""
    parts = source_identifier.split(":")
    if len(parts) >= 4 and parts[-2] == "chart":
        try:
            return int(parts[-1])
        except ValueError:
            return None
    return None


def unique_id_from_source_identifier(source_identifier: str) -> str | None:
    """Extract dbt unique ids from manifest/catalog source identifiers."""
    if ":" not in source_identifier:
        return None
    prefix, unique_id = source_identifier.split(":", 1)
    if prefix not in {"manifest", "catalog"}:
        return None
    return unique_id

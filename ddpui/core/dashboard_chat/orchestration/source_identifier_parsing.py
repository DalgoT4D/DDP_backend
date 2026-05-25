"""Helpers for parsing stored dashboard-chat source identifiers."""


def chart_id_from_source_identifier(source_identifier: str) -> int | None:
    """Extract chart ids from dashboard export source identifiers."""
    parts = source_identifier.split(":")
    if len(parts) >= 4 and parts[-2] == "chart":
        try:
            return int(parts[-1])
        except ValueError:
            return None
    return None

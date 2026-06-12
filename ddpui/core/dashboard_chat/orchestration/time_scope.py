"""Deterministic time-window helpers for dashboard chat generation."""

from __future__ import annotations

import re
from datetime import date
from typing import Any

QUARTER_PATTERN = re.compile(r"\bq([1-4])(?:\s*[-–]\s*q([1-4]))?\b", re.IGNORECASE)
FISCAL_YEAR_PATTERN = re.compile(
    r"\b(?:fy|fiscal\s+year|financial\s+year)\s*"
    r"(20\d{2}|\d{2})\s*[-/]\s*(20\d{2}|\d{2})\b",
    re.IGNORECASE,
)
CALENDAR_YEAR_PATTERN = re.compile(
    r"\b(?:calendar\s+year|in|during|for)\s+(20\d{2})\b",
    re.IGNORECASE,
)
FISCAL_CONTEXT_PATTERN = re.compile(r"\b(?:fy|fiscal|financial)\b", re.IGNORECASE)


def resolve_time_scope(question_text: str, current_date: date) -> list[dict[str, Any]]:
    """Resolve explicit relative, quarter, and fiscal-year windows."""
    lowered = question_text.lower()
    ranges: list[dict[str, Any]] = []

    fiscal_year_match = FISCAL_YEAR_PATTERN.search(lowered)
    quarter_match = QUARTER_PATTERN.search(lowered)
    if quarter_match:
        start_quarter = int(quarter_match.group(1))
        end_quarter = int(quarter_match.group(2) or quarter_match.group(1))
        if fiscal_year_match or FISCAL_CONTEXT_PATTERN.search(lowered):
            fiscal_year_start = _infer_fiscal_year_start(fiscal_year_match, current_date)
            for quarter_number in range(start_quarter, end_quarter + 1):
                start_date, end_date = _fiscal_quarter_dates(fiscal_year_start, quarter_number)
                ranges.append(
                    {
                        "label": f"Q{quarter_number}",
                        "start_date": start_date.isoformat(),
                        "end_date_exclusive": end_date.isoformat(),
                        "calendar_basis": f"fiscal year starting {fiscal_year_start}-04-01",
                    }
                )
            return ranges

        for quarter_number in range(start_quarter, end_quarter + 1):
            start_date, end_date = _calendar_quarter_dates(current_date.year, quarter_number)
            ranges.append(
                {
                    "label": f"Q{quarter_number}",
                    "start_date": start_date.isoformat(),
                    "end_date_exclusive": end_date.isoformat(),
                    "calendar_basis": f"calendar year {current_date.year}",
                }
            )
        return ranges

    if fiscal_year_match:
        fiscal_year_start = _infer_fiscal_year_start(fiscal_year_match, current_date)
        ranges.append(
            {
                "label": f"FY {fiscal_year_start}-{str(fiscal_year_start + 1)[-2:]}",
                "start_date": date(fiscal_year_start, 4, 1).isoformat(),
                "end_date_exclusive": date(fiscal_year_start + 1, 4, 1).isoformat(),
                "calendar_basis": "April-to-March fiscal year",
            }
        )
        return ranges

    if "this year" in lowered or "current year" in lowered:
        start_date = date(current_date.year, 1, 1)
        end_date = date(current_date.year + 1, 1, 1)
        ranges.append(
            {
                "label": "current_calendar_year",
                "start_date": start_date.isoformat(),
                "end_date_exclusive": end_date.isoformat(),
            }
        )

    calendar_year_match = CALENDAR_YEAR_PATTERN.search(lowered)
    if calendar_year_match:
        year = int(calendar_year_match.group(1))
        ranges.append(
            {
                "label": f"calendar_year_{year}",
                "start_date": date(year, 1, 1).isoformat(),
                "end_date_exclusive": date(year + 1, 1, 1).isoformat(),
            }
        )

    if "this month" in lowered or "current month" in lowered:
        start_date = date(current_date.year, current_date.month, 1)
        if current_date.month == 12:
            end_date = date(current_date.year + 1, 1, 1)
        else:
            end_date = date(current_date.year, current_date.month + 1, 1)
        ranges.append(
            {
                "label": "current_calendar_month",
                "start_date": start_date.isoformat(),
                "end_date_exclusive": end_date.isoformat(),
            }
        )

    if "this quarter" in lowered or "current quarter" in lowered:
        quarter_start_month = ((current_date.month - 1) // 3) * 3 + 1
        start_date = date(current_date.year, quarter_start_month, 1)
        if quarter_start_month == 10:
            end_date = date(current_date.year + 1, 1, 1)
        else:
            end_date = date(current_date.year, quarter_start_month + 3, 1)
        ranges.append(
            {
                "label": "current_calendar_quarter",
                "start_date": start_date.isoformat(),
                "end_date_exclusive": end_date.isoformat(),
            }
        )
    return ranges


def build_required_time_scope_prompt(
    question_text: str,
    current_date: date,
) -> str | None:
    """Build a generator constraint for explicit period-bounded questions."""
    ranges = resolve_time_scope(question_text, current_date)
    if not ranges:
        return None

    range_lines = [
        (
            f"- {time_range['label']}: start_date={time_range['start_date']}, "
            f"end_date_exclusive={time_range['end_date_exclusive']}"
            + (
                f", basis={time_range['calendar_basis']}"
                if time_range.get("calendar_basis")
                else ""
            )
        )
        for time_range in ranges
    ]
    return (
        "REQUIRED TIME SCOPE:\n"
        "- The user explicitly asked for a period-bounded answer.\n"
        f"- Today is {current_date.isoformat()}.\n"
        "- Resolve and apply the time window before writing SQL.\n"
        f"{chr(10).join(range_lines)}\n\n"
        "SQL REQUIREMENTS:\n"
        "- Every metric source used to answer this period-bounded question must be filtered "
        "to the resolved window before aggregation.\n"
        "- Use the table metadata primary_filter_time_column, or a clearly equivalent "
        "date/time column from that table, for the direct date filter.\n"
        "- Write the filter as: <time_column> >= DATE '<start_date>' AND "
        "<time_column> < DATE '<end_date_exclusive>'.\n"
        "- If a candidate metric table has no usable time column for this window, do not "
        "use it as the metric source; choose a lower-grain or upstream table that can be "
        "filtered.\n"
        "- Do not call run_sql_query until the SQL includes the direct date filter for the "
        "resolved window."
    )


def _infer_fiscal_year_start(
    fiscal_year_match: re.Match[str] | None,
    current_date: date,
) -> int:
    """Infer the April-start fiscal year base year."""
    if fiscal_year_match is None:
        return current_date.year if current_date.month >= 4 else current_date.year - 1
    start_token = fiscal_year_match.group(1)
    start_year = int(start_token)
    if start_year < 100:
        start_year += 2000
    return start_year


def _calendar_quarter_dates(year: int, quarter_number: int) -> tuple[date, date]:
    """Return inclusive start and exclusive end dates for a calendar quarter."""
    start_month = (quarter_number - 1) * 3 + 1
    end_month = start_month + 3
    start_date = date(year, start_month, 1)
    if end_month == 13:
        return start_date, date(year + 1, 1, 1)
    return start_date, date(year, end_month, 1)


def _fiscal_quarter_dates(fiscal_year_start: int, quarter_number: int) -> tuple[date, date]:
    """Return inclusive start and exclusive end dates for an April-based fiscal quarter."""
    month_lookup = {
        1: (4, 7),
        2: (7, 10),
        3: (10, 13),
        4: (1, 4),
    }
    start_month, end_month = month_lookup[quarter_number]
    if quarter_number == 4:
        start_date = date(fiscal_year_start + 1, start_month, 1)
        end_date = date(fiscal_year_start + 1, end_month, 1)
    else:
        start_date = date(fiscal_year_start, start_month, 1)
        if end_month == 13:
            end_date = date(fiscal_year_start + 1, 1, 1)
        else:
            end_date = date(fiscal_year_start, end_month, 1)
    return start_date, end_date

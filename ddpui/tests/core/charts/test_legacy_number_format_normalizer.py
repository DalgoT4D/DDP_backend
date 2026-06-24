"""Unit tests for the module-level helpers inside the
``migrate_legacy_number_formats`` management command.
"""

from __future__ import annotations

from ddpui.management.commands.migrate_legacy_number_formats import (
    normalize_customizations,
    walk_extra_config,
)


# ── Number chart: full preservation via numberPrefix / numberSuffix ────────


class TestNumberChartPreservation:
    def test_percentage_appends_to_suffix(self):
        cust = {"numberFormat": "percentage"}
        log = normalize_customizations(cust, "number")
        assert cust["numberFormat"] == "default"
        assert cust["numberSuffix"] == "%"
        assert log == ["numberFormat: percentage -> default + suffix '%'"]

    def test_percentage_preserves_existing_suffix(self):
        cust = {"numberFormat": "percentage", "numberSuffix": " of total"}
        normalize_customizations(cust, "number")
        assert cust["numberSuffix"] == " of total%"

    def test_currency_prepends_to_prefix(self):
        cust = {"numberFormat": "currency"}
        log = normalize_customizations(cust, "number")
        assert cust["numberFormat"] == "default"
        assert cust["numberPrefix"] == "$"
        assert log == ["numberFormat: currency -> default + prefix '$'"]

    def test_currency_preserves_existing_prefix(self):
        cust = {"numberFormat": "currency", "numberPrefix": "Rs "}
        normalize_customizations(cust, "number")
        assert cust["numberPrefix"] == "$Rs "

    def test_other_formats_untouched(self):
        cust = {"numberFormat": "indian", "decimalPlaces": 2}
        assert normalize_customizations(cust, "number") == []
        assert cust == {"numberFormat": "indian", "decimalPlaces": 2}

    def test_no_number_format_key_is_noop(self):
        cust = {"decimalPlaces": 2, "numberPrefix": "₹"}
        assert normalize_customizations(cust, "number") == []
        assert cust == {"decimalPlaces": 2, "numberPrefix": "₹"}


# ── Pie chart: currency-only migration ─────────────────────────────────────
#
# Percentage is intentionally LEFT IN PLACE on the pie chart's numberFormat
# slot. The new multiply-by-100 semantic kicks in at render time; values may
# change visually but the format itself remains "percentage".


class TestPieChartCurrencyOnly:
    def test_percentage_left_in_place(self):
        cust = {"numberFormat": "percentage", "decimalPlaces": 1}
        log = normalize_customizations(cust, "pie")
        # Untouched — new multiply-by-100 semantic kicks in at render.
        assert cust["numberFormat"] == "percentage"
        assert log == []

    def test_currency_demoted(self):
        cust = {"numberFormat": "currency"}
        log = normalize_customizations(cust, "pie")
        assert cust["numberFormat"] == "default"
        assert "numberPrefix" not in cust  # no prefix slot on PieChartCustomizations
        assert log == ["numberFormat: currency -> default"]

    def test_pie_label_format_percentage_untouched(self):
        # Pie's labelFormat='percentage' is a SEPARATE field (slice label
        # rendering — show "%" share). Must not be touched.
        cust = {"labelFormat": "percentage", "numberFormat": "indian"}
        normalize_customizations(cust, "pie")
        assert cust["labelFormat"] == "percentage"


# ── Bar / Line: two axis fields ────────────────────────────────────────────


class TestBarLineAxis:
    def test_currency_axis_demoted_percentage_axis_left(self):
        cust = {"xAxisNumberFormat": "percentage", "yAxisNumberFormat": "currency"}
        log = normalize_customizations(cust, "bar")
        # percentage stays put — new multiply-by-100 semantic kicks in
        assert cust["xAxisNumberFormat"] == "percentage"
        assert cust["yAxisNumberFormat"] == "default"
        assert log == ["yAxisNumberFormat: currency -> default"]

    def test_percentage_axes_untouched(self):
        cust = {"xAxisNumberFormat": "percentage", "yAxisNumberFormat": "percentage"}
        log = normalize_customizations(cust, "line")
        assert cust["xAxisNumberFormat"] == "percentage"
        assert cust["yAxisNumberFormat"] == "percentage"
        assert log == []

    def test_top_level_numberFormat_on_bar_untouched(self):
        # Bar uses axis-level keys, not top-level numberFormat.
        cust = {"numberFormat": "percentage"}
        log = normalize_customizations(cust, "bar")
        assert cust["numberFormat"] == "percentage"
        assert log == []


# ── Table: per-column nested ───────────────────────────────────────────────


class TestTableColumnFormatting:
    def test_currency_demoted_percentage_left(self):
        cust = {
            "columnFormatting": {
                "revenue": {"numberFormat": "currency", "decimalPlaces": 2},
                "growth": {"numberFormat": "percentage"},
                "count": {"numberFormat": "indian"},
            }
        }
        log = normalize_customizations(cust, "table")
        assert cust["columnFormatting"]["revenue"]["numberFormat"] == "default"
        # percentage stays put — new multiply-by-100 semantic kicks in
        assert cust["columnFormatting"]["growth"]["numberFormat"] == "percentage"
        assert cust["columnFormatting"]["count"]["numberFormat"] == "indian"
        # Decimal places preserved on the migrated row
        assert cust["columnFormatting"]["revenue"]["decimalPlaces"] == 2
        assert log == ["columnFormatting[revenue].numberFormat: currency -> default"]

    def test_empty_column_formatting(self):
        cust = {"columnFormatting": {}}
        assert normalize_customizations(cust, "table") == []

    def test_no_column_formatting_key(self):
        cust = {"dateColumnFormatting": {}}
        assert normalize_customizations(cust, "table") == []

    def test_malformed_column_entry_skipped(self):
        cust = {"columnFormatting": {"col": "not-a-dict", "ok": {"numberFormat": "currency"}}}
        log = normalize_customizations(cust, "table")
        assert cust["columnFormatting"]["col"] == "not-a-dict"
        assert cust["columnFormatting"]["ok"]["numberFormat"] == "default"
        assert log == ["columnFormatting[ok].numberFormat: currency -> default"]


# ── Map + unknown chart types ──────────────────────────────────────────────


class TestUnhandledTypes:
    def test_map_returns_empty(self):
        cust = {"colorScheme": "Blues"}
        assert normalize_customizations(cust, "map") == []

    def test_unknown_chart_type_returns_empty(self):
        # Unknown type — value untouched (intentional: only handled types
        # get migrated, others stay raw).
        cust = {"numberFormat": "percentage"}
        assert normalize_customizations(cust, "future-chart-type") == []
        assert cust["numberFormat"] == "percentage"


# ── Defensive: bad inputs don't crash ──────────────────────────────────────


class TestDefensive:
    def test_none_customizations(self):
        assert normalize_customizations(None, "number") == []

    def test_non_dict_customizations(self):
        assert normalize_customizations("oops", "number") == []
        assert normalize_customizations([], "number") == []
        assert normalize_customizations(42, "number") == []

    def test_walk_handles_none_extra_config(self):
        assert walk_extra_config(None, "number") == []

    def test_walk_handles_missing_customizations(self):
        assert walk_extra_config({"unrelated": "value"}, "number") == []

    def test_walk_handles_non_dict_customizations(self):
        assert walk_extra_config({"customizations": "oops"}, "number") == []


# ── Idempotency ────────────────────────────────────────────────────────────


class TestIdempotency:
    def test_re_running_is_noop_for_number(self):
        cust = {"numberFormat": "percentage"}
        normalize_customizations(cust, "number")
        first_snapshot = dict(cust)
        log = normalize_customizations(cust, "number")
        assert log == []
        assert cust == first_snapshot

    def test_re_running_is_noop_for_table(self):
        cust = {"columnFormatting": {"x": {"numberFormat": "currency"}}}
        normalize_customizations(cust, "table")
        first_snapshot = {"columnFormatting": dict(cust["columnFormatting"])}
        log = normalize_customizations(cust, "table")
        assert log == []
        assert cust == first_snapshot


# ── walk_extra_config integration ──────────────────────────────────────────


class TestWalkExtraConfig:
    def test_writes_back_to_extra_config(self):
        extra = {
            "metrics": [{"column": "x"}],
            "customizations": {"numberFormat": "percentage"},
        }
        log = walk_extra_config(extra, "number")
        assert extra["customizations"]["numberFormat"] == "default"
        assert extra["customizations"]["numberSuffix"] == "%"
        assert log

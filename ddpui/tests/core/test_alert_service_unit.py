"""Unit tests for Batch 3 AlertService helpers — pure-Python (no DB)."""

import os
import django

import pytest

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from ddpui.core.alerts.alert_service import (
    AlertService,
    _normalize_recipients_for_storage,
)
from ddpui.core.alerts.exceptions import AlertValidationError


# ── _threshold_fires ────────────────────────────────────────────────────────


class TestThresholdFires:
    @pytest.mark.parametrize(
        "value,op,threshold,expected",
        [
            (10, ">", 5, True),
            (10, ">", 10, False),
            (5, "<", 10, True),
            (10, "<", 10, False),
            (10, ">=", 10, True),
            (10, "<=", 10, True),
            (10, "=", 10, True),
            (10, "!=", 5, True),
            (10, "!=", 10, False),
        ],
    )
    def test_comparisons(self, value, op, threshold, expected):
        assert AlertService._threshold_fires(value, op, threshold) is expected

    def test_null_value_does_not_fire(self):
        assert AlertService._threshold_fires(None, ">", 5) is False

    def test_non_numeric_does_not_fire(self):
        assert AlertService._threshold_fires("abc", ">", 5) is False

    def test_unknown_operator_does_not_fire(self):
        assert AlertService._threshold_fires(10, "<>", 5) is False


# ── _alert_triggered_by ─────────────────────────────────────────────────────


class _FakeAlert:
    def __init__(self, triggers):
        self.pipeline_triggers = triggers


class TestAlertTriggeredBy:
    def test_empty_triggers_matches_any_deployment(self):
        alert = _FakeAlert([])
        assert AlertService._alert_triggered_by(alert, "deploy-A") is True

    def test_null_triggers_matches_any_deployment(self):
        alert = _FakeAlert(None)
        assert AlertService._alert_triggered_by(alert, "deploy-A") is True

    def test_explicit_trigger_matches(self):
        alert = _FakeAlert(["deploy-A", "deploy-B"])
        assert AlertService._alert_triggered_by(alert, "deploy-A") is True

    def test_explicit_trigger_does_not_match_unrelated_deployment(self):
        alert = _FakeAlert(["deploy-A", "deploy-B"])
        assert AlertService._alert_triggered_by(alert, "deploy-C") is False


# ── _validate_type_configuration ────────────────────────────────────────────


class _FakeKPI:
    def __init__(self, target_value=100):
        self.target_value = target_value


class _FakeMetric:
    pass


class TestValidateTypeConfiguration:
    def test_rag_requires_kpi(self):
        with pytest.raises(AlertValidationError, match="require a KPI"):
            AlertService._validate_type_configuration(
                alert_type="rag", kpi=None, metric=None, metric_rag_level="red"
            )

    def test_rag_requires_level(self):
        with pytest.raises(AlertValidationError, match="metric_rag_level"):
            AlertService._validate_type_configuration(
                alert_type="rag", kpi=_FakeKPI(), metric=None, metric_rag_level=None
            )

    def test_rag_requires_non_zero_target(self):
        with pytest.raises(AlertValidationError, match="non-zero target"):
            AlertService._validate_type_configuration(
                alert_type="rag",
                kpi=_FakeKPI(target_value=0),
                metric=None,
                metric_rag_level="red",
            )

    def test_rag_valid(self):
        # Should not raise.
        AlertService._validate_type_configuration(
            alert_type="rag", kpi=_FakeKPI(), metric=None, metric_rag_level="amber"
        )

    def test_threshold_requires_metric(self):
        with pytest.raises(AlertValidationError, match="require a Metric"):
            AlertService._validate_type_configuration(
                alert_type="threshold", kpi=None, metric=None, metric_rag_level=None
            )

    def test_threshold_valid(self):
        AlertService._validate_type_configuration(
            alert_type="threshold", kpi=None, metric=_FakeMetric(), metric_rag_level=None
        )

    def test_standalone_rejects_kpi(self):
        with pytest.raises(AlertValidationError, match="must not reference"):
            AlertService._validate_type_configuration(
                alert_type="standalone", kpi=_FakeKPI(), metric=None, metric_rag_level=None
            )

    def test_standalone_rejects_metric(self):
        with pytest.raises(AlertValidationError, match="must not reference"):
            AlertService._validate_type_configuration(
                alert_type="standalone", kpi=None, metric=_FakeMetric(), metric_rag_level=None
            )

    def test_standalone_valid(self):
        AlertService._validate_type_configuration(
            alert_type="standalone", kpi=None, metric=None, metric_rag_level=None
        )

    def test_unknown_type_rejected(self):
        with pytest.raises(AlertValidationError, match="Unknown alert_type"):
            AlertService._validate_type_configuration(
                alert_type="bogus", kpi=None, metric=None, metric_rag_level=None
            )


# ── _normalize_recipients_for_storage ───────────────────────────────────────


class TestNormalizeRecipientsForStorage:
    def test_plain_string_becomes_email_type(self):
        result = _normalize_recipients_for_storage(["a@b.com"])
        assert result == [{"type": "email", "ref": "a@b.com"}]

    def test_typed_dict_passes_through(self):
        result = _normalize_recipients_for_storage(
            [{"type": "email", "ref": "x@y.com"}, {"type": "user", "ref": "42"}]
        )
        assert result == [
            {"type": "email", "ref": "x@y.com"},
            {"type": "user", "ref": "42"},
        ]

    def test_mixed_input(self):
        result = _normalize_recipients_for_storage(
            ["a@b.com", {"type": "user", "ref": "7"}]
        )
        assert result == [
            {"type": "email", "ref": "a@b.com"},
            {"type": "user", "ref": "7"},
        ]

    def test_empty_list(self):
        assert _normalize_recipients_for_storage([]) == []

    def test_invalid_raises(self):
        with pytest.raises(AlertValidationError):
            _normalize_recipients_for_storage([12345])

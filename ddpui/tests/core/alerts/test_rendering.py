"""Tests for ddpui.core.alerts.rendering."""

import os
import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from types import SimpleNamespace
from unittest.mock import MagicMock

from ddpui.core.alerts.rendering import (
    TOKENS_BY_TYPE,
    render,
    resolve_tokens,
    tokens_for_alert,
)
from ddpui.models.alert import AlertType


# ── render() ────────────────────────────────────────────────────────────────


def test_render_substitutes_known_tokens():
    out = render("Hello {{name}}, value is {{value}}", {"name": "Priya", "value": 42})
    assert out == "Hello Priya, value is 42"


def test_render_preserves_unknown_tokens():
    out = render("{{alert_name}} fired at {{unknown}}", {"alert_name": "X"})
    assert out == "X fired at {{unknown}}"


def test_render_preserves_none_or_empty_values():
    """None and empty string should be left as raw tokens — caller didn't supply a value."""
    out = render("{{a}} | {{b}} | {{c}}", {"a": "ok", "b": None, "c": ""})
    assert out == "ok | {{b}} | {{c}}"


def test_render_handles_whitespace_inside_braces():
    out = render("{{ name }}", {"name": "Dalgo"})
    assert out == "Dalgo"


def test_render_handles_empty_template():
    assert render("", {"a": 1}) == ""
    assert render(None, {"a": 1}) == ""


# ── resolve_tokens() ────────────────────────────────────────────────────────


def test_resolve_tokens_metric_threshold():
    tokens = resolve_tokens(
        "metric_threshold",
        alert_name="Daily check",
        metric_name="Active users",
        target_value=50,
        current_value=42,
    )
    assert tokens == {
        "alert_name": "Daily check",
        "metric_name": "Active users",
        "target_value": 50,
        "current_value": 42,
    }
    # No kpi_name / dataset_name pollution
    assert "kpi_name" not in tokens
    assert "dataset_name" not in tokens


def test_resolve_tokens_kpi_rag_includes_rag_status():
    tokens = resolve_tokens(
        "kpi_rag",
        alert_name="KPI watch",
        kpi_name="Signups",
        target_value=1000,
        current_value=900,
        rag_status="amber",
    )
    assert tokens["rag_status"] == "amber"
    assert tokens["kpi_name"] == "Signups"


def test_resolve_tokens_standalone_includes_dataset_name():
    tokens = resolve_tokens(
        "standalone",
        alert_name="Raw count",
        dataset_name="analytics.events",
        target_value=100,
        current_value=120,
    )
    assert tokens["dataset_name"] == "analytics.events"


# ── TOKENS_BY_TYPE ──────────────────────────────────────────────────────────


def test_tokens_by_type_constants_match_frontend_expectations():
    """If this changes, update webapp_v2/types/alerts.ts TOKENS_BY_TYPE too."""
    assert TOKENS_BY_TYPE["metric_threshold"] == [
        "alert_name",
        "metric_name",
        "target_value",
        "current_value",
    ]
    assert TOKENS_BY_TYPE["kpi_rag"] == [
        "alert_name",
        "kpi_name",
        "target_value",
        "current_value",
        "rag_status",
    ]
    assert TOKENS_BY_TYPE["standalone"] == [
        "alert_name",
        "dataset_name",
        "target_value",
        "current_value",
    ]


# ── tokens_for_alert: kpi_rag formatting via customizations ─────────────────


def _kpi_rag_alert(*, extra_config: dict, target_value=10000, kpi_name="Signups"):
    """Build a minimal Alert mock with a kpi_rag type and an attached KPI."""
    kpi = SimpleNamespace(
        name=kpi_name,
        target_value=target_value,
        extra_config=extra_config,
    )
    alert = MagicMock()
    alert.alert_type = AlertType.KPI_RAG
    alert.name = "Signups RAG"
    alert.kpi_id = 42
    alert.kpi = kpi
    alert.metric_id = None
    alert.metric = None
    alert.standalone_config = None
    alert.condition = {}
    return alert


def test_kpi_rag_formats_current_and_target_with_customizations():
    """numberFormat='indian' + prefix='₹' → both values use Indian grouping + ₹."""
    alert = _kpi_rag_alert(
        extra_config={
            "customizations": {
                "numberFormat": "indian",
                "decimalPlaces": 0,
                "numberPrefix": "₹",
                "numberSuffix": "",
            }
        },
        target_value=1234567,
    )
    tokens = tokens_for_alert(alert, current_value=987654, rag_status="amber")
    assert tokens["current_value"] == "₹9,87,654"
    assert tokens["target_value"] == "₹12,34,567"
    assert tokens["rag_status"] == "amber"


def test_kpi_rag_percentage_format_multiplies_by_100():
    """numberFormat='percentage' → multiplies by 100, appends '%'."""
    alert = _kpi_rag_alert(
        extra_config={"customizations": {"numberFormat": "percentage", "decimalPlaces": 2}},
        target_value=1.0,
    )
    tokens = tokens_for_alert(alert, current_value=0.855, rag_status="green")
    assert tokens["current_value"] == "85.50%"
    assert tokens["target_value"] == "100.00%"


def test_kpi_rag_no_customizations_leaves_values_raw():
    """No extra_config → values pass through untouched (raw number)."""
    alert = _kpi_rag_alert(extra_config={}, target_value=10000)
    tokens = tokens_for_alert(alert, current_value=9500)
    # Raw int — render() will str() it at substitution time
    assert tokens["current_value"] == 9500
    assert tokens["target_value"] == 10000


def test_kpi_rag_customizations_without_numberFormat_apply_decimal_places():
    """extra_config.customizations present but no numberFormat → format defaults
    to "No Formatting" so decimal places (and prefix/suffix) still apply.
    Matches frontend formatKPIValue."""
    alert = _kpi_rag_alert(
        extra_config={"customizations": {"decimalPlaces": 2}},
        target_value=10000,
    )
    tokens = tokens_for_alert(alert, current_value=9500)
    assert tokens["current_value"] == "9500.00"
    assert tokens["target_value"] == "10000.00"


def test_kpi_rag_customizations_without_numberFormat_apply_prefix_suffix():
    """Prefix/suffix without numberFormat should still wrap the raw value."""
    alert = _kpi_rag_alert(
        extra_config={"customizations": {"numberPrefix": "₹", "numberSuffix": " total"}},
        target_value=10000,
    )
    tokens = tokens_for_alert(alert, current_value=9500)
    assert tokens["current_value"] == "₹9500 total"
    assert tokens["target_value"] == "₹10000 total"


def test_kpi_rag_none_current_value_preserved_for_unresolved_token():
    """A None current_value must stay None so render() leaves {{current_value}}
    unresolved (avoids rendering literal 'No data' into email bodies)."""
    alert = _kpi_rag_alert(
        extra_config={"customizations": {"numberFormat": "indian"}},
        target_value=10000,
    )
    tokens = tokens_for_alert(alert, current_value=None, rag_status="red")
    assert tokens["current_value"] is None
    # Target still formats because it isn't None
    assert tokens["target_value"] == "10,000"


def test_kpi_rag_with_existing_prefix_suffix_combination():
    """Customizations combining prefix + suffix produce both in the token."""
    alert = _kpi_rag_alert(
        extra_config={
            "customizations": {
                "numberFormat": "default",
                "decimalPlaces": 0,
                "numberPrefix": "",
                "numberSuffix": " users",
            }
        },
        target_value=1000,
    )
    tokens = tokens_for_alert(alert, current_value=857)
    assert tokens["current_value"] == "857 users"
    assert tokens["target_value"] == "1000 users"


def test_metric_threshold_alert_unaffected_by_kpi_formatting_path():
    """Non-kpi_rag alerts must not run the KPI formatting branch even if a KPI
    is somehow attached. The current_value stays raw."""
    alert = MagicMock()
    alert.alert_type = AlertType.METRIC_THRESHOLD
    alert.name = "Metric alert"
    alert.metric_id = 1
    alert.metric = SimpleNamespace(name="Active users")
    alert.kpi_id = None
    alert.kpi = None
    alert.standalone_config = None
    alert.condition = {"value": 100}
    tokens = tokens_for_alert(alert, current_value=42)
    # Raw — no format_number_v2 applied
    assert tokens["current_value"] == 42
    assert tokens["target_value"] == 100
    assert tokens["metric_name"] == "Active users"


def test_kpi_rag_end_to_end_render_matches_card_format():
    """End-to-end: tokens_for_alert + render() yields an email body that
    matches the KPI card render byte-for-byte."""
    alert = _kpi_rag_alert(
        extra_config={
            "customizations": {
                "numberFormat": "indian",
                "decimalPlaces": 0,
                "numberPrefix": "₹",
                "numberSuffix": "",
            }
        },
        target_value=200000,
    )
    tokens = tokens_for_alert(alert, current_value=123456, rag_status="amber")
    body = render(
        "{{kpi_name}} is at {{current_value}} (target {{target_value}}, {{rag_status}})",
        tokens,
    )
    assert body == "Signups is at ₹1,23,456 (target ₹2,00,000, amber)"

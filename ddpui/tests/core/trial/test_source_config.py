import json
import pytest
from unittest.mock import patch
from ddpui.core.trial import source_config


@patch("ddpui.core.trial.source_config.settings")
def test_load_returns_config_for_name(mock_settings, tmp_path):
    f = tmp_path / "creds.json"
    f.write_text(json.dumps({"Postgres warehouse": {"host": "h", "password": "p"}}))
    mock_settings.TEMPLATE_SOURCE_CREDS_FILE = str(f)
    assert source_config.load_template_source_config("Postgres warehouse") == {
        "host": "h",
        "password": "p",
    }
    assert source_config.load_template_source_config("absent") is None


@patch("ddpui.core.trial.source_config.settings")
def test_validate_reports_missing(mock_settings, tmp_path):
    f = tmp_path / "creds.json"
    f.write_text(json.dumps({"A": {}}))
    mock_settings.TEMPLATE_SOURCE_CREDS_FILE = str(f)
    assert source_config.validate_template_source_configs(["A", "B"]) == ["B"]

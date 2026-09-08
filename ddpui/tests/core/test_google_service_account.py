"""MANAGED-SA bridge — tests for the Dalgo-managed Google service-account key.

Delete alongside `ddpui/core/oauth/google_service_account.py` once Google OAuth verification
lands and the bridge is retired.
"""

import json

import pytest

from ddpui.core.oauth import google_service_account
from ddpui.core.oauth.google_oauth_provider import GSHEETS_SOURCE_NAME
from ddpui.ddpairbyte import airbytehelpers

# No django_db mark: every path here is pure config plumbing — env var in, credentials dict
# out — with the one Airbyte lookup monkeypatched. Nothing touches the ORM.

SERVICE_ACCOUNT_EMAIL = "dalgo-gsheets@dalgo-test.iam.gserviceaccount.com"


def _key(**overrides) -> str:
    payload = {
        "type": "service_account",
        "project_id": "dalgo-test",
        "private_key": "-----BEGIN PRIVATE KEY-----\nnot-a-real-key\n-----END PRIVATE KEY-----\n",
        "client_email": SERVICE_ACCOUNT_EMAIL,
    }
    payload.update(overrides)
    return json.dumps(payload)


@pytest.fixture(name="managed_key")
def fixture_managed_key(monkeypatch, tmp_path):
    """Switch the bridge on with a usable key file."""
    keyfile = tmp_path / "sa.json"
    keyfile.write_text(_key(), encoding="utf-8")
    monkeypatch.setenv(google_service_account.MANAGED_SA_PATH_ENV, str(keyfile))
    return _key()


@pytest.fixture(name="bridge_off")
def fixture_bridge_off(monkeypatch):
    """The default deployment: the env var is not set."""
    monkeypatch.delenv(google_service_account.MANAGED_SA_PATH_ENV, raising=False)


def _write_key(monkeypatch, tmp_path, contents: str) -> None:
    """Point the bridge at a key file holding exactly `contents`."""
    keyfile = tmp_path / "sa.json"
    keyfile.write_text(contents, encoding="utf-8")
    monkeypatch.setenv(google_service_account.MANAGED_SA_PATH_ENV, str(keyfile))


# ---------------------------------------------------------------- availability


def test_unavailable_when_no_env_var(bridge_off):  # pylint: disable=unused-argument
    assert google_service_account.managed_service_account_json() is None


def test_available_from_the_key_file(managed_key):  # pylint: disable=unused-argument
    assert google_service_account.managed_service_account_json() == _key()


def test_unavailable_when_the_key_file_is_empty(
    monkeypatch, tmp_path, bridge_off
):  # pylint: disable=unused-argument
    """An empty placeholder file is a real state — the key gets created before it is filled."""
    _write_key(monkeypatch, tmp_path, "")

    assert google_service_account.managed_service_account_json() is None


def test_unavailable_when_the_key_file_is_missing(
    monkeypatch, tmp_path, bridge_off
):  # pylint: disable=unused-argument
    monkeypatch.setenv(
        google_service_account.MANAGED_SA_PATH_ENV, str(tmp_path / "does-not-exist.json")
    )

    assert google_service_account.managed_service_account_json() is None


# ------------------------------------------------------------------- injection
#
# The connector name comes off the request payload — there is no Airbyte lookup — so these
# call the injector directly with the name the frontend would have sent.


def test_injection_is_a_noop_while_the_bridge_is_off(bridge_off):  # pylint: disable=unused-argument
    config = {"spreadsheet_id": "abc"}

    assert airbytehelpers.inject_managed_gsheets_credentials(GSHEETS_SOURCE_NAME, config) == config


def test_injection_fills_in_the_managed_key_for_google_sheets(managed_key):
    result = airbytehelpers.inject_managed_gsheets_credentials(
        GSHEETS_SOURCE_NAME, {"spreadsheet_id": "abc"}
    )

    assert result["spreadsheet_id"] == "abc"
    assert result["credentials"] == {
        "auth_type": "Service",
        "service_account_info": managed_key,
    }


def test_injection_fills_in_an_empty_service_branch(managed_key):
    """What the frontend actually sends on the managed path: the Service discriminator and
    no key."""
    result = airbytehelpers.inject_managed_gsheets_credentials(
        GSHEETS_SOURCE_NAME, {"spreadsheet_id": "abc", "credentials": {"auth_type": "Service"}}
    )

    assert result["credentials"]["service_account_info"] == managed_key


def test_injection_skips_other_connectors(managed_key):  # pylint: disable=unused-argument
    """Every source type shares these endpoints, so a Postgres config must pass through
    untouched even with the bridge on."""
    config = {"host": "localhost"}

    assert airbytehelpers.inject_managed_gsheets_credentials("Postgres", config) == config


def test_injection_skips_a_source_with_no_name(managed_key):  # pylint: disable=unused-argument
    """`sourceDefName` is optional on the payload schema — an older client that omits it gets the
    old behaviour rather than a Google credentials block."""
    config = {"host": "localhost"}

    assert airbytehelpers.inject_managed_gsheets_credentials(None, config) == config


def test_injection_never_trusts_a_client_supplied_key(
    managed_key,
):  # pylint: disable=unused-argument
    """A pasted key is the bring-your-own-key path — it stays the user's, not Dalgo's."""
    own_key = _key(client_email="someone-else@example.iam.gserviceaccount.com")
    config = {"credentials": {"auth_type": "Service", "service_account_info": own_key}}

    result = airbytehelpers.inject_managed_gsheets_credentials(GSHEETS_SOURCE_NAME, config)

    assert result["credentials"]["service_account_info"] == own_key

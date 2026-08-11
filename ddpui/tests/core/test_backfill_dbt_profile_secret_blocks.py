"""Tests for the backfill_dbt_profile_secret_blocks management command.

These deliberately do NOT mock create_or_update_dbt_profile_secret_blk. A Mock
accepts any signature, so mocking it hides exactly the class of bug this file
exists to catch — the command called it with 4 positional args when it takes 3,
and called preprocess_airbyte_creds_for_dbt with 3 when it takes 2. Only the
external boundaries (secretsmanager, Prefect) are mocked, so the real call
chain is exercised.
"""

import json
import os
from unittest.mock import Mock, patch

import django
import pytest

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from django.core.management import call_command

from ddpui.models.org import Org, OrgDbt, OrgPrefectBlockv1, OrgWarehouse

pytestmark = pytest.mark.django_db


@pytest.fixture
def org_with_warehouse():
    """An org with dbt configured and a postgres warehouse, no dataflows."""
    orgdbt = OrgDbt.objects.create(
        gitrepo_url="repo", target_type="postgres", default_schema="intermediate"
    )
    org = Org.objects.create(name="backfill org", slug="backfill-org", dbt=orgdbt)
    warehouse = OrgWarehouse.objects.create(org=org, wtype="postgres", name="wh")
    return org, warehouse


@patch("ddpui.ddpdbt.dbthelpers.prefect_service.upsert_secret_block")
@patch(
    "ddpui.management.commands.backfill_dbt_profile_secret_blocks."
    "secretsmanager.retrieve_warehouse_credentials"
)
def test_backfill_writes_secret_block_for_org(
    mock_retrieve: Mock, mock_upsert: Mock, org_with_warehouse
):
    """The non-dry-run path upserts the Secret block and wires the warehouse FK.

    Regression guard: the command used to pass pre-processed creds plus an
    extras dict (4 positional args) into a 3-arg helper, so every org failed
    with a TypeError while --dry-run still reported success.
    """
    org, warehouse = org_with_warehouse
    mock_retrieve.return_value = {
        "username": "u",
        "password": "pw",
        "host": "h",
        "port": 5432,
        "database": "db",
    }
    mock_upsert.return_value = {
        "block_id": "sec-id-1",
        "block_name": "dbt-profile-backfill-org",
    }

    call_command("backfill_dbt_profile_secret_blocks", org="backfill-org")

    # the deterministic block name reached Prefect
    mock_upsert.assert_called_once()
    assert mock_upsert.call_args.args[0].block_name == "dbt-profile-backfill-org"

    # and the row is wired to the warehouse, which is what migration 0172 added
    block = OrgPrefectBlockv1.objects.get(block_name="dbt-profile-backfill-org")
    warehouse.refresh_from_db()
    assert warehouse.dbt_profile_secret_block == block

    org.dbt.refresh_from_db()
    assert org.dbt.dbt_profile_secret_block == block


@patch("ddpui.ddpdbt.dbthelpers.prefect_service.upsert_secret_block")
@patch(
    "ddpui.management.commands.backfill_dbt_profile_secret_blocks."
    "secretsmanager.retrieve_warehouse_credentials"
)
def test_backfill_passes_raw_airbyte_creds_to_the_helper(
    mock_retrieve: Mock, mock_upsert: Mock, org_with_warehouse
):
    """Creds are mapped once, by the helper — not pre-mapped by the command.

    `username` must survive into the block value as dbt's `user`, and airbyte-only
    keys must be stripped. Double-preprocessing or no preprocessing both break this.
    """
    _, _warehouse = org_with_warehouse
    mock_retrieve.return_value = {
        "username": "u",
        "password": "pw",
        "host": "h",
        "port": 5432,
        "database": "db",
        "ssl_mode": {"mode": "require"},
    }
    mock_upsert.return_value = {
        "block_id": "sec-id-2",
        "block_name": "dbt-profile-backfill-org",
    }

    call_command("backfill_dbt_profile_secret_blocks", org="backfill-org")

    block_value = json.loads(mock_upsert.call_args.args[0].secret)
    assert block_value["wtype"] == "postgres"
    assert block_value["default_schema"] == "intermediate"
    # mapped by preprocess_airbyte_creds_for_dbt inside the helper
    assert block_value["creds"]["user"] == "u"
    assert block_value["creds"]["sslmode"] == "require"
    assert "ssl_mode" not in block_value["creds"]


@patch("ddpui.ddpdbt.dbthelpers.prefect_service.upsert_secret_block")
@patch(
    "ddpui.management.commands.backfill_dbt_profile_secret_blocks."
    "secretsmanager.retrieve_warehouse_credentials"
)
def test_backfill_dry_run_writes_nothing(
    mock_retrieve: Mock, mock_upsert: Mock, org_with_warehouse
):
    """--dry-run must not touch Prefect or the FK."""
    _org, warehouse = org_with_warehouse
    mock_retrieve.return_value = {"username": "u", "password": "pw"}

    call_command("backfill_dbt_profile_secret_blocks", org="backfill-org", dry_run=True)

    mock_upsert.assert_not_called()
    warehouse.refresh_from_db()
    assert warehouse.dbt_profile_secret_block is None

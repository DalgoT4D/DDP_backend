import json
from unittest.mock import patch, Mock
import pytest

from ddpui.models.org import (
    Org,
    OrgPrefectBlockv1,
    OrgWarehouse,
    OrgDbt,
)

from ddpui.ddpdbt.dbthelpers import create_or_update_dbt_profile_secret_blk
from ddpui.ddpprefect import SECRET

pytestmark = pytest.mark.django_db


@patch("ddpui.ddpdbt.dbthelpers.prefect_service.upsert_secret_block")
def test_create_or_update_dbt_profile_secret_blk_creates_row(mock_upsert: Mock):
    """First-time call creates a new SECRET-type OrgPrefectBlockv1 named
    `dbt-profile-<slug>` and wires it to `warehouse.dbt_profile_secret_block`
    (and `org.dbt.dbt_profile_secret_block` when org.dbt is set)."""
    org = Org.objects.create(name="org", slug="org")
    warehouse = OrgWarehouse.objects.create(org=org, wtype="postgres", name="wh")
    orgdbt = OrgDbt.objects.create(
        gitrepo_url="repo", target_type="postgres", default_schema="dbt_schema"
    )
    org.dbt = orgdbt
    org.save()

    mock_upsert.return_value = {"block_id": "sec-id-1", "block_name": "dbt-profile-org"}

    creds = {"username": "u", "password": "pw", "host": "h", "port": 5432}

    create_or_update_dbt_profile_secret_blk(org, warehouse, creds)

    # Prefect API was called with the deterministic block name
    mock_upsert.assert_called_once()
    payload = mock_upsert.call_args.args[0]
    assert payload.block_name == "dbt-profile-org"

    # DB row created — one, of type SECRET
    row = OrgPrefectBlockv1.objects.get(org=org, block_type=SECRET)
    assert row.block_name == "dbt-profile-org"
    assert row.block_id == "sec-id-1"

    # Both FKs point to the new row (warehouse is authoritative; org.dbt mirrored)
    warehouse.refresh_from_db()
    orgdbt.refresh_from_db()
    assert warehouse.dbt_profile_secret_block_id == row.id
    assert orgdbt.dbt_profile_secret_block_id == row.id


@patch("ddpui.ddpdbt.dbthelpers.prefect_service.upsert_secret_block")
def test_create_or_update_dbt_profile_secret_blk_reuses_row(mock_upsert: Mock):
    """Subsequent call reuses the existing OrgPrefectBlockv1 row via update_or_create
    on block_name — no duplicate created, block_id refreshed from the response."""
    org = Org.objects.create(name="org", slug="org")
    warehouse = OrgWarehouse.objects.create(org=org, wtype="postgres", name="wh")
    orgdbt = OrgDbt.objects.create(
        gitrepo_url="repo", target_type="postgres", default_schema="dbt_schema"
    )
    org.dbt = orgdbt
    org.save()

    existing = OrgPrefectBlockv1.objects.create(
        org=org, block_type=SECRET, block_id="sec-id-1", block_name="dbt-profile-org"
    )
    warehouse.dbt_profile_secret_block = existing
    warehouse.save()

    mock_upsert.return_value = {"block_id": "sec-id-2", "block_name": "dbt-profile-org"}

    create_or_update_dbt_profile_secret_blk(org, warehouse, {"username": "u", "password": "pw2"})

    # Still only one row for this org (same name → update_or_create reused it)
    assert OrgPrefectBlockv1.objects.filter(org=org, block_type=SECRET).count() == 1
    existing.refresh_from_db()
    assert existing.block_id == "sec-id-2"  # updated to the new Prefect block id


@patch("ddpui.ddpdbt.dbthelpers.prefect_service.upsert_secret_block")
def test_create_or_update_dbt_profile_secret_blk_no_orgdbt(mock_upsert: Mock):
    """When org.dbt is None: still upserts the block; default_schema is derived
    from airbyte_creds (postgres → creds['schema'], bigquery → creds['dataset_id']).
    warehouse.dbt_profile_secret_block is still set; org.dbt.* is skipped (no orgdbt)."""
    org = Org.objects.create(name="org", slug="org")
    warehouse = OrgWarehouse.objects.create(org=org, wtype="postgres", name="wh")

    mock_upsert.return_value = {"block_id": "sec-id-1", "block_name": "dbt-profile-org"}

    creds = {"username": "u", "password": "pw", "schema": "creds_schema"}

    create_or_update_dbt_profile_secret_blk(org, warehouse, creds)

    mock_upsert.assert_called_once()
    row = OrgPrefectBlockv1.objects.get(org=org, block_type=SECRET)
    warehouse.refresh_from_db()
    assert warehouse.dbt_profile_secret_block_id == row.id


@patch("ddpui.ddpdbt.dbthelpers.prefect_service.upsert_secret_block")
def test_secret_payload_shape_postgres(mock_upsert: Mock):
    """SECRET block stores {wtype, default_schema, creds, extras} JSON — the
    exact contract read by proxy/_run_post_sync_ops. If any top-level key is
    renamed, the runner blows up at flow-run time."""
    org = Org.objects.create(name="org", slug="org")
    warehouse = OrgWarehouse.objects.create(org=org, wtype="postgres", name="wh")
    orgdbt = OrgDbt.objects.create(
        gitrepo_url="repo", target_type="postgres", default_schema="prod_schema"
    )
    org.dbt = orgdbt
    org.save()
    mock_upsert.return_value = {"block_id": "sec-id", "block_name": "dbt-profile-org"}

    creds = {
        "host": "h",
        "port": 5432,
        "username": "u",
        "password": "pw",
        "database": "db",
        "schema": "creds_schema",
    }
    create_or_update_dbt_profile_secret_blk(org, warehouse, creds)

    edit_payload = mock_upsert.call_args.args[0]
    assert edit_payload.block_name == "dbt-profile-org"
    value = json.loads(edit_payload.secret)
    assert set(value.keys()) == {"wtype", "default_schema", "creds", "extras"}
    assert value["wtype"] == "postgres"
    assert value["default_schema"] == "prod_schema"  # from org.dbt, not from creds
    # dbt-postgres uses `user` not `username`
    assert value["creds"]["user"] == "u"
    assert value["creds"]["password"] == "pw"
    assert value["extras"] == {}


@patch("ddpui.ddpdbt.dbthelpers.prefect_service.upsert_secret_block")
def test_secret_payload_shape_bigquery(mock_upsert: Mock):
    """BigQuery: extras carry location + priority; creds is the service-account dict."""
    org = Org.objects.create(name="org", slug="org")
    warehouse = OrgWarehouse.objects.create(org=org, wtype="bigquery", name="wh")
    orgdbt = OrgDbt.objects.create(
        gitrepo_url="repo", target_type="bigquery", default_schema="prod_dataset"
    )
    org.dbt = orgdbt
    org.save()
    mock_upsert.return_value = {"block_id": "sec-id", "block_name": "dbt-profile-org"}

    creds = {
        "project_id": "my-proj",
        "private_key": "k",
        "client_email": "svc@my-proj.iam",
        "dataset_location": "US",
        "transformation_priority": "batch",
    }
    create_or_update_dbt_profile_secret_blk(org, warehouse, creds)

    value = json.loads(mock_upsert.call_args.args[0].secret)
    assert value["wtype"] == "bigquery"
    assert value["default_schema"] == "prod_dataset"
    assert value["extras"] == {"location": "US", "priority": "batch"}
    # dataset_location / transformation_priority moved to extras, not in creds
    assert "dataset_location" not in value["creds"]
    assert "transformation_priority" not in value["creds"]

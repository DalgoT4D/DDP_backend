from unittest.mock import patch, Mock
import pytest
import os
from pathlib import Path
import yaml

from ddpui.models.org import (
    Org,
    OrgPrefectBlockv1,
    OrgWarehouse,
    OrgDbt,
    TransformType,
)

from ddpui.ddpdbt.dbthelpers import create_or_update_org_cli_block
from ddpui.ddpprefect import DBTCLIPROFILE

pytestmark = pytest.mark.django_db


@patch(
    "ddpui.ddpprefect.prefect_service.create_dbt_cli_profile_block",
    mock_create_dbt_cli_profile_block=Mock(),
)
def test_create_or_update_org_cli_block_create_case(
    mock_create_dbt_cli_profile_block: Mock,
):
    """test create_or_update_org_cli_block when its created for the first time"""
    org = Org.objects.create(name="org", slug="org")
    warehouse = OrgWarehouse.objects.create(org=org, wtype="postgres", name="name")
    orgdbt = OrgDbt.objects.create(gitrepo_url="A", target_type="B", default_schema="C")
    org.dbt = orgdbt
    org.save()

    mock_create_dbt_cli_profile_block.return_value = {
        "block_id": "some_id",
        "block_name": "some_name",
    }

    dummy_creds = {
        "username": "username",
        "password": "password",
        "host": "host",
        "port": "port",
        "database": "database",
    }

    create_or_update_org_cli_block(org, warehouse, dummy_creds)

    org_cli_block = OrgPrefectBlockv1.objects.filter(org=org, block_type=DBTCLIPROFILE).first()
    assert org_cli_block is not None
    assert org_cli_block.block_id == "some_id"
    assert org_cli_block.block_name == "some_name"


@patch(
    "ddpui.ddpprefect.prefect_service.update_dbt_cli_profile_block",
    mock_update_dbt_cli_profile_block=Mock(),
)
def test_create_or_update_org_cli_block_update_case(
    mock_update_dbt_cli_profile_block: Mock, tmp_path
):
    """test create_or_update_org_cli_block when the block is updated"""
    os.environ["CLIENTDBT_ROOT"] = str(tmp_path)
    org = Org.objects.create(name="org", slug="org")
    warehouse = OrgWarehouse.objects.create(org=org, wtype="postgres", name="name")

    mock_update_dbt_cli_profile_block.return_value = {
        "block_id": "some_id",
        "block_name": "some_name",
    }

    dummy_creds = {
        "username": "username",
        "password": "password",
        "host": "host",
        "port": "port",
        "database": "database",
    }
    cli_profile_block = OrgPrefectBlockv1.objects.create(
        org=org,
        block_type=DBTCLIPROFILE,
        block_id="some_id",
        block_name="some_name",
    )
    project_name = "dbtrepo"

    project_dir = Path(tmp_path) / org.slug
    project_dir.mkdir(parents=True, exist_ok=True)
    dbtrepo_dir = project_dir / project_name
    dbtrepo_dir.mkdir(parents=True, exist_ok=True)
    dbt = OrgDbt.objects.create(
        project_dir=f"{org.slug}/{project_name}",
        dbt_venv=str(tmp_path),
        target_type="postgres",
        default_schema="default",
        transform_type=TransformType.GIT,
        cli_profile_block=cli_profile_block,
    )
    org.dbt = dbt
    org.save()

    # create dbt_project.yml file
    yml_obj = {"profile": "dummy"}
    with open(str(dbtrepo_dir / "dbt_project.yml"), "w", encoding="utf-8") as output:
        yaml.safe_dump(yml_obj, output)

    create_or_update_org_cli_block(org, warehouse, dummy_creds)

    mock_update_dbt_cli_profile_block.assert_called_once_with(
        block_name=cli_profile_block.block_name,
        wtype=warehouse.wtype,
        credentials=dummy_creds,
        bqlocation=None,
        profilename=yml_obj["profile"],
        target="default",
        priority=None,
    )

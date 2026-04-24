"""Tests for generate_manifest_json_for_dbt_project function."""

import pytest
import json
import os
import yaml
from pathlib import Path
from unittest.mock import patch, Mock
from ddpui.ddpdbt.dbt_service import generate_manifest_json_for_dbt_project
from ddpui.core.orgdbt_manager import DbtProjectManager, DbtCommandError
from ddpui.models.org import Org, OrgDbt, OrgPrefectBlockv1
from ddpui.ddpprefect import DBTCLIPROFILE


@pytest.fixture()
def org_with_dbt_workspace(tmpdir_factory):
    """a pytest fixture which creates an Org having an dbt workspace"""
    print("creating org_with_dbt_workspace")
    org_slug = "test-org-slug"
    client_dir = tmpdir_factory.mktemp("clients")
    org_dir = client_dir.mkdir(org_slug)
    org_dir.mkdir("dbtrepo")

    os.environ["CLIENTDBT_ROOT"] = str(client_dir)

    # create dbt_project.yml file
    yml_obj = {"profile": "dummy"}
    with open(str(org_dir / "dbtrepo" / "dbt_project.yml"), "w", encoding="utf-8") as output:
        yaml.safe_dump(yml_obj, output)

    dbt = OrgDbt.objects.create(
        gitrepo_url="dummy-git-url.github.com",
        project_dir="tmp/",
        dbt_venv=tmpdir_factory.mktemp("venv"),
        target_type="postgres",
        default_schema="prod",
    )
    org = Org.objects.create(
        airbyte_workspace_id="FAKE-WORKSPACE-ID-1",
        slug=org_slug,
        dbt=dbt,
        name=org_slug,
    )
    cli_profile_block = OrgPrefectBlockv1.objects.create(
        org=org,
        block_type=DBTCLIPROFILE,
        block_name="test-cli-block",
        block_id="test-block-id",
    )
    dbt.cli_profile_block = cli_profile_block
    dbt.save()
    yield org
    print("deleting org_with_dbt_workspace")
    org.delete()


@pytest.mark.django_db
def test_generate_manifest_no_cli_profile_block(org_with_dbt_workspace: Org):
    """Test that exception is raised when CLI profile block is missing"""

    org_with_dbt_workspace.dbt.cli_profile_block = None

    with pytest.raises(Exception) as exc_info:
        generate_manifest_json_for_dbt_project(org_with_dbt_workspace, org_with_dbt_workspace.dbt)

    assert "DBT CLI profile block not found" in str(exc_info.value)


@pytest.mark.django_db
def test_generate_manifest_success(org_with_dbt_workspace: Org):
    """Test successful manifest generation"""
    # Create CLI profile block

    # Mock the subprocess call to simulate successful dbt docs generation
    mock_manifest = {"metadata": {"project_name": "test_project"}}

    # Create the manifest file in tmp_path
    dbtrepo_dir = DbtProjectManager.get_dbt_project_dir(org_with_dbt_workspace.dbt)
    manifest_path = Path(dbtrepo_dir) / "target" / "manifest.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(mock_manifest))

    with patch(
        "ddpui.ddpdbt.dbt_service.prefect_service.get_dbt_cli_profile_block"
    ) as mock_get_profile, patch(
        "ddpui.ddpdbt.dbt_service.DbtProjectManager.run_dbt_command"
    ) as mock_run_dbt:
        # Mock the profile block content
        mock_get_profile.return_value = {
            "profile": {
                "test_profile": {
                    "outputs": {"public": {"type": "postgres", "host": "localhost", "port": 5432}}
                }
            }
        }

        result = generate_manifest_json_for_dbt_project(
            org_with_dbt_workspace, org_with_dbt_workspace.dbt
        )

        # Verify dbt commands were called (deps and compile)
        assert mock_run_dbt.call_count == 2
        # First call should be deps
        deps_call = mock_run_dbt.call_args_list[0]
        assert "deps" in deps_call[1]["command"]
        # Second call should be compile
        compile_call = mock_run_dbt.call_args_list[1]
        assert "compile" in compile_call[1]["command"]

        # Verify manifest was read and returned
        assert result == mock_manifest


@pytest.mark.django_db
def test_generate_manifest_error(org_with_dbt_workspace: Org):
    """Test error handling when dbt docs generate fails"""

    with patch(
        "ddpui.ddpdbt.dbt_service.prefect_service.get_dbt_cli_profile_block"
    ) as mock_get_profile, patch(
        "ddpui.ddpdbt.dbt_service.DbtProjectManager.run_dbt_command"
    ) as mock_run_dbt:
        # Mock the profile block content
        mock_get_profile.return_value = {
            "profile": {
                "test_profile": {
                    "outputs": {"public": {"type": "postgres", "host": "localhost", "port": 5432}}
                }
            }
        }

        # Mock dbt command to fail
        mock_run_dbt.side_effect = DbtCommandError("dbt deps failed", "command failed")

        with pytest.raises(Exception) as exc_info:
            generate_manifest_json_for_dbt_project(
                org_with_dbt_workspace, org_with_dbt_workspace.dbt
            )

        assert "dbt deps failed" in str(exc_info.value)

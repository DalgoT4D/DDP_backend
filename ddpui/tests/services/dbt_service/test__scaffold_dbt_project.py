"""Tests for _scaffold_dbt_project function."""

import pytest
import subprocess
from pathlib import Path
from unittest.mock import patch, Mock
from ddpui.ddpdbt.dbt_service import _scaffold_dbt_project
from ddpui.models.org import Org, OrgDbt


@pytest.mark.django_db
def test_scaffold_dbt_project_success(tmp_path):
    """Test successful dbt project scaffolding"""
    project_name = "test_project"
    dbtrepo_dir = tmp_path / project_name

    # Create test org and orgdbt
    org = Org.objects.create(name="test", slug="test")
    orgdbt = OrgDbt.objects.create(project_dir=str(dbtrepo_dir), dbt_venv="test_venv")

    # Mock DbtProjectManager.run_dbt_command to succeed
    with patch(
        "ddpui.ddpdbt.dbt_service.DbtProjectManager.run_dbt_command"
    ) as mock_run_command, patch("ddpui.ddpdbt.dbt_service.shutil.copy") as mock_copy, patch(
        "ddpui.ddpdbt.dbt_service.glob.glob"
    ) as mock_glob, patch(
        "ddpui.ddpdbt.dbt_service.shutil.rmtree"
    ) as mock_rmtree:
        # Setup mocks
        mock_glob.return_value = ["/fake/assets/macro1.sql", "/fake/assets/macro2.sql"]

        # Create the dbtrepo directory
        dbtrepo_dir.mkdir(parents=True, exist_ok=True)
        example_models_dir = dbtrepo_dir / "models" / "example"
        example_models_dir.mkdir(parents=True, exist_ok=True)

        # Call the function
        _scaffold_dbt_project(org, orgdbt, project_name, dbtrepo_dir)

        # Verify dbt init was called correctly
        mock_run_command.assert_called_once_with(
            org,
            orgdbt,
            ["init", project_name],
            cwd=str(dbtrepo_dir.parent),
            flags=["--skip-profile-setup"],
        )

        # Verify example models directory was removed
        mock_rmtree.assert_called_once_with(example_models_dir)

        # Verify packages.yml and macros were copied
        assert mock_copy.call_count >= 1  # At least packages.yml was copied

        # Verify SQL files were processed
        mock_glob.assert_called_once()


@pytest.mark.django_db
def test_scaffold_dbt_project_dbt_init_fails(tmp_path):
    """Test dbt init failure during scaffolding"""
    project_name = "test_project"
    dbtrepo_dir = tmp_path / project_name

    # Create test org and orgdbt
    org = Org.objects.create(name="test", slug="test")
    orgdbt = OrgDbt.objects.create(project_dir=str(dbtrepo_dir), dbt_venv="test_venv")

    # Mock DbtProjectManager.run_dbt_command to fail
    with patch("ddpui.ddpdbt.dbt_service.DbtProjectManager.run_dbt_command") as mock_run_command:
        mock_run_command.side_effect = subprocess.CalledProcessError(1, "dbt init")

        dbtrepo_dir.mkdir(parents=True, exist_ok=True)

        # Call the function and expect it to fail
        with pytest.raises(Exception, match="dbt init failed"):
            _scaffold_dbt_project(org, orgdbt, project_name, dbtrepo_dir)


@pytest.mark.django_db
def test_scaffold_dbt_project_asset_copy_fails(tmp_path):
    """Test asset file copying failure during scaffolding"""
    project_name = "test_project"
    dbtrepo_dir = tmp_path / project_name

    # Create test org and orgdbt
    org = Org.objects.create(name="test", slug="test")
    orgdbt = OrgDbt.objects.create(project_dir=str(dbtrepo_dir), dbt_venv="test_venv")

    # Mock successful dbt init but failing asset copy
    with patch(
        "ddpui.ddpdbt.dbt_service.DbtProjectManager.run_dbt_command"
    ) as mock_run_command, patch("ddpui.ddpdbt.dbt_service.shutil.copy") as mock_copy:
        # Make copy fail
        mock_copy.side_effect = Exception("Copy failed")

        dbtrepo_dir.mkdir(parents=True, exist_ok=True)

        # Call the function and expect it to fail
        with pytest.raises(Exception, match="Something went wrong while copying asset files"):
            _scaffold_dbt_project(org, orgdbt, project_name, dbtrepo_dir)

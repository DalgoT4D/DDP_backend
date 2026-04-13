"""Tests for update_github_pat_storage function."""

import pytest
from unittest.mock import patch
from ddpui.ddpdbt.dbt_service import update_github_pat_storage
from ddpui.models.org import Org, OrgPrefectBlockv1
from ddpui.ddpprefect import SECRET


@patch("ddpui.ddpdbt.dbt_service.secretsmanager")
@patch("ddpui.ddpdbt.dbt_service.prefect_service")
@patch("ddpui.ddpdbt.dbt_service.GitManager.generate_oauth_url_static")
@pytest.mark.django_db
def test_update_github_pat_storage_new_pat_creation(
    mock_generate_oauth_url, mock_prefect_service, mock_secretsmanager
):
    """Test creating a new PAT when no existing secret exists"""
    # Setup
    org = Org.objects.create(name="test-org", slug="test-org")
    git_repo_url = "https://github.com/test/repo.git"
    access_token = "test-pat-token"

    # Mock responses
    mock_generate_oauth_url.return_value = "https://oauth2:test-pat-token@github.com/test/repo.git"
    mock_prefect_service.upsert_secret_block.return_value = {"block_id": "test-block-id"}
    mock_secretsmanager.save_github_pat.return_value = "new-secret-key"

    # Execute
    result = update_github_pat_storage(org, git_repo_url, access_token)

    # Verify
    assert result == "new-secret-key"

    # Verify OAuth URL generation
    mock_generate_oauth_url.assert_called_once_with(git_repo_url, access_token)

    # Verify Prefect secret block creation
    mock_prefect_service.upsert_secret_block.assert_called_once()
    call_args = mock_prefect_service.upsert_secret_block.call_args[0][0]
    assert call_args.block_name == "test-org-git-pull-url"
    assert call_args.secret == "https://oauth2:test-pat-token@github.com/test/repo.git"

    # Verify OrgPrefectBlockv1 creation
    created_block = OrgPrefectBlockv1.objects.get(
        org=org, block_type=SECRET, block_name="test-org-git-pull-url"
    )
    assert created_block.block_id == "test-block-id"

    # Verify secrets manager new PAT creation
    mock_secretsmanager.save_github_pat.assert_called_once_with(access_token)
    mock_secretsmanager.update_github_pat.assert_not_called()


@patch("ddpui.ddpdbt.dbt_service.secretsmanager")
@patch("ddpui.ddpdbt.dbt_service.prefect_service")
@patch("ddpui.ddpdbt.dbt_service.GitManager.generate_oauth_url_static")
@pytest.mark.django_db
def test_update_github_pat_storage_existing_pat_update(
    mock_generate_oauth_url, mock_prefect_service, mock_secretsmanager
):
    """Test updating an existing PAT when secret already exists"""
    # Setup
    org = Org.objects.create(name="test-org", slug="test-org")
    git_repo_url = "https://github.com/test/repo.git"
    access_token = "updated-pat-token"
    existing_secret = "existing-secret-key"

    # Mock responses
    mock_generate_oauth_url.return_value = (
        "https://oauth2:updated-pat-token@github.com/test/repo.git"
    )
    mock_prefect_service.upsert_secret_block.return_value = {"block_id": "test-block-id"}

    # Execute
    result = update_github_pat_storage(org, git_repo_url, access_token, existing_secret)

    # Verify
    assert result == existing_secret

    # Verify OAuth URL generation
    mock_generate_oauth_url.assert_called_once_with(git_repo_url, access_token)

    # Verify Prefect secret block update
    mock_prefect_service.upsert_secret_block.assert_called_once()
    call_args = mock_prefect_service.upsert_secret_block.call_args[0][0]
    assert call_args.block_name == "test-org-git-pull-url"
    assert call_args.secret == "https://oauth2:updated-pat-token@github.com/test/repo.git"

    # Verify secrets manager PAT update (not creation)
    mock_secretsmanager.update_github_pat.assert_called_once_with(existing_secret, access_token)
    mock_secretsmanager.save_github_pat.assert_not_called()


@patch("ddpui.ddpdbt.dbt_service.secretsmanager")
@patch("ddpui.ddpdbt.dbt_service.prefect_service")
@patch("ddpui.ddpdbt.dbt_service.GitManager.generate_oauth_url_static")
@pytest.mark.django_db
def test_update_github_pat_storage_prefect_block_already_exists(
    mock_generate_oauth_url, mock_prefect_service, mock_secretsmanager
):
    """Test when Prefect secret block already exists in database"""
    # Setup
    org = Org.objects.create(name="test-org", slug="test-org")
    git_repo_url = "https://github.com/test/repo.git"
    access_token = "test-pat-token"

    # Pre-create the OrgPrefectBlockv1 record
    existing_block = OrgPrefectBlockv1.objects.create(
        org=org,
        block_type=SECRET,
        block_name="test-org-git-pull-url",
        block_id="existing-block-id",
    )

    # Mock responses
    mock_generate_oauth_url.return_value = "https://oauth2:test-pat-token@github.com/test/repo.git"
    mock_prefect_service.upsert_secret_block.return_value = {"block_id": "updated-block-id"}
    mock_secretsmanager.save_github_pat.return_value = "new-secret-key"

    # Execute
    result = update_github_pat_storage(org, git_repo_url, access_token)

    # Verify
    assert result == "new-secret-key"

    # Verify Prefect service was still called
    mock_prefect_service.upsert_secret_block.assert_called_once()

    # Verify no new OrgPrefectBlockv1 was created (count should still be 1)
    blocks_count = OrgPrefectBlockv1.objects.filter(
        org=org, block_type=SECRET, block_name="test-org-git-pull-url"
    ).count()
    assert blocks_count == 1

    # Verify the existing block is still there
    existing_block.refresh_from_db()
    assert existing_block.block_id == "existing-block-id"  # Should not be updated


@patch("ddpui.ddpdbt.dbt_service.secretsmanager")
@patch("ddpui.ddpdbt.dbt_service.prefect_service")
@patch("ddpui.ddpdbt.dbt_service.GitManager.generate_oauth_url_static")
@pytest.mark.django_db
def test_update_github_pat_storage_prefect_service_error(
    mock_generate_oauth_url, mock_prefect_service, mock_secretsmanager
):
    """Test error handling when Prefect service fails"""
    # Setup
    org = Org.objects.create(name="test-org", slug="test-org")
    git_repo_url = "https://github.com/test/repo.git"
    access_token = "test-pat-token"

    # Mock responses
    mock_generate_oauth_url.return_value = "https://oauth2:test-pat-token@github.com/test/repo.git"
    mock_prefect_service.upsert_secret_block.side_effect = Exception("Prefect service error")

    # Execute and verify exception is raised
    with pytest.raises(Exception, match="Prefect service error"):
        update_github_pat_storage(org, git_repo_url, access_token)

    # Verify secrets manager was not called since Prefect failed
    mock_secretsmanager.save_github_pat.assert_not_called()
    mock_secretsmanager.update_github_pat.assert_not_called()


@patch("ddpui.ddpdbt.dbt_service.secretsmanager")
@patch("ddpui.ddpdbt.dbt_service.prefect_service")
@patch("ddpui.ddpdbt.dbt_service.GitManager.generate_oauth_url_static")
@pytest.mark.django_db
def test_update_github_pat_storage_secretsmanager_error(
    mock_generate_oauth_url, mock_prefect_service, mock_secretsmanager
):
    """Test error handling when secrets manager fails"""
    # Setup
    org = Org.objects.create(name="test-org", slug="test-org")
    git_repo_url = "https://github.com/test/repo.git"
    access_token = "test-pat-token"

    # Mock responses
    mock_generate_oauth_url.return_value = "https://oauth2:test-pat-token@github.com/test/repo.git"
    mock_prefect_service.upsert_secret_block.return_value = {"block_id": "test-block-id"}
    mock_secretsmanager.save_github_pat.side_effect = Exception("Secrets manager error")

    # Execute and verify exception is raised
    with pytest.raises(Exception, match="Secrets manager error"):
        update_github_pat_storage(org, git_repo_url, access_token)

    # Verify Prefect service was called (it succeeded)
    mock_prefect_service.upsert_secret_block.assert_called_once()


@patch("ddpui.ddpdbt.dbt_service.secretsmanager")
@patch("ddpui.ddpdbt.dbt_service.prefect_service")
@patch("ddpui.ddpdbt.dbt_service.GitManager.generate_oauth_url_static")
@pytest.mark.django_db
def test_update_github_pat_storage_oauth_url_generation_error(
    mock_generate_oauth_url, mock_prefect_service, mock_secretsmanager
):
    """Test error handling when OAuth URL generation fails"""
    # Setup
    org = Org.objects.create(name="test-org", slug="test-org")
    git_repo_url = "https://github.com/test/repo.git"
    access_token = "test-pat-token"

    # Mock responses
    mock_generate_oauth_url.side_effect = Exception("OAuth URL generation failed")

    # Execute and verify exception is raised
    with pytest.raises(Exception, match="OAuth URL generation failed"):
        update_github_pat_storage(org, git_repo_url, access_token)

    # Verify subsequent services were not called
    mock_prefect_service.upsert_secret_block.assert_not_called()
    mock_secretsmanager.save_github_pat.assert_not_called()

"""Tests for clear_github_pat_storage function."""

import pytest
from unittest.mock import patch, Mock
from ddpui.ddpdbt.dbt_service import clear_github_pat_storage
from ddpui.models.org import Org, OrgPrefectBlockv1
from ddpui.ddpprefect import SECRET


@pytest.mark.django_db
def test_clear_github_pat_storage_both_storage_types():
    """Test clearing both Prefect secret block and secrets manager PAT"""
    # Setup
    org = Org.objects.create(name="test-org", slug="test-org")

    # Create existing Prefect secret block
    secret_block = OrgPrefectBlockv1.objects.create(
        org=org,
        block_type=SECRET,
        block_name="test-org-git-pull-url",
        block_id="test-block-id",
    )

    with (
        patch("ddpui.ddpdbt.dbt_service.prefect_service.delete_secret_block") as mock_delete_block,
        patch("ddpui.ddpdbt.dbt_service.secretsmanager.delete_github_pat") as mock_delete_pat,
    ):
        # Execute
        clear_github_pat_storage(org, "test-pat-secret-key")

        # Verify Prefect secret block was deleted
        mock_delete_block.assert_called_once_with("test-block-id")
        assert not OrgPrefectBlockv1.objects.filter(id=secret_block.id).exists()

        # Verify secrets manager PAT was deleted
        mock_delete_pat.assert_called_once_with("test-pat-secret-key")


@pytest.mark.django_db
def test_clear_github_pat_storage_only_secret_block():
    """Test clearing only Prefect secret block when no PAT secret key provided"""
    # Setup
    org = Org.objects.create(name="test-org", slug="test-org")

    # Create existing Prefect secret block
    secret_block = OrgPrefectBlockv1.objects.create(
        org=org,
        block_type=SECRET,
        block_name="test-org-git-pull-url",
        block_id="test-block-id",
    )

    with (
        patch("ddpui.ddpdbt.dbt_service.prefect_service.delete_secret_block") as mock_delete_block,
        patch("ddpui.ddpdbt.dbt_service.secretsmanager.delete_github_pat") as mock_delete_pat,
    ):
        # Execute without PAT secret key
        clear_github_pat_storage(org, None)

        # Verify Prefect secret block was deleted
        mock_delete_block.assert_called_once_with("test-block-id")
        assert not OrgPrefectBlockv1.objects.filter(id=secret_block.id).exists()

        # Verify secrets manager deletion was not called
        mock_delete_pat.assert_not_called()


@pytest.mark.django_db
def test_clear_github_pat_storage_no_secret_block():
    """Test clearing when no Prefect secret block exists"""
    # Setup
    org = Org.objects.create(name="test-org", slug="test-org")

    with (
        patch("ddpui.ddpdbt.dbt_service.prefect_service.delete_secret_block") as mock_delete_block,
        patch("ddpui.ddpdbt.dbt_service.secretsmanager.delete_github_pat") as mock_delete_pat,
    ):
        # Execute
        clear_github_pat_storage(org, "test-pat-secret-key")

        # Verify no secret block deletion attempted
        mock_delete_block.assert_not_called()

        # Verify secrets manager PAT was deleted
        mock_delete_pat.assert_called_once_with("test-pat-secret-key")


@pytest.mark.django_db
def test_clear_github_pat_storage_prefect_error():
    """Test handling Prefect secret block deletion error"""
    # Setup
    org = Org.objects.create(name="test-org", slug="test-org")

    # Create existing Prefect secret block
    secret_block = OrgPrefectBlockv1.objects.create(
        org=org,
        block_type=SECRET,
        block_name="test-org-git-pull-url",
        block_id="test-block-id",
    )

    with (
        patch("ddpui.ddpdbt.dbt_service.prefect_service.delete_secret_block") as mock_delete_block,
        patch("ddpui.ddpdbt.dbt_service.secretsmanager.delete_github_pat") as mock_delete_pat,
        patch("ddpui.ddpdbt.dbt_service.logger.warning") as mock_log_warning,
    ):
        # Mock Prefect error
        mock_delete_block.side_effect = Exception("Prefect API error")

        # Execute - should not raise exception despite Prefect error
        clear_github_pat_storage(org, "test-pat-secret-key")

        # Verify error was logged
        mock_log_warning.assert_called_once_with(
            "Failed to delete Prefect secret block test-org-git-pull-url: Prefect API error"
        )

        # Verify secrets manager was still called
        mock_delete_pat.assert_called_once_with("test-pat-secret-key")


@pytest.mark.django_db
def test_clear_github_pat_storage_secrets_manager_error():
    """Test graceful handling of secrets manager errors"""
    # Setup
    org = Org.objects.create(name="test-org", slug="test-org")

    # Create existing Prefect secret block
    secret_block = OrgPrefectBlockv1.objects.create(
        org=org,
        block_type=SECRET,
        block_name="test-org-git-pull-url",
        block_id="test-block-id",
    )

    with (
        patch("ddpui.ddpdbt.dbt_service.prefect_service.delete_secret_block") as mock_delete_block,
        patch(
            "ddpui.ddpdbt.dbt_service.secretsmanager.delete_github_pat",
            side_effect=Exception("Secrets manager error"),
        ) as mock_delete_pat,
    ):
        # Execute - should not raise exception despite secrets manager error
        clear_github_pat_storage(org, "test-pat-secret-key")

        # Verify Prefect secret block deletion proceeded
        mock_delete_block.assert_called_once_with("test-block-id")
        assert not OrgPrefectBlockv1.objects.filter(id=secret_block.id).exists()

        # Verify secrets manager deletion was attempted
        mock_delete_pat.assert_called_once_with("test-pat-secret-key")


@pytest.mark.django_db
def test_clear_github_pat_storage_both_errors():
    """Test handling both Prefect and secrets manager errors"""
    # Setup
    org = Org.objects.create(name="test-org", slug="test-org")

    # Create existing Prefect secret block
    secret_block = OrgPrefectBlockv1.objects.create(
        org=org,
        block_type=SECRET,
        block_name="test-org-git-pull-url",
        block_id="test-block-id",
    )

    with (
        patch("ddpui.ddpdbt.dbt_service.prefect_service.delete_secret_block") as mock_delete_block,
        patch("ddpui.ddpdbt.dbt_service.secretsmanager.delete_github_pat") as mock_delete_pat,
        patch("ddpui.ddpdbt.dbt_service.logger.warning") as mock_log_warning,
    ):
        # Mock both errors
        mock_delete_block.side_effect = Exception("Prefect API error")
        mock_delete_pat.side_effect = Exception("Secrets manager error")

        # Execute - should not raise exception despite both errors
        clear_github_pat_storage(org, "test-pat-secret-key")

        # Verify both errors were logged
        assert mock_log_warning.call_count == 2
        mock_log_warning.assert_any_call(
            "Failed to delete Prefect secret block test-org-git-pull-url: Prefect API error"
        )
        mock_log_warning.assert_any_call(
            "Failed to delete PAT from secrets manager: Secrets manager error"
        )

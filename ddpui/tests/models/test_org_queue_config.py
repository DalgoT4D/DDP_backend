import os
import pytest
from unittest.mock import patch
from ddpui.models.org import Org, get_default_queue_config
from ddpui.ddpprefect import DDP_WORK_QUEUE, MANUL_DBT_WORK_QUEUE


@pytest.fixture
def mock_workpool_env():
    """Mock the PREFECT_WORKER_POOL_NAME environment variable."""
    with patch.dict(os.environ, {"PREFECT_WORKER_POOL_NAME": "test_workpool"}):
        yield


class TestOrgQueueConfig:
    """Test cases for Org.get_queue_config() method."""

    @patch.dict(os.environ, {"PREFECT_WORKER_POOL_NAME": "test_workpool"})
    def test_get_queue_config_with_no_stored_config(self):
        """Test get_queue_config when queue_config is None or empty."""
        org = Org(name="Test Org", queue_config=None)

        config = org.get_queue_config()

        assert config.scheduled_pipeline_queue.name == DDP_WORK_QUEUE
        assert config.scheduled_pipeline_queue.workpool == "test_workpool"
        assert config.connection_sync_queue.name == DDP_WORK_QUEUE
        assert config.connection_sync_queue.workpool == "test_workpool"
        assert config.transform_task_queue.name == MANUL_DBT_WORK_QUEUE
        assert config.transform_task_queue.workpool == "test_workpool"

    @patch.dict(os.environ, {"PREFECT_WORKER_POOL_NAME": "test_workpool"})
    def test_get_queue_config_with_empty_dict(self):
        """Test get_queue_config when queue_config is an empty dictionary."""
        org = Org(name="Test Org", queue_config={})

        config = org.get_queue_config()

        assert config.scheduled_pipeline_queue.name == DDP_WORK_QUEUE
        assert config.scheduled_pipeline_queue.workpool == "test_workpool"
        assert config.connection_sync_queue.name == DDP_WORK_QUEUE
        assert config.connection_sync_queue.workpool == "test_workpool"
        assert config.transform_task_queue.name == MANUL_DBT_WORK_QUEUE
        assert config.transform_task_queue.workpool == "test_workpool"

    @patch.dict(os.environ, {"PREFECT_WORKER_POOL_NAME": "test_workpool"})
    def test_get_queue_config_with_complete_nested_format(self):
        """Test get_queue_config with complete nested format data."""
        queue_config = {
            "scheduled_pipeline_queue": {"name": "custom-sync", "workpool": "custom-workpool"},
            "connection_sync_queue": {"name": "custom-connection", "workpool": "another-workpool"},
            "transform_task_queue": {"name": "custom-transform", "workpool": "transform-workpool"},
        }
        org = Org(name="Test Org", queue_config=queue_config)

        config = org.get_queue_config()

        assert config.scheduled_pipeline_queue.name == "custom-sync"
        assert config.scheduled_pipeline_queue.workpool == "custom-workpool"
        assert config.connection_sync_queue.name == "custom-connection"
        assert config.connection_sync_queue.workpool == "another-workpool"
        assert config.transform_task_queue.name == "custom-transform"
        assert config.transform_task_queue.workpool == "transform-workpool"

    @patch.dict(os.environ, {"PREFECT_WORKER_POOL_NAME": "test_workpool"})
    def test_get_queue_config_with_nested_format_missing_workpool(self):
        """Test get_queue_config with nested format but missing workpool values."""
        queue_config = {
            "scheduled_pipeline_queue": {"name": "custom-sync"},  # missing workpool
            "connection_sync_queue": {
                "name": "custom-connection",
                "workpool": None,
            },  # None workpool
            "transform_task_queue": {"name": "custom-transform", "workpool": "transform-workpool"},
        }
        org = Org(name="Test Org", queue_config=queue_config)

        config = org.get_queue_config()

        # Should fall back to default workpool for missing/None workpool
        assert config.scheduled_pipeline_queue.name == "custom-sync"
        assert config.scheduled_pipeline_queue.workpool == "test_workpool"
        assert config.connection_sync_queue.name == "custom-connection"
        assert config.connection_sync_queue.workpool == "test_workpool"
        assert config.transform_task_queue.name == "custom-transform"
        assert config.transform_task_queue.workpool == "transform-workpool"

    @patch.dict(os.environ, {"PREFECT_WORKER_POOL_NAME": "test_workpool"})
    def test_get_queue_config_with_legacy_flat_format(self):
        """Test get_queue_config with legacy flat format (strings)."""
        queue_config = {
            "scheduled_pipeline_queue": "legacy-sync",
            "connection_sync_queue": "legacy-connection",
            "transform_task_queue": "legacy-transform",
        }
        org = Org(name="Test Org", queue_config=queue_config)

        config = org.get_queue_config()

        # Should use queue names from stored config, workpool from default
        assert config.scheduled_pipeline_queue.name == "legacy-sync"
        assert config.scheduled_pipeline_queue.workpool == "test_workpool"
        assert config.connection_sync_queue.name == "legacy-connection"
        assert config.connection_sync_queue.workpool == "test_workpool"
        assert config.transform_task_queue.name == "legacy-transform"
        assert config.transform_task_queue.workpool == "test_workpool"

    @patch.dict(os.environ, {"PREFECT_WORKER_POOL_NAME": "test_workpool"})
    def test_get_queue_config_with_mixed_formats(self):
        """Test get_queue_config with mixed format data."""
        queue_config = {
            "scheduled_pipeline_queue": {"name": "nested-sync", "workpool": "custom-workpool"},
            "connection_sync_queue": "flat-connection",  # legacy flat format
            # transform_task_queue missing entirely
        }
        org = Org(name="Test Org", queue_config=queue_config)

        config = org.get_queue_config()

        assert config.scheduled_pipeline_queue.name == "nested-sync"
        assert config.scheduled_pipeline_queue.workpool == "custom-workpool"
        assert config.connection_sync_queue.name == "flat-connection"
        assert config.connection_sync_queue.workpool == "test_workpool"
        # Missing key should fall back to defaults
        assert config.transform_task_queue.name == MANUL_DBT_WORK_QUEUE
        assert config.transform_task_queue.workpool == "test_workpool"

    @patch.dict(os.environ, {"PREFECT_WORKER_POOL_NAME": "test_workpool"})
    def test_get_queue_config_with_invalid_data_types(self):
        """Test get_queue_config with invalid data types (should fall back to defaults)."""
        queue_config = {
            "scheduled_pipeline_queue": 123,  # invalid type (number)
            "connection_sync_queue": [],  # invalid type (list)
            "transform_task_queue": {"invalid": "structure"},  # invalid structure (missing name)
        }
        org = Org(name="Test Org", queue_config=queue_config)

        config = org.get_queue_config()

        # All should fall back to defaults due to invalid data
        assert config.scheduled_pipeline_queue.name == DDP_WORK_QUEUE
        assert config.scheduled_pipeline_queue.workpool == "test_workpool"
        assert config.connection_sync_queue.name == DDP_WORK_QUEUE
        assert config.connection_sync_queue.workpool == "test_workpool"
        assert config.transform_task_queue.name == MANUL_DBT_WORK_QUEUE
        assert config.transform_task_queue.workpool == "test_workpool"

    def test_get_queue_config_without_env_var(self):
        """Test get_queue_config when PREFECT_WORKER_POOL_NAME is not set."""
        # Ensure env var is not set
        with patch.dict(os.environ, {}, clear=True):
            org = Org(name="Test Org", queue_config=None)

            config = org.get_queue_config()

            # Should fall back to "default" for workpool when env var is not set
            assert config.scheduled_pipeline_queue.name == DDP_WORK_QUEUE
            assert config.scheduled_pipeline_queue.workpool == "default"
            assert config.connection_sync_queue.name == DDP_WORK_QUEUE
            assert config.connection_sync_queue.workpool == "default"
            assert config.transform_task_queue.name == MANUL_DBT_WORK_QUEUE
            assert config.transform_task_queue.workpool == "default"

    @patch.dict(os.environ, {"PREFECT_WORKER_POOL_NAME": ""})
    def test_get_queue_config_with_empty_env_var(self):
        """Test get_queue_config when PREFECT_WORKER_POOL_NAME is empty string."""
        org = Org(name="Test Org", queue_config=None)

        config = org.get_queue_config()

        # Should fall back to "default" when env var is empty string
        assert config.scheduled_pipeline_queue.workpool == "default"
        assert config.connection_sync_queue.workpool == "default"
        assert config.transform_task_queue.workpool == "default"

    @patch.dict(os.environ, {"PREFECT_WORKER_POOL_NAME": "test_workpool"})
    def test_get_queue_config_partial_nested_data(self):
        """Test get_queue_config with partial nested data for some queues."""
        queue_config = {
            "scheduled_pipeline_queue": {"name": "custom-sync", "workpool": "custom-workpool"},
        }
        org = Org(name="Test Org", queue_config=queue_config)

        config = org.get_queue_config()

        # Custom queue should use stored data
        assert config.scheduled_pipeline_queue.name == "custom-sync"
        assert config.scheduled_pipeline_queue.workpool == "custom-workpool"

        # Missing queues should fall back to defaults
        assert config.connection_sync_queue.name == DDP_WORK_QUEUE
        assert config.connection_sync_queue.workpool == "test_workpool"
        assert config.transform_task_queue.name == MANUL_DBT_WORK_QUEUE
        assert config.transform_task_queue.workpool == "test_workpool"


class TestGetDefaultQueueConfig:
    """Test cases for get_default_queue_config() function."""

    @patch.dict(os.environ, {"PREFECT_WORKER_POOL_NAME": "test_workpool"})
    def test_get_default_queue_config_with_env_var(self):
        """Test get_default_queue_config when environment variable is set."""
        config = get_default_queue_config()

        expected = {
            "scheduled_pipeline_queue": {"name": DDP_WORK_QUEUE, "workpool": "test_workpool"},
            "connection_sync_queue": {"name": DDP_WORK_QUEUE, "workpool": "test_workpool"},
            "transform_task_queue": {"name": MANUL_DBT_WORK_QUEUE, "workpool": "test_workpool"},
        }

        assert config == expected

    def test_get_default_queue_config_without_env_var(self):
        """Test get_default_queue_config when environment variable is not set."""
        with patch.dict(os.environ, {}, clear=True):
            config = get_default_queue_config()

            expected = {
                "scheduled_pipeline_queue": {"name": DDP_WORK_QUEUE, "workpool": "default"},
                "connection_sync_queue": {"name": DDP_WORK_QUEUE, "workpool": "default"},
                "transform_task_queue": {"name": MANUL_DBT_WORK_QUEUE, "workpool": "default"},
            }

            assert config == expected

    @patch.dict(os.environ, {"PREFECT_WORKER_POOL_NAME": ""})
    def test_get_default_queue_config_with_empty_env_var(self):
        """Test get_default_queue_config when environment variable is empty."""
        config = get_default_queue_config()

        expected = {
            "scheduled_pipeline_queue": {"name": DDP_WORK_QUEUE, "workpool": "default"},
            "connection_sync_queue": {"name": DDP_WORK_QUEUE, "workpool": "default"},
            "transform_task_queue": {"name": MANUL_DBT_WORK_QUEUE, "workpool": "default"},
        }

        assert config == expected

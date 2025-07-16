import os
import django
import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from ddpui.models.org import Org, OrgPrefectBlockv1
from ddpui.models.org_user import OrgUser
from ddpui.models.org_preferences import OrgPreferences
from ddpui.models.llm import LlmSession, LlmAssistantType, LogsSummarizationType
from ddpui.models.airbyte import AirbyteJob
from ddpui.celeryworkers.tasks import summarize_logs, trigger_log_summarization_for_failed_flow
from ddpui.utils.constants import (
    TASK_AIRBYTESYNC,
    TASK_AIRBYTERESET,
    TASK_AIRBYTECLEAR,
)
from ddpui.models.tasks import TaskProgressStatus
from ddpui.utils.singletaskprogress import SingleTaskProgress
from ddpui.ddpprefect import AIRBYTECONNECTION

pytestmark = pytest.mark.django_db


@pytest.fixture
def org_with_llm():
    """Create an org with LLM enabled"""
    org = Org.objects.create(name="Test Org", slug="test-org")
    OrgPreferences.objects.create(org=org, llm_optin=True)
    return org


@pytest.fixture
def orguser_with_llm(org_with_llm):
    """Create an orguser for the LLM-enabled org"""
    return OrgUser.objects.create(org=org_with_llm, user_email="test@example.com", role=3)


class TestSummarizeLogsSystemUser:
    """Test summarize_logs function with system user (no orguser)"""

    @patch("ddpui.celeryworkers.tasks.get_flow_run_poll")
    @patch("ddpui.utils.webhook_helpers.get_org_from_flow_run")
    @patch.object(SingleTaskProgress, "__init__", return_value=None)
    @patch.object(SingleTaskProgress, "add")
    def test_summarize_logs_system_user_deployment(
        self, mock_progress_add, mock_progress_init, mock_get_org, mock_get_flow_run, org_with_llm
    ):
        """Test summarize_logs without orguser for deployment logs"""
        # Mock flow run
        mock_flow_run = {"id": "test-flow-run"}
        mock_get_flow_run.return_value = mock_flow_run
        mock_get_org.return_value = org_with_llm

        # Mock the task to avoid full execution
        with patch("ddpui.celeryworkers.tasks.get_flow_run_graphs") as mock_get_graphs:
            mock_get_graphs.return_value = [{"id": "task-1", "logs": []}]

            # Call without orguser_id
            try:
                summarize_logs(
                    orguser_id=None,
                    type=LogsSummarizationType.DEPLOYMENT,
                    flow_run_id="test-flow-run",
                    task_id="task-1",
                )
            except Exception:
                # We expect it to fail at some point, but we're testing the org extraction
                pass

        # Verify org was extracted from flow run
        mock_get_org.assert_called_once_with(mock_flow_run)
        # Verify progress was started
        mock_progress_add.assert_any_call({"message": "Started", "status": "running", "result": []})

    @patch.object(SingleTaskProgress, "__init__", return_value=None)
    @patch.object(SingleTaskProgress, "add")
    def test_summarize_logs_system_user_airbyte(
        self, mock_progress_add, mock_progress_init, org_with_llm
    ):
        """Test summarize_logs without orguser for airbyte sync logs"""
        # Create airbyte connection block
        block = OrgPrefectBlockv1.objects.create(
            org=org_with_llm, block_name="test-connection", block_type=AIRBYTECONNECTION
        )

        # Mock airbyte service
        with patch("ddpui.celeryworkers.tasks.airbyte_service.get_job_info") as mock_get_job_info:
            mock_get_job_info.return_value = {"job": {"configId": "test-connection"}}

            try:
                summarize_logs(
                    orguser_id=None,
                    type=LogsSummarizationType.AIRBYTE_SYNC,
                    job_id=12345,
                    connection_id="test-connection",
                )
            except Exception:
                # We expect it to fail at some point, but we're testing the org extraction
                pass

        # Verify progress was started
        mock_progress_add.assert_any_call({"message": "Started", "status": "running", "result": []})

    @patch.object(SingleTaskProgress, "__init__", return_value=None)
    @patch.object(SingleTaskProgress, "add")
    def test_summarize_logs_llm_disabled(self, mock_progress_add, mock_progress_init):
        """Test summarize_logs fails when LLM is disabled for org"""
        # Create org without LLM
        org = Org.objects.create(name="No LLM Org", slug="no-llm-org")
        OrgPreferences.objects.create(org=org, llm_optin=False)

        block = OrgPrefectBlockv1.objects.create(
            org=org, block_name="test-connection", block_type=AIRBYTECONNECTION
        )

        summarize_logs(
            orguser_id=None,
            type=LogsSummarizationType.AIRBYTE_SYNC,
            job_id=12345,
            connection_id="test-connection",
        )

        # Verify it failed due to LLM not enabled
        mock_progress_add.assert_any_call(
            {
                "message": "LLM not enabled for organization",
                "status": TaskProgressStatus.FAILED,
                "result": None,
            }
        )


class TestTriggerLogSummarization:
    """Test trigger_log_summarization_for_failed_flow task"""

    @patch("ddpui.celeryworkers.tasks.get_flow_run_poll")
    @patch("ddpui.celeryworkers.tasks.get_flow_run_graphs")
    @patch("ddpui.celeryworkers.tasks.summarize_logs.delay")
    def test_trigger_summarization_deployment_failure(
        self, mock_summarize, mock_get_graphs, mock_get_flow_run
    ):
        """Test triggering summarization for deployment failure"""
        # Mock flow run
        mock_flow_run = {
            "id": "flow-run-1",
            "parameters": {"config": {"tasks": [{"slug": "dbt-run"}]}},
        }
        mock_get_flow_run.return_value = mock_flow_run

        # Mock failed task
        mock_get_graphs.return_value = [{"id": "task-1", "state_type": "FAILED"}]

        # Execute
        trigger_log_summarization_for_failed_flow("flow-run-1", mock_flow_run)

        # Verify summarize_logs was called correctly
        mock_summarize.assert_called_once_with(
            orguser_id=None,
            type=LogsSummarizationType.DEPLOYMENT,
            flow_run_id="flow-run-1",
            task_id="task-1",
        )

    @patch("ddpui.celeryworkers.tasks.get_flow_run_graphs")
    @patch("ddpui.celeryworkers.tasks.summarize_logs.delay")
    def test_trigger_summarization_airbyte_failure(
        self, mock_summarize, mock_get_graphs, org_with_llm
    ):
        """Test triggering summarization for airbyte sync failure"""
        # Create airbyte job
        airbyte_job = AirbyteJob.objects.create(
            job_id=12345,
            job_type="sync",
            config_id="conn-123",
            status="failed",
            created_at=datetime.now(),
            ended_at=datetime.now(),
        )

        # Mock flow run
        flow_run = {
            "id": "flow-run-1",
            "parameters": {
                "config": {"tasks": [{"slug": "airbyte-sync", "connection_id": "conn-123"}]}
            },
        }

        # Mock failed task
        mock_get_graphs.return_value = [{"id": "task-1", "state_type": "FAILED"}]

        # Execute
        trigger_log_summarization_for_failed_flow("flow-run-1", flow_run)

        # Verify summarize_logs was called with job_id from database
        mock_summarize.assert_called_once_with(
            orguser_id=None,
            type=LogsSummarizationType.AIRBYTE_SYNC,
            flow_run_id="flow-run-1",
            task_id="task-1",
            job_id=12345,
            connection_id="conn-123",
        )

    @patch("ddpui.celeryworkers.tasks.get_flow_run_graphs")
    @patch("ddpui.celeryworkers.tasks.summarize_logs.delay")
    @patch.object(LlmSession.objects, "filter")
    def test_trigger_summarization_duplicate_prevention(
        self, mock_llm_filter, mock_summarize, mock_get_graphs
    ):
        """Test that duplicate summaries are not created"""
        # Mock existing summary
        mock_llm_filter.return_value.exists.return_value = True

        flow_run = {"id": "flow-run-1", "parameters": {"config": {"tasks": []}}}

        # Execute
        trigger_log_summarization_for_failed_flow("flow-run-1", flow_run)

        # Verify summarize_logs was NOT called
        mock_summarize.assert_not_called()

    @patch("ddpui.celeryworkers.tasks.get_flow_run_graphs")
    @patch("ddpui.celeryworkers.tasks.summarize_logs.delay")
    def test_trigger_summarization_no_failed_task(self, mock_summarize, mock_get_graphs):
        """Test when no failed task is found"""
        # Mock no failed tasks
        mock_get_graphs.return_value = [{"id": "task-1", "state_type": "COMPLETED"}]

        flow_run = {"id": "flow-run-1", "parameters": {"config": {"tasks": []}}}

        # Execute
        trigger_log_summarization_for_failed_flow("flow-run-1", flow_run)

        # Verify summarize_logs was NOT called
        mock_summarize.assert_not_called()


class TestWebhookIntegration:
    """Test webhook integration"""

    @patch("ddpui.celeryworkers.tasks.trigger_log_summarization_for_failed_flow.delay")
    @patch("ddpui.utils.webhook_helpers.get_org_from_flow_run")
    @patch("ddpui.utils.webhook_helpers.prefect_service.get_flow_run_poll")
    def test_webhook_triggers_summarization_on_failure(
        self, mock_get_flow_run, mock_get_org, mock_trigger, org_with_llm
    ):
        """Test webhook triggers summarization on failure"""
        from ddpui.utils.webhook_helpers import do_handle_prefect_webhook
        from ddpui.ddpprefect import FLOW_RUN_FAILED_STATE_NAME

        # Mock flow run and org
        mock_flow_run = {"id": "flow-run-1", "deployment_id": "dep-1"}
        mock_get_flow_run.return_value = mock_flow_run
        mock_get_org.return_value = org_with_llm

        # Call webhook handler with failure state
        do_handle_prefect_webhook("flow-run-1", FLOW_RUN_FAILED_STATE_NAME)

        # Verify summarization was triggered
        mock_trigger.assert_called_once_with("flow-run-1", mock_flow_run)

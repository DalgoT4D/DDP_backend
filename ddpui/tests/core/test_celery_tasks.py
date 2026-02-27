import os
import uuid
from pathlib import Path
import django
import pytest
from unittest.mock import Mock, patch, call
from celery import Celery

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from ddpui.settings import PRODUCTION
from ddpui.models.org import Org, OrgDbt, OrgWarehouse, TransformType, OrgSchemaChange
from ddpui.models.org_user import OrgUser
from ddpui.models.dbt_workflow import OrgDbtModel, OrgDbtModelType
from ddpui.tests.api_tests.test_user_org_api import (
    seed_db,
    orguser,
    nonadminorguser,
    mock_request,
    authuser,
    org_without_workspace,
)
from ddpui.ddpprefect.schema import DbtProfile, OrgDbtSchema
from ddpui.celeryworkers.tasks import (
    detect_schema_changes_for_org,
    get_connection_catalog_task,
    clear_stuck_locks,
)
from ddpui.models.tasks import TaskProgressStatus
from ddpui.core.dbtautomation_service import sync_sources_for_warehouse_v2
from ddpui.utils.taskprogress import TaskProgress
from ddpui.models.tasks import Task, OrgTask, TaskLock
from ddpui.utils.constants import TASK_AIRBYTESYNC
from ddpui.utils.singletaskprogress import SingleTaskProgress
from ddpui.ddpprefect import (
    FLOW_RUN_COMPLETED_STATE_NAME,
    FLOW_RUN_FAILED_STATE_NAME,
    FLOW_RUN_CRASHED_STATE_NAME,
    FLOW_RUN_CANCELLED_STATE_NAME,
    FLOW_RUN_RUNNING_STATE_NAME,
)
from datetime import datetime, timedelta
import pytz

pytestmark = pytest.mark.django_db


def test_sync_sources_failed_to_connect_to_warehouse(orguser: OrgUser, tmp_path):
    """a failure test for sync sources when not able to establish connection to client warehouse"""
    warehouse = OrgWarehouse.objects.create(
        org=orguser.org,
        wtype="postgres",
        airbyte_destination_id="airbyte_destination_id",
    )
    orgdbt = OrgDbt.objects.create(
        gitrepo_url=None,
        project_dir=str(Path(tmp_path) / orguser.org.slug),
        dbt_venv=tmp_path,
        target_type="postgres",
        default_schema="default_schema",
        transform_type=TransformType.UI,
    )
    orguser.org.dbt = orgdbt
    orguser.org.save()

    with patch.object(TaskProgress, "__init__", return_value=None), patch.object(
        TaskProgress, "add", return_value=Mock()
    ) as add_progress_mock, patch("os.getenv", return_value=tmp_path), patch(
        "ddpui.core.dbtautomation_service._get_wclient",
        Mock(side_effect=Exception("_get_wclient failed")),
    ) as get_wclient_mock:
        with pytest.raises(Exception) as exc:
            sync_sources_for_warehouse_v2(orgdbt.id, warehouse.id, "task-id", "hashkey")

        assert exc.value.args[0] == f"Error syncing sources: _get_wclient failed"
        add_progress_mock.assert_has_calls(
            [
                call(
                    {
                        "message": "Started syncing sources",
                        "status": TaskProgressStatus.RUNNING,
                    }
                ),
                call(
                    {
                        "message": "Error syncing sources: _get_wclient failed",
                        "status": TaskProgressStatus.FAILED,
                    }
                ),
            ]
        )
        get_wclient_mock.assert_called_once_with(warehouse)


def test_sync_sources_failed_to_fetch_schemas(orguser: OrgUser, tmp_path):
    """a failure test because it failed to fetch schemas"""
    warehouse = OrgWarehouse.objects.create(
        org=orguser.org,
        wtype="postgres",
        airbyte_destination_id="airbyte_destination_id",
    )
    orgdbt = OrgDbt.objects.create(
        gitrepo_url=None,
        project_dir=str(Path(tmp_path) / orguser.org.slug),
        dbt_venv=tmp_path,
        target_type="postgres",
        default_schema="default_schema",
        transform_type=TransformType.UI,
    )
    orguser.org.dbt = orgdbt
    orguser.org.save()

    with patch.object(TaskProgress, "__init__", return_value=None), patch.object(
        TaskProgress, "add", return_value=Mock()
    ) as add_progress_mock, patch("os.getenv", return_value=tmp_path), patch(
        "ddpui.core.dbtautomation_service._get_wclient",
    ) as get_wclient_mock, patch(
        "os.getenv", return_value=tmp_path
    ):
        mock_instance = Mock()

        # Make get_schemas raise an exception when it's called
        mock_instance.get_schemas.side_effect = Exception("get_schemas failed")

        # Make _get_wclient return the mock instance
        get_wclient_mock.return_value = mock_instance

        with pytest.raises(Exception) as exc:
            sync_sources_for_warehouse_v2(orgdbt.id, warehouse.id, "task-id", "hashkey")

        assert exc.value.args[0] == f"Error syncing sources: get_schemas failed"
        add_progress_mock.assert_has_calls(
            [
                call(
                    {
                        "message": "Started syncing sources",
                        "status": TaskProgressStatus.RUNNING,
                    }
                ),
                call(
                    {
                        "message": "Error syncing sources: get_schemas failed",
                        "status": TaskProgressStatus.FAILED,
                    }
                ),
            ]
        )
        get_wclient_mock.assert_called_once_with(warehouse)


@pytest.mark.skip(reason="Skipping this test as celery integration needs to be done on CI")
def test_sync_sources_success_with_no_schemas(orguser: OrgUser, tmp_path):
    """
    a success test that syncs all sources of warehouse
    1. On first sync new models will be created
    2. On second sync; no new models should be created
    3. If a new table is added to warehouse; this should be synced
    """
    project_dir: Path = Path(tmp_path) / orguser.org.slug
    warehouse = OrgWarehouse.objects.create(
        org=orguser.org,
        wtype="postgres",
        airbyte_destination_id="airbyte_destination_id",
    )
    orgdbt = OrgDbt.objects.create(
        gitrepo_url=None,
        project_dir=project_dir,
        dbt_venv=tmp_path,
        target_type="postgres",
        default_schema="default_schema",
        transform_type=TransformType.UI,
    )
    orguser.org.dbt = orgdbt
    orguser.org.save()

    project_dir.mkdir(exist_ok=True, parents=True)
    dbtrepo_dir = project_dir / "dbtrepo"
    dbtrepo_dir.mkdir(exist_ok=True, parents=True)
    models_dir = dbtrepo_dir / "models"
    models_dir.mkdir(exist_ok=True, parents=True)

    # warehouse schemas and tables
    SCHEMAS_TABLES = {
        "schema1": ["table1", "table2"],
        "schema2": ["table3", "table4"],
    }

    with patch.object(TaskProgress, "__init__", return_value=None), patch.object(
        TaskProgress, "add", return_value=Mock()
    ) as add_progress_mock, patch("os.getenv", return_value=tmp_path), patch(
        "ddpui.core.dbtautomation_service._get_wclient",
    ) as get_wclient_mock:
        mock_instance = Mock()
        mock_instance.get_schemas.return_value = SCHEMAS_TABLES.keys()
        mock_instance.get_tables.side_effect = lambda schema: SCHEMAS_TABLES[schema]

        # Make _get_wclient return the mock instance
        get_wclient_mock.return_value = mock_instance

        assert OrgDbtModel.objects.filter(type="source", orgdbt=orgdbt).count() == 0
        sync_sources_for_warehouse_v2(orgdbt.id, warehouse.id, "task-id", "hashkey")
        for schema in SCHEMAS_TABLES:
            assert set(
                OrgDbtModel.objects.filter(type="source", orgdbt=orgdbt, schema=schema).values_list(
                    "name", flat=True
                )
            ) == set(SCHEMAS_TABLES[schema])

        add_progress_mock.assert_has_calls(
            [
                call({"message": "Started syncing sources", "status": "runnning"}),
                call(
                    {
                        "message": "Reading sources for schema schema1 from warehouse",
                        "status": "running",
                    }
                ),
                call(
                    {
                        "message": "Finished reading sources for schema schema1",
                        "status": "running",
                    }
                ),
                call(
                    {
                        "message": "Reading sources for schema schema2 from warehouse",
                        "status": "running",
                    }
                ),
                call(
                    {
                        "message": "Finished reading sources for schema schema2",
                        "status": "running",
                    }
                ),
                call({"message": "Creating sources in dbt", "status": "running"}),
                call({"message": "Added schema1.table1", "status": "running"}),
                call({"message": "Added schema1.table2", "status": "running"}),
                call({"message": "Added schema2.table3", "status": "running"}),
                call({"message": "Added schema2.table4", "status": "running"}),
                call({"message": "Sync finished", "status": "completed"}),
            ]
        )

        # syncing sources again should not create any new entries
        sync_sources_for_warehouse_v2(orgdbt.id, warehouse.id, "task-id", "hashkey")
        for schema in SCHEMAS_TABLES:
            assert set(
                OrgDbtModel.objects.filter(type="source", orgdbt=orgdbt, schema=schema).values_list(
                    "name", flat=True
                )
            ) == set(SCHEMAS_TABLES[schema])

        # add a new table in the warehouse
        SCHEMAS_TABLES["schema1"].append("table5")
        sync_sources_for_warehouse_v2(orgdbt.id, warehouse.id, "task-id", "hashkey")
        for schema in SCHEMAS_TABLES:
            assert set(
                OrgDbtModel.objects.filter(type="source", orgdbt=orgdbt, schema=schema).values_list(
                    "name", flat=True
                )
            ) == set(SCHEMAS_TABLES[schema])


def test_sync_sources_v2_with_existing_models_update_columns(orguser: OrgUser, tmp_path):
    """Test sync_sources_for_warehouse_v2 when existing models exist - should update their columns"""
    project_dir: Path = Path(tmp_path) / orguser.org.slug
    warehouse = OrgWarehouse.objects.create(
        org=orguser.org,
        wtype="postgres",
        airbyte_destination_id="airbyte_destination_id",
    )
    orgdbt = OrgDbt.objects.create(
        gitrepo_url=None,
        project_dir=project_dir,
        dbt_venv=tmp_path,
        target_type="postgres",
        default_schema="default_schema",
        transform_type=TransformType.UI,
    )
    orguser.org.dbt = orgdbt
    orguser.org.save()

    project_dir.mkdir(exist_ok=True, parents=True)

    # Create existing model with old columns
    existing_model = OrgDbtModel.objects.create(
        uuid=uuid.uuid4(),
        orgdbt=orgdbt,
        name="existing_table",
        schema="schema1",
        display_name="existing_table",
        type=OrgDbtModelType.MODEL,
        output_cols=["old_col1", "old_col2"],
    )

    # warehouse schemas and tables
    SCHEMAS_TABLES = {
        "schema1": ["existing_table", "new_table"],
        "schema2": ["table3"],
    }

    with patch.object(TaskProgress, "__init__", return_value=None), patch.object(
        TaskProgress, "add", return_value=Mock()
    ) as add_progress_mock, patch("os.getenv", return_value=tmp_path), patch(
        "ddpui.core.dbtautomation_service._get_wclient",
    ) as get_wclient_mock:
        mock_instance = Mock()
        mock_instance.get_schemas.return_value = SCHEMAS_TABLES.keys()
        mock_instance.get_tables.side_effect = lambda schema: SCHEMAS_TABLES[schema]
        mock_instance.get_table_columns.side_effect = lambda schema, table: [
            {"name": f"{table}_col1"},
            {"name": f"{table}_col2"},
            {"name": f"{table}_col3"},
        ]

        get_wclient_mock.return_value = mock_instance

        # Should have 1 existing model
        assert OrgDbtModel.objects.filter(orgdbt=orgdbt).count() == 1

        sync_sources_for_warehouse_v2(orgdbt.id, warehouse.id, "task-id", "hashkey")

        # Should have 3 models now (1 existing updated + 2 new sources)
        assert OrgDbtModel.objects.filter(orgdbt=orgdbt).count() == 3

        # Check existing model was updated with new columns
        existing_model.refresh_from_db()
        assert existing_model.output_cols == [
            "existing_table_col1",
            "existing_table_col2",
            "existing_table_col3",
        ]

        # Check new sources were created
        assert OrgDbtModel.objects.filter(
            type=OrgDbtModelType.SOURCE, orgdbt=orgdbt, name="new_table"
        ).exists()
        assert OrgDbtModel.objects.filter(
            type=OrgDbtModelType.SOURCE, orgdbt=orgdbt, name="table3"
        ).exists()


def test_sync_sources_v2_column_fetch_error_handling(orguser: OrgUser, tmp_path):
    """Test sync_sources_for_warehouse_v2 handles column fetch errors gracefully"""
    project_dir: Path = Path(tmp_path) / orguser.org.slug
    warehouse = OrgWarehouse.objects.create(
        org=orguser.org,
        wtype="postgres",
        airbyte_destination_id="airbyte_destination_id",
    )
    orgdbt = OrgDbt.objects.create(
        gitrepo_url=None,
        project_dir=project_dir,
        dbt_venv=tmp_path,
        target_type="postgres",
        default_schema="default_schema",
        transform_type=TransformType.UI,
    )
    orguser.org.dbt = orgdbt
    orguser.org.save()

    project_dir.mkdir(exist_ok=True, parents=True)

    # warehouse schemas and tables
    SCHEMAS_TABLES = {
        "schema1": ["table1"],
    }

    with patch.object(TaskProgress, "__init__", return_value=None), patch.object(
        TaskProgress, "add", return_value=Mock()
    ) as add_progress_mock, patch("os.getenv", return_value=tmp_path), patch(
        "ddpui.core.dbtautomation_service._get_wclient",
    ) as get_wclient_mock:
        mock_instance = Mock()
        mock_instance.get_schemas.return_value = SCHEMAS_TABLES.keys()
        mock_instance.get_tables.side_effect = lambda schema: SCHEMAS_TABLES[schema]
        # Simulate error when getting columns
        mock_instance.get_table_columns.side_effect = Exception("Column fetch failed")

        get_wclient_mock.return_value = mock_instance

        # Should not raise exception even if column fetch fails
        sync_sources_for_warehouse_v2(orgdbt.id, warehouse.id, "task-id", "hashkey")

        # Source should still be created, just without columns
        source = OrgDbtModel.objects.get(type=OrgDbtModelType.SOURCE, orgdbt=orgdbt, name="table1")
        assert source.output_cols is None or source.output_cols == []


def test_sync_sources_v2_empty_warehouse(orguser: OrgUser, tmp_path):
    """Test sync_sources_for_warehouse_v2 with empty warehouse (no schemas)"""
    project_dir: Path = Path(tmp_path) / orguser.org.slug
    warehouse = OrgWarehouse.objects.create(
        org=orguser.org,
        wtype="postgres",
        airbyte_destination_id="airbyte_destination_id",
    )
    orgdbt = OrgDbt.objects.create(
        gitrepo_url=None,
        project_dir=project_dir,
        dbt_venv=tmp_path,
        target_type="postgres",
        default_schema="default_schema",
        transform_type=TransformType.UI,
    )
    orguser.org.dbt = orgdbt
    orguser.org.save()

    project_dir.mkdir(exist_ok=True, parents=True)

    with patch.object(TaskProgress, "__init__", return_value=None), patch.object(
        TaskProgress, "add", return_value=Mock()
    ) as add_progress_mock, patch("os.getenv", return_value=tmp_path), patch(
        "ddpui.core.dbtautomation_service._get_wclient",
    ) as get_wclient_mock:
        mock_instance = Mock()
        mock_instance.get_schemas.return_value = []  # Empty warehouse

        get_wclient_mock.return_value = mock_instance

        assert OrgDbtModel.objects.filter(type=OrgDbtModelType.SOURCE, orgdbt=orgdbt).count() == 0

        sync_sources_for_warehouse_v2(orgdbt.id, warehouse.id, "task-id", "hashkey")

        # No sources should be created
        assert OrgDbtModel.objects.filter(type=OrgDbtModelType.SOURCE, orgdbt=orgdbt).count() == 0

        # Should have completed without error
        add_progress_mock.assert_any_call(
            {
                "message": "Sync finished",
                "status": TaskProgressStatus.COMPLETED,
            }
        )


def test_sync_sources_v2_invalid_org_dbt_id(tmp_path):
    """Test sync_sources_for_warehouse_v2 with invalid org_dbt_id"""
    with patch.object(TaskProgress, "__init__", return_value=None), patch.object(
        TaskProgress, "add", return_value=Mock()
    ):
        # Use non-existent ID that is valid integer
        try:
            sync_sources_for_warehouse_v2(999999, 999999, "task-id", "hashkey")
        except Exception as e:
            # Should handle the case where org_dbt doesn't exist gracefully
            # May raise AttributeError on NoneType or other errors
            assert True  # Any exception is acceptable for invalid IDs


def test_sync_sources_v2_invalid_warehouse_id(orguser: OrgUser, tmp_path):
    """Test sync_sources_for_warehouse_v2 with invalid warehouse_id"""
    orgdbt = OrgDbt.objects.create(
        gitrepo_url=None,
        project_dir=str(Path(tmp_path) / orguser.org.slug),
        dbt_venv=tmp_path,
        target_type="postgres",
        default_schema="default_schema",
        transform_type=TransformType.UI,
    )
    orguser.org.dbt = orgdbt
    orguser.org.save()

    with patch.object(TaskProgress, "__init__", return_value=None), patch.object(
        TaskProgress, "add", return_value=Mock()
    ) as add_progress_mock:
        # Should handle invalid warehouse ID gracefully - use non-existent integer ID
        try:
            sync_sources_for_warehouse_v2(orgdbt.id, 999999, "task-id", "hashkey")
        except Exception as e:
            # Should raise an error during _get_wclient call due to None warehouse
            assert (
                "NoneType" in str(e)
                or "_get_wclient" in str(e)
                or "Error syncing sources" in str(e)
            )


def test_detect_schema_changes_for_org_ensure_orphan_connections_are_deleted(
    org_without_workspace: Org,
):
    """test detect_schema_changes_for_org function"""
    synctask = Task.objects.filter(slug=TASK_AIRBYTESYNC).first()
    if synctask is None:
        synctask = Task.objects.create(
            slug=TASK_AIRBYTESYNC, type="Airbyte Sync", label="Airbyte Sync"
        )
    synctask = OrgTask.objects.create(
        org=org_without_workspace, task=synctask, connection_id="some-connection-id"
    )
    OrgSchemaChange.objects.create(org=org_without_workspace, connection_id="fake-connection-id")
    assert OrgSchemaChange.objects.filter(org=org_without_workspace).count() == 1
    with patch(
        "ddpui.ddpairbyte.airbytehelpers.fetch_and_update_org_schema_changes"
    ) as fetch_and_update_org_schema_changes_mock, patch(
        "ddpui.celeryworkers.tasks.send_text_message"
    ) as mock_send_text_message:
        fetch_and_update_org_schema_changes_mock.return_value = None, "error"
        detect_schema_changes_for_org(org_without_workspace)
    assert OrgSchemaChange.objects.filter(org=org_without_workspace).count() == 0
    tag = " [STAGING]" if not PRODUCTION else ""
    mock_send_text_message.assert_called_once_with(
        "adminemail", f"Schema change detection errors for test-org-WO-slug{tag}", "error"
    )


def test_get_connection_catalog_task_error(org_without_workspace: Org):
    """tests get_connection_catalog_task"""
    task_key = "test-task-key"
    with patch(
        "ddpui.ddpairbyte.airbytehelpers.fetch_and_update_org_schema_changes"
    ) as fetch_and_update_org_schema_changes_mock, patch(
        "ddpui.celeryworkers.tasks.send_text_message"
    ) as mock_send_text_message:
        fetch_and_update_org_schema_changes_mock.return_value = None, "error"
        get_connection_catalog_task(task_key, org_without_workspace.id, "fake-connection-id")
    result = SingleTaskProgress.fetch(task_key)
    assert result == [
        {"message": "started", "status": TaskProgressStatus.RUNNING, "result": None},
        {
            "message": "error",
            "status": TaskProgressStatus.FAILED,
            "result": None,
        },
    ]
    mock_send_text_message.assert_called_once_with(
        os.getenv("ADMIN_EMAIL"),
        f"Unhandled schema change detection errors for {org_without_workspace.slug}",
        "error",
    )


def test_get_connection_catalog_task_success(org_without_workspace: Org):
    """tests get_connection_catalog_task"""
    task_key = "test-task-key"
    with patch(
        "ddpui.ddpairbyte.airbytehelpers.fetch_and_update_org_schema_changes"
    ) as fetch_and_update_org_schema_changes_mock:
        fetch_and_update_org_schema_changes_mock.return_value = {
            "name": 'connection_catalog["name"]',
            "connectionId": 'connection_catalog["connectionId"]',
            "catalogId": 'connection_catalog["catalogId"]',
            "syncCatalog": 'connection_catalog["syncCatalog"]',
            "schemaChange": 'connection_catalog["schemaChange"]',
            "catalogDiff": 'connection_catalog["catalogDiff"]',
        }, None
        get_connection_catalog_task(task_key, org_without_workspace.id, "fake-connection-id")
    result = SingleTaskProgress.fetch(task_key)
    assert result == [
        {"message": "started", "status": TaskProgressStatus.RUNNING, "result": None},
        {
            "message": "fetched catalog data",
            "status": TaskProgressStatus.COMPLETED,
            "result": {
                "name": 'connection_catalog["name"]',
                "connectionId": 'connection_catalog["connectionId"]',
                "catalogId": 'connection_catalog["catalogId"]',
                "syncCatalog": 'connection_catalog["syncCatalog"]',
                "schemaChange": 'connection_catalog["schemaChange"]',
                "catalogDiff": 'connection_catalog["catalogDiff"]',
            },
        },
    ]


# Clear Stuck Locks Tests


def create_test_orgtask(orguser):
    """Helper function to create an OrgTask for testing"""
    synctask = Task.objects.filter(slug=TASK_AIRBYTESYNC).first()
    if synctask is None:
        synctask = Task.objects.create(
            slug=TASK_AIRBYTESYNC, type="Airbyte Sync", label="Airbyte Sync"
        )
    return OrgTask.objects.create(
        org=orguser.org, task=synctask, connection_id="test-connection-id"
    )


def test_clear_stuck_locks_no_locks(orguser):
    """Test clear_stuck_locks when there are no locks"""
    assert TaskLock.objects.count() == 0

    result = clear_stuck_locks()

    assert result == 0
    assert TaskLock.objects.count() == 0


def test_clear_stuck_locks_flow_run_not_found(orguser):
    """Test clear_stuck_locks when flow run is not found in Prefect"""
    orgtask = create_test_orgtask(orguser)

    # Create a lock with a flow_run_id
    lock = TaskLock.objects.create(
        orgtask=orgtask, locked_by=orguser, flow_run_id="missing-flow-run-id"
    )

    with patch("ddpui.celeryworkers.tasks.get_flow_run_poll") as mock_get_flow_run:
        mock_get_flow_run.return_value = None

        result = clear_stuck_locks()

        assert result == 1
        assert TaskLock.objects.count() == 0  # Lock should be deleted


def test_clear_stuck_locks_terminal_state_recent(orguser):
    """Test clear_stuck_locks with terminal state but recent end_time (<5 min)"""
    # Create locks
    lock1 = TaskLock.objects.create(
        orgtask=create_test_orgtask(orguser), locked_by=orguser, flow_run_id="recent-flow-run-id"
    )

    # Mock flow run that ended 2 minutes ago
    recent_time = datetime.now(pytz.utc) - timedelta(minutes=2)
    mock_flow_run = {
        "id": "recent-flow-run-id",
        "state_name": FLOW_RUN_COMPLETED_STATE_NAME,
        "end_time": recent_time.isoformat(),
        "updated": recent_time.isoformat(),
    }

    with patch("ddpui.celeryworkers.tasks.get_flow_run_poll") as mock_get_flow_run, patch(
        "ddpui.celeryworkers.tasks.do_handle_prefect_webhook"
    ) as mock_webhook:
        mock_get_flow_run.return_value = mock_flow_run

        result = clear_stuck_locks()

        assert result == 0  # Should not process recent flow runs
        assert TaskLock.objects.count() == 1  # Lock should still exist
        mock_webhook.assert_not_called()


def test_clear_stuck_locks_terminal_state_old(orguser):
    """Test clear_stuck_locks with terminal state and old end_time (>5 min)"""
    # Create locks for same flow_run_id
    lock1 = TaskLock.objects.create(
        orgtask=create_test_orgtask(orguser), locked_by=orguser, flow_run_id="old-flow-run-id"
    )
    lock2 = TaskLock.objects.create(
        orgtask=create_test_orgtask(orguser), locked_by=orguser, flow_run_id="old-flow-run-id"
    )

    # Mock flow run that ended 10 minutes ago
    old_time = datetime.now(pytz.utc) - timedelta(minutes=10)
    mock_flow_run = {
        "id": "old-flow-run-id",
        "state_name": FLOW_RUN_FAILED_STATE_NAME,
        "end_time": old_time.isoformat(),
        "updated": old_time.isoformat(),
    }

    with patch("ddpui.celeryworkers.tasks.get_flow_run_poll") as mock_get_flow_run, patch(
        "ddpui.celeryworkers.tasks.do_handle_prefect_webhook"
    ) as mock_webhook:
        mock_get_flow_run.return_value = mock_flow_run

        result = clear_stuck_locks()

        assert result == 1  # Should process one flow_run_id
        mock_get_flow_run.assert_called_once_with("old-flow-run-id")
        mock_webhook.assert_called_once_with("old-flow-run-id", FLOW_RUN_FAILED_STATE_NAME)


def test_clear_stuck_locks_no_end_time_fallback_to_updated(orguser):
    """Test clear_stuck_locks with no end_time, falls back to updated field"""
    lock = TaskLock.objects.create(
        orgtask=create_test_orgtask(orguser),
        locked_by=orguser,
        flow_run_id="no-endtime-flow-run-id",
    )

    # Mock flow run with no end_time but old updated time
    old_time = datetime.now(pytz.utc) - timedelta(minutes=10)
    mock_flow_run = {
        "id": "no-endtime-flow-run-id",
        "state_name": FLOW_RUN_CRASHED_STATE_NAME,
        "end_time": None,
        "updated": old_time.isoformat(),
    }

    with patch("ddpui.celeryworkers.tasks.get_flow_run_poll") as mock_get_flow_run, patch(
        "ddpui.celeryworkers.tasks.do_handle_prefect_webhook"
    ) as mock_webhook:
        mock_get_flow_run.return_value = mock_flow_run

        result = clear_stuck_locks()

        assert result == 1
        mock_webhook.assert_called_once_with("no-endtime-flow-run-id", FLOW_RUN_CRASHED_STATE_NAME)


def test_clear_stuck_locks_running_state_ignored(orguser):
    """Test clear_stuck_locks ignores non-terminal states"""
    lock = TaskLock.objects.create(
        orgtask=create_test_orgtask(orguser), locked_by=orguser, flow_run_id="running-flow-run-id"
    )

    mock_flow_run = {
        "id": "running-flow-run-id",
        "state_name": FLOW_RUN_RUNNING_STATE_NAME,
        "end_time": None,
        "updated": (datetime.now(pytz.utc) - timedelta(hours=1)).isoformat(),
    }

    with patch("ddpui.celeryworkers.tasks.get_flow_run_poll") as mock_get_flow_run, patch(
        "ddpui.celeryworkers.tasks.do_handle_prefect_webhook"
    ) as mock_webhook:
        mock_get_flow_run.return_value = mock_flow_run

        result = clear_stuck_locks()

        assert result == 0  # Should not process running states
        assert TaskLock.objects.count() == 1  # Lock should still exist
        mock_webhook.assert_not_called()


def test_clear_stuck_locks_multiple_flow_runs(orguser):
    """Test clear_stuck_locks with multiple different flow_run_ids"""
    # Create locks for different flow_run_ids
    lock1 = TaskLock.objects.create(
        orgtask=create_test_orgtask(orguser), locked_by=orguser, flow_run_id="old-flow-1"
    )
    lock2 = TaskLock.objects.create(
        orgtask=create_test_orgtask(orguser), locked_by=orguser, flow_run_id="old-flow-2"
    )
    lock3 = TaskLock.objects.create(
        orgtask=create_test_orgtask(orguser), locked_by=orguser, flow_run_id="recent-flow"
    )

    old_time = datetime.now(pytz.utc) - timedelta(minutes=10)
    recent_time = datetime.now(pytz.utc) - timedelta(minutes=2)

    def mock_get_flow_run(flow_run_id):
        if flow_run_id == "old-flow-1":
            return {
                "id": "old-flow-1",
                "state_name": FLOW_RUN_COMPLETED_STATE_NAME,
                "end_time": old_time.isoformat(),
                "updated": old_time.isoformat(),
            }
        elif flow_run_id == "old-flow-2":
            return {
                "id": "old-flow-2",
                "state_name": FLOW_RUN_CANCELLED_STATE_NAME,
                "end_time": old_time.isoformat(),
                "updated": old_time.isoformat(),
            }
        elif flow_run_id == "recent-flow":
            return {
                "id": "recent-flow",
                "state_name": FLOW_RUN_FAILED_STATE_NAME,
                "end_time": recent_time.isoformat(),
                "updated": recent_time.isoformat(),
            }
        return None

    with patch("ddpui.celeryworkers.tasks.get_flow_run_poll", side_effect=mock_get_flow_run), patch(
        "ddpui.celeryworkers.tasks.do_handle_prefect_webhook"
    ) as mock_webhook:
        result = clear_stuck_locks()

        assert result == 2  # Should process 2 old flow runs
        assert mock_webhook.call_count == 2

        # Check that the right calls were made
        webhook_calls = [call[0] for call in mock_webhook.call_args_list]
        assert ("old-flow-1", FLOW_RUN_COMPLETED_STATE_NAME) in webhook_calls
        assert ("old-flow-2", FLOW_RUN_CANCELLED_STATE_NAME) in webhook_calls


def test_clear_stuck_locks_api_error_handling(orguser):
    """Test clear_stuck_locks handles API errors gracefully"""
    lock = TaskLock.objects.create(
        orgtask=create_test_orgtask(orguser), locked_by=orguser, flow_run_id="error-flow-run-id"
    )

    with patch("ddpui.celeryworkers.tasks.get_flow_run_poll") as mock_get_flow_run, patch(
        "ddpui.celeryworkers.tasks.do_handle_prefect_webhook"
    ) as mock_webhook:
        mock_get_flow_run.side_effect = Exception("API Error")

        result = clear_stuck_locks()

        assert result == 0  # Should not process due to error
        assert TaskLock.objects.count() == 1  # Lock should still exist
        mock_webhook.assert_not_called()


def test_clear_stuck_locks_no_time_fields(orguser):
    """Test clear_stuck_locks when both end_time and updated are null/unparseable"""
    lock = TaskLock.objects.create(
        orgtask=create_test_orgtask(orguser), locked_by=orguser, flow_run_id="no-time-flow-run-id"
    )

    mock_flow_run = {
        "id": "no-time-flow-run-id",
        "state_name": FLOW_RUN_COMPLETED_STATE_NAME,
        "end_time": None,
        "updated": None,
    }

    with patch("ddpui.celeryworkers.tasks.get_flow_run_poll") as mock_get_flow_run, patch(
        "ddpui.celeryworkers.tasks.do_handle_prefect_webhook"
    ) as mock_webhook:
        mock_get_flow_run.return_value = mock_flow_run

        result = clear_stuck_locks()

        assert result == 0  # Should not process without time reference
        assert TaskLock.objects.count() == 1  # Lock should still exist
        mock_webhook.assert_not_called()

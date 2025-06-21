import os
from pathlib import Path
import django
import pytest
from unittest.mock import Mock, patch, call
from celery import Celery

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from ddpui.models.org import Org, OrgDbt, OrgWarehouse, TransformType, OrgSchemaChange
from ddpui.models.org_user import OrgUser, Role
from ddpui.models.dbt_workflow import OrgDbtModel
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
    setup_dbtworkspace,
    detect_schema_changes_for_org,
    get_connection_catalog_task,
)
from ddpui.models.tasks import TaskProgressStatus
from ddpui.core.dbtautomation_service import sync_sources_for_warehouse
from ddpui.utils.taskprogress import TaskProgress
from ddpui.models.tasks import Task, OrgTask
from ddpui.utils.constants import TASK_AIRBYTESYNC
from ddpui.utils.singletaskprogress import SingleTaskProgress

pytestmark = pytest.mark.django_db


def test_post_dbt_workspace_failed_warehouse_not_present(orguser):
    """a failure test case when trying to setup dbt workspace without the warehouse"""
    dbtprofile = DbtProfile(name="fake-name", target_configs_schema="target_configs_schema")
    payload = OrgDbtSchema(
        profile=dbtprofile,
        gitrepoUrl="gitrepoUrl",
    )

    with patch.object(TaskProgress, "__init__", return_value=None), patch.object(
        TaskProgress, "add", return_value=Mock()
    ) as add_progress_mock:
        with pytest.raises(Exception) as exc:
            setup_dbtworkspace(orguser.org.id, payload.dict())
        assert exc.value.args[0] == f"need to set up a warehouse first for org {orguser.org.name}"
        add_progress_mock.assert_has_calls(
            [
                call({"message": "started", "status": "running"}),
                call(
                    {
                        "message": "need to set up a warehouse first",
                        "status": "failed",
                    }
                ),
            ]
        )


def test_post_dbt_workspace_failed_gitclone(orguser, tmp_path):
    """a failure test case when trying to setup dbt workspace due to failure in cloning git repo"""
    OrgWarehouse.objects.create(
        org=orguser.org,
        wtype="postgres",
        airbyte_destination_id="airbyte_destination_id",
    )
    dbtprofile = DbtProfile(name="fake-name", target_configs_schema="target_configs_schema")
    payload = OrgDbtSchema(
        profile=dbtprofile,
        gitrepoUrl="gitrepoUrl",
    )

    with patch.object(TaskProgress, "__init__", return_value=None), patch.object(
        TaskProgress, "add", return_value=Mock()
    ) as add_progress_mock, patch("os.getenv", return_value=tmp_path), patch(
        "ddpui.celeryworkers.tasks.clone_github_repo", return_value=False
    ) as gitclone_method_mock:
        with pytest.raises(Exception) as exc:
            setup_dbtworkspace(orguser.org.id, payload.dict())
        assert exc.value.args[0] == f"Failed to clone git repo"
        add_progress_mock.assert_has_calls([call({"message": "started", "status": "running"})])
        gitclone_method_mock.assert_called_once()


def test_post_dbt_workspace_success(orguser, tmp_path):
    """a success test case for setting up dbt workspace"""
    os.environ["CLIENTDBT_ROOT"] = str(tmp_path)
    OrgWarehouse.objects.create(
        org=orguser.org,
        wtype="postgres",
        airbyte_destination_id="airbyte_destination_id",
    )
    dbtprofile = DbtProfile(name="fake-name", target_configs_schema="target_configs_schema")
    payload = OrgDbtSchema(
        profile=dbtprofile,
        gitrepoUrl="gitrepoUrl",
    )

    with patch.object(TaskProgress, "__init__", return_value=None), patch.object(
        TaskProgress, "add", return_value=Mock()
    ) as add_progress_mock, patch(
        "ddpui.celeryworkers.tasks.DbtProjectManager.get_org_dir",
        return_value=str(tmp_path / orguser.org.slug),
    ), patch(
        "ddpui.celeryworkers.tasks.clone_github_repo",
        return_value=str(tmp_path / orguser.org.slug / "dbtrepo"),
    ) as gitclone_method_mock:
        assert OrgDbt.objects.filter(org=orguser.org).count() == 0
        setup_dbtworkspace(orguser.org.id, payload.dict())

        add_progress_mock.assert_has_calls([call({"message": "started", "status": "running"})])
        gitclone_method_mock.assert_called_once()
        assert OrgDbt.objects.filter(org=orguser.org).count() == 1
        add_progress_mock.assert_has_calls(
            [
                call({"message": "started", "status": "running"}),
                call({"message": "wrote OrgDbt entry", "status": "completed"}),
            ]
        )


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
            sync_sources_for_warehouse(orgdbt.id, warehouse.id, "task-id", "hashkey")

        assert exc.value.args[0] == f"Error syncing sources: _get_wclient failed"
        add_progress_mock.assert_has_calls(
            [
                call(
                    {
                        "message": "Started syncing sources",
                        "status": "runnning",
                    }
                ),
                call(
                    {
                        "message": "Error syncing sources: _get_wclient failed",
                        "status": "failed",
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
            sync_sources_for_warehouse(orgdbt.id, warehouse.id, "task-id", "hashkey")

        assert exc.value.args[0] == f"Error syncing sources: get_schemas failed"
        add_progress_mock.assert_has_calls(
            [
                call(
                    {
                        "message": "Started syncing sources",
                        "status": "runnning",
                    }
                ),
                call(
                    {
                        "message": "Error syncing sources: get_schemas failed",
                        "status": "failed",
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
        sync_sources_for_warehouse(orgdbt.id, warehouse.id, "task-id", "hashkey")
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
        sync_sources_for_warehouse(orgdbt.id, warehouse.id, "task-id", "hashkey")
        for schema in SCHEMAS_TABLES:
            assert set(
                OrgDbtModel.objects.filter(type="source", orgdbt=orgdbt, schema=schema).values_list(
                    "name", flat=True
                )
            ) == set(SCHEMAS_TABLES[schema])

        # add a new table in the warehouse
        SCHEMAS_TABLES["schema1"].append("table5")
        sync_sources_for_warehouse(orgdbt.id, warehouse.id, "task-id", "hashkey")
        for schema in SCHEMAS_TABLES:
            assert set(
                OrgDbtModel.objects.filter(type="source", orgdbt=orgdbt, schema=schema).values_list(
                    "name", flat=True
                )
            ) == set(SCHEMAS_TABLES[schema])


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
    ) as fetch_and_update_org_schema_changes_mock:
        fetch_and_update_org_schema_changes_mock.return_value = None, "error"
        detect_schema_changes_for_org(org_without_workspace)
    assert OrgSchemaChange.objects.filter(org=org_without_workspace).count() == 0


def test_get_connection_catalog_task_error(org_without_workspace: Org):
    """tests get_connection_catalog_task"""
    task_key = "test-task-key"
    with patch(
        "ddpui.ddpairbyte.airbytehelpers.fetch_and_update_org_schema_changes"
    ) as fetch_and_update_org_schema_changes_mock:
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

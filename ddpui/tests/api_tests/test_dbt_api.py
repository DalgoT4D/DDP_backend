import os
from unittest.mock import Mock, patch

import django
import pytest
from ninja.errors import HttpError

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from django.contrib.auth.models import User

from ddpui.api.dbt_api import (
    dbt_delete,
    get_dbt_workspace,
    post_dbt_git_pull,
    post_dbt_makedocs,
    put_dbt_schema_v1,
    get_transform_type,
    post_run_dbt_commands,
    post_dbt_workspace,
    put_dbt_github,
    put_connect_git_remote,
    get_elementary_setup_status,
    get_check_dbt_files,
    post_create_elementary_tracking_tables,
    post_create_elementary_profile,
    post_create_edr_sendreport_dataflow,
)
from ddpui.auth import ACCOUNT_MANAGER_ROLE
from ddpui.ddpprefect import SECRET, DBTCLIPROFILE
from ddpui.ddpprefect.schema import (
    DbtProfile,
    OrgDbtGitHub,
    OrgDbtSchema,
    OrgDbtTarget,
    OrgDbtConnectGitRemote,
)
from ddpui.core.git_manager import GitManagerError
from ddpui.models.org import Org, OrgDbt, OrgPrefectBlockv1, OrgWarehouse
from ddpui.models.org_user import OrgUser
from ddpui.models.role_based_access import Permission, Role, RolePermission
from ddpui.models.tasks import Task, OrgTask, TaskLock, TaskType
from ddpui.tests.api_tests.test_user_org_api import mock_request, seed_db
from ddpui.utils.custom_logger import CustomLogger
from ddpui.schemas.org_task_schema import TaskParameters
from ddpui.utils.constants import TASK_DBTCLEAN, TASK_DBTDEPS, TASK_DBTRUN

logger = CustomLogger("ddpui-pytest")

pytestmark = pytest.mark.django_db


@pytest.fixture
def org_with_workspace():
    """a pytest fixture which creates an Org having an airbyte workspace"""
    print("creating org_with_workspace")
    org = Org.objects.create(
        name="org-name", airbyte_workspace_id="FAKE-WORKSPACE-ID", slug="test-org-slug"
    )
    yield org
    print("deleting org_with_workspace")
    org.delete()


@pytest.fixture
def authuser():
    """a django User object"""
    user = User.objects.create(
        username="tempusername", email="tempuseremail", password="tempuserpassword"
    )
    yield user
    user.delete()


@pytest.fixture
def orguser(authuser, org_with_workspace):
    """a pytest fixture representing an OrgUser having the account-manager role"""
    org_user = OrgUser.objects.create(
        user=authuser,
        org=org_with_workspace,
        new_role=Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first(),
    )
    yield org_user
    org_user.delete()


@pytest.fixture
def f_orgwarehouse(org_with_workspace):
    """an OrgWarehouse attached to the org_with_workspace"""
    warehouse = OrgWarehouse.objects.create(
        org=org_with_workspace, wtype="postgres", credentials=""
    )
    yield warehouse
    warehouse.delete()


@pytest.fixture
def f_dbtcliprofileblock(org_with_workspace):
    """an OrgPrefectBlockv1 attached to the org_with_workspace"""
    block = OrgPrefectBlockv1.objects.create(
        org=org_with_workspace, block_type=DBTCLIPROFILE, block_name="fake-block_name"
    )
    yield block
    block.delete()


@pytest.fixture
def f_dbt_tasks():
    """Create the three required dbt tasks for testing"""
    tasks: list[Task] = []

    # Create the three tasks that post_run_dbt_commands looks for
    task_clean = Task.objects.create(
        type=TaskType.DBT,
        slug=TASK_DBTCLEAN,
        label="dbt clean",
        command="dbt clean",
        is_system=True,
    )
    tasks.append(task_clean)

    task_deps = Task.objects.create(
        type=TaskType.DBT, slug=TASK_DBTDEPS, label="dbt deps", command="dbt deps", is_system=True
    )
    tasks.append(task_deps)

    task_run = Task.objects.create(
        type=TaskType.DBT, slug=TASK_DBTRUN, label="dbt run", command="dbt run", is_system=True
    )
    tasks.append(task_run)

    yield tasks

    # Cleanup
    for task in tasks:
        task.delete()


@pytest.fixture
def f_org_tasks(org_with_workspace, f_dbt_tasks):
    """Create OrgTask instances for the dbt tasks"""
    org_tasks: list[OrgTask] = []

    for task in f_dbt_tasks:
        org_task = OrgTask.objects.create(org=org_with_workspace, task=task, generated_by="system")
        org_tasks.append(org_task)

    yield org_tasks

    # Cleanup
    for org_task in org_tasks:
        org_task.delete()


def test_seed_data(seed_db):
    """a test to seed the database"""
    assert Role.objects.count() == 5
    assert RolePermission.objects.count() > 5
    assert Permission.objects.count() > 5


def test_post_dbt_workspace(orguser):
    """
    passes an org with an orgdbt
    verifies that the orgdbt is deleted
    ensures that the celery setup task is called
    """
    request = mock_request(orguser)
    orgdbt = OrgDbt.objects.create()
    request.orguser.org.dbt = orgdbt
    request.orguser.org.save()

    dbtprofile = DbtProfile(name="fake-name", target_configs_schema="target_configs_schema")
    payload = OrgDbtSchema(
        profile=dbtprofile,
        gitrepoUrl="gitrepoUrl",
    )

    mocked_task = Mock()
    mocked_task.id = "task-id"
    with patch(
        "ddpui.celeryworkers.tasks.setup_dbtworkspace.delay", return_value=mocked_task
    ) as delay, patch("ddpui.api.dbt_api.dbt_service.check_repo_exists", return_value=True):
        post_dbt_workspace(request, payload)
        delay.assert_called_once_with(orguser.org.id, payload.dict())
        assert orguser.org.dbt is None


def test_put_dbt_github(orguser):
    """
    verifies that the orgdbt is updated with the new parameters
    verifies that the celery task is called with the right parameters
    """
    request = mock_request(orguser)
    orgdbt = OrgDbt.objects.create()
    request.orguser.org.dbt = orgdbt
    request.orguser.org.save()
    request.orguser.org.slug = "org-slug"

    payload = OrgDbtGitHub(gitrepoUrl="new-url", gitrepoAccessToken="new-access-token")

    OrgPrefectBlockv1.objects.create(
        org=request.orguser.org,
        block_type=SECRET,
        block_name=f"{request.orguser.org.slug}-git-pull-url",
    )

    mocked_task = Mock()
    mocked_task.id = "task-id"
    mock_secret_name = "gitrepoAccessToken-test-secret"
    with patch(
        "ddpui.celeryworkers.tasks.clone_github_repo.delay", return_value=mocked_task
    ) as delay, patch("ddpui.api.dbt_api.dbt_service.check_repo_exists", return_value=True), patch(
        "ddpui.ddpprefect.prefect_service.upsert_secret_block"
    ), patch(
        "ddpui.api.dbt_api.secretsmanager.save_github_pat", return_value=mock_secret_name
    ):
        put_dbt_github(request, payload)
        delay.assert_called_once_with(
            "org-slug",
            "new-url",
            mock_secret_name,
            os.getenv("CLIENTDBT_ROOT") + "/org-slug",
            None,
            False,
        )
        assert request.orguser.org.dbt.gitrepo_url == "new-url"
        assert request.orguser.org.dbt.gitrepo_access_token_secret == mock_secret_name


def test_put_dbt_github_with_elementary_setup(orguser):
    """
    verifies that when elementary is already set up and we update the git repo URL,
    the clone_github_repo task is called with setup_elementary=True
    """
    request = mock_request(orguser)
    orgdbt = OrgDbt.objects.create()
    request.orguser.org.dbt = orgdbt
    request.orguser.org.save()
    request.orguser.org.slug = "org-slug"

    payload = OrgDbtGitHub(gitrepoUrl="new-url", gitrepoAccessToken="new-access-token")

    OrgPrefectBlockv1.objects.create(
        org=request.orguser.org,
        block_type=SECRET,
        block_name=f"{request.orguser.org.slug}-git-pull-url",
    )

    mocked_task = Mock()
    mocked_task.id = "task-id"
    mock_secret_name = "gitrepoAccessToken-test-secret"

    with patch(
        "ddpui.celeryworkers.tasks.clone_github_repo.delay", return_value=mocked_task
    ) as delay, patch("ddpui.api.dbt_api.dbt_service.check_repo_exists", return_value=True), patch(
        "ddpui.ddpprefect.prefect_service.upsert_secret_block"
    ), patch(
        "ddpui.api.dbt_api.elementary_service.elementary_setup_status",
        return_value={"status": "set-up"},
    ), patch(
        "ddpui.api.dbt_api.secretsmanager.save_github_pat", return_value=mock_secret_name
    ):
        put_dbt_github(request, payload)
        delay.assert_called_once_with(
            "org-slug",
            "new-url",
            mock_secret_name,
            os.getenv("CLIENTDBT_ROOT") + "/org-slug",
            None,
            True,
        )
        assert request.orguser.org.dbt.gitrepo_url == "new-url"
        assert request.orguser.org.dbt.gitrepo_access_token_secret == mock_secret_name


def test_dbt_delete_no_org(orguser):
    """ensures that delete_dbt_workspace is called"""
    orguser.org = None
    request = mock_request(orguser)

    with pytest.raises(HttpError) as excinfo:
        dbt_delete(request)
        assert str(excinfo.value) == "create an organization first"


def test_dbt_delete(orguser):
    """ensures that delete_dbt_workspace is called"""
    request = mock_request(orguser)

    with patch("ddpui.api.dbt_api.OrgCleanupService") as mocked:
        instance = mocked.return_value
        dbt_delete(request)
        mocked.assert_called_once_with(request.orguser.org, dry_run=False)
        instance.delete_transformation_layer.assert_called_once()


def test_get_dbt_workspace_error(orguser):
    """verify the return value"""
    request = mock_request(orguser)

    response = get_dbt_workspace(request)
    assert response["error"] == "no dbt workspace has been configured"


def test_get_dbt_workspace_success(orguser):
    """verify the return value"""
    orguser = orguser
    orguser.org.dbt = OrgDbt(gitrepo_url="A", target_type="B", default_schema="C")
    request = mock_request(orguser)

    response = get_dbt_workspace(request)
    assert response["gitrepo_url"] == "A"
    assert response["target_type"] == "B"
    assert response["default_schema"] == "C"


def test_post_dbt_git_pull_dbt_not_configured(orguser: OrgUser):
    """fail - dbt not configured"""
    orguser = orguser
    orguser.org.dbt = None
    request = mock_request(orguser)

    with pytest.raises(HttpError) as excinfo:
        post_dbt_git_pull(request)
    assert str(excinfo.value) == "dbt is not configured for this client"


@patch.multiple("os.path", exists=Mock(return_value=False))
def test_post_dbt_git_pull_no_env(orguser: OrgUser):
    """fail - dbt not configured"""
    orguser = orguser
    orguser.org.dbt = OrgDbt(gitrepo_url="A", target_type="B", default_schema="C")
    request = mock_request(orguser)

    with pytest.raises(HttpError) as excinfo:
        post_dbt_git_pull(request)

    assert str(excinfo.value) == "create the dbt env first"


@patch.multiple("os.path", exists=Mock(return_value=True))
def test_post_dbt_git_pull_gitpull_failed(orguser: OrgUser):
    """fail - dbt not configured"""
    orguser = orguser
    orguser.org.dbt = OrgDbt(gitrepo_url="A", target_type="B", default_schema="C")
    request = mock_request(orguser)

    with patch(
        "ddpui.api.dbt_api.DbtProjectManager.get_dbt_project_dir", return_value="project_dir"
    ), patch("ddpui.api.dbt_api.GitManager") as mock_git_manager, pytest.raises(
        HttpError
    ) as excinfo:
        mock_git_manager.return_value.pull_changes.side_effect = Exception("git pull failed")
        post_dbt_git_pull(request)
    assert str(excinfo.value) == "git pull failed"


@patch.multiple("os.path", exists=Mock(return_value=True))
def test_post_dbt_git_pull_succes(orguser: OrgUser):
    """fail - dbt not configured"""
    orguser = orguser
    orguser.org.dbt = OrgDbt(gitrepo_url="A", target_type="B", default_schema="C")
    request = mock_request(orguser)

    with patch(
        "ddpui.api.dbt_api.DbtProjectManager.get_dbt_project_dir", return_value="project_dir"
    ), patch("ddpui.api.dbt_api.GitManager") as mock_git_manager:
        mock_git_manager.return_value.pull_changes.return_value = None
        response = post_dbt_git_pull(request)
    assert response == {"success": True}


def test_post_dbt_makedocs_dbt_not_configured(orguser: OrgUser):
    """fail - dbt not configured"""
    orguser = orguser
    orguser.org.dbt = None
    request = mock_request(orguser)

    with pytest.raises(HttpError) as excinfo:
        post_dbt_makedocs(request)
    assert str(excinfo.value) == "dbt is not configured for this client"


@patch.multiple("os.path", exists=Mock(return_value=False))
def test_post_dbt_makedocs_no_env(orguser: OrgUser):
    """fail - dbt not configured"""
    orguser.org.dbt = OrgDbt(gitrepo_url="A", target_type="B", default_schema="C")
    request = mock_request(orguser)

    with pytest.raises(HttpError) as excinfo:
        post_dbt_makedocs(request)
    assert str(excinfo.value) == "create the dbt env first"


@patch.multiple("os.path", exists=Mock(side_effect=[True, False]))
def test_post_dbt_makedocs_no_target(orguser: OrgUser):
    """fail - dbt docs not generated"""
    orguser.org.dbt = OrgDbt(gitrepo_url="A", target_type="B", default_schema="C")
    request = mock_request(orguser)

    with pytest.raises(HttpError) as excinfo:
        post_dbt_makedocs(request)
    assert str(excinfo.value) == "run dbt docs generate first"


@patch("os.path.exists", mock_exists=Mock(side_effect=[True, True]))
@patch(
    "ddpui.api.dbt_api.create_single_html",
    mock_create_single_html=Mock(return_value="html"),
)
@patch("builtins.open", mock_open=Mock(write=Mock(), close=Mock()))
@patch(
    "ddpui.api.dbt_api.RedisClient",
    mock_Redis=Mock(return_value=Mock(get_instance=Mock(set=Mock(), expire=Mock()))),
)
def test_post_dbt_makedocs(
    mock_Redis: Mock,
    mock_open: Mock,
    mock_create_single_html: Mock,
    mock_exists: Mock,
    orguser: OrgUser,
):
    """success"""
    orguser.org.dbt = OrgDbt(gitrepo_url="A", target_type="B", default_schema="C")
    request = mock_request(orguser)

    post_dbt_makedocs(request)
    mock_create_single_html.assert_called_once()


def test_put_dbt_schema_v1_no_dbt(orguser: OrgUser):
    """test put_dbt_schema_v1 no orgdbt"""
    orguser.org.dbt = None
    payload = OrgDbtTarget(target_configs_schema="new-target")
    request = mock_request(orguser)
    with pytest.raises(HttpError) as excinfo:
        put_dbt_schema_v1(request, payload)
    assert str(excinfo.value) == "create a dbt workspace first"


def test_put_dbt_schema_v1_no_warehouse(orguser: OrgUser):
    """test put_dbt_schema_v1 no warehouse"""
    orguser.org.dbt = OrgDbt(gitrepo_url="A", target_type="B", default_schema="C")
    payload = OrgDbtTarget(target_configs_schema="new-target")
    request = mock_request(orguser)
    with pytest.raises(HttpError) as excinfo:
        put_dbt_schema_v1(request, payload)
    assert str(excinfo.value) == "No warehouse configuration found for this organization"


def test_put_dbt_schema_v1_no_cli_profile(orguser: OrgUser, f_orgwarehouse: OrgWarehouse):
    """test put_dbt_schema_v1 no dbt cli profile"""
    orguser.org.dbt = OrgDbt(gitrepo_url="A", target_type="B", default_schema="C")
    payload = OrgDbtTarget(target_configs_schema="new-target")
    request = mock_request(orguser)
    retval = put_dbt_schema_v1(request, payload)
    assert retval == {"success": 1}
    assert orguser.org.dbt.default_schema == payload.target_configs_schema


def test_put_dbt_schema_v1_success(
    orguser: OrgUser, f_orgwarehouse: OrgWarehouse, f_dbtcliprofileblock: OrgPrefectBlockv1
):
    """test put_dbt_schema_v1 success flow"""
    orguser.org.dbt = OrgDbt(
        gitrepo_url="A",
        target_type="B",
        default_schema="C",
        cli_profile_block=f_dbtcliprofileblock,
    )
    orguser.org.dbt.save()
    payload = OrgDbtTarget(target_configs_schema="new-target")
    request = mock_request(orguser)
    with patch(
        "ddpui.ddpprefect.prefect_service.update_dbt_cli_profile_block"
    ) as mock_update_dbt_cli_profile_block:
        retval = put_dbt_schema_v1(request, payload)
        assert retval == {"success": 1}
        mock_update_dbt_cli_profile_block.assert_called_once_with(
            block_name=f_dbtcliprofileblock.block_name,
            target=payload.target_configs_schema,
            wtype=f_orgwarehouse.wtype,
        )


def test_get_transform_type_none(orguser: OrgUser):
    """tests get_transform_type"""
    request = mock_request(orguser)
    retval = get_transform_type(request)
    assert retval == {"transform_type": None}


def test_get_transform_type_non_none(orguser: OrgUser):
    """tests get_transform_type"""
    orguser.org.dbt = OrgDbt(transform_type="ui")
    request = mock_request(orguser)
    retval = get_transform_type(request)
    assert retval == {"transform_type": "ui"}


def test_post_run_dbt_commands_no_payload(orguser: OrgUser, f_org_tasks):
    """tests post_run_dbt_commands with no payload"""
    orguser.org.dbt = OrgDbt(gitrepo_url="A", target_type="B", default_schema="C")
    request = mock_request(orguser)

    mock_task_id = "test-task-id-123"
    mock_celery_task = Mock()
    mock_celery_task.id = "celery-task-id"

    with patch("ddpui.api.dbt_api.uuid4", return_value=mock_task_id), patch(
        "ddpui.api.dbt_api.TaskProgress"
    ) as mock_task_progress, patch(
        "ddpui.celeryworkers.tasks.run_dbt_commands.delay", return_value=mock_celery_task
    ) as mock_run_dbt:
        response = post_run_dbt_commands(request)

        # Verify task ID is returned
        assert response == {"task_id": mock_task_id}

        # Verify TaskProgress was initialized correctly
        mock_task_progress.assert_called_once_with(
            mock_task_id, f"run-dbt-commands-{orguser.org.slug}"
        )

        # Verify TaskProgress.add was called
        mock_task_progress.return_value.add.assert_called_once_with(
            {"message": "Added dbt commands in queue", "status": "queued"}
        )

        # Verify celery task was called with correct parameters
        mock_run_dbt.assert_called_once_with(orguser.org.id, orguser.org.dbt.id, mock_task_id, None)


def test_post_run_dbt_commands_with_payload(orguser: OrgUser, f_org_tasks):
    """tests post_run_dbt_commands with TaskParameters payload"""
    orguser.org.dbt = OrgDbt(gitrepo_url="A", target_type="B", default_schema="C")
    request = mock_request(orguser)
    payload = TaskParameters(flags=["full-refresh"], options={"target": "dev"})

    mock_task_id = "test-task-id-456"
    mock_celery_task = Mock()
    mock_celery_task.id = "celery-task-id"

    with patch("ddpui.api.dbt_api.uuid4", return_value=mock_task_id) as mock_uuid, patch(
        "ddpui.api.dbt_api.TaskProgress"
    ) as mock_task_progress, patch(
        "ddpui.celeryworkers.tasks.run_dbt_commands.delay", return_value=mock_celery_task
    ) as mock_run_dbt:
        response = post_run_dbt_commands(request, payload)

        # Verify task ID is returned
        assert response == {"task_id": mock_task_id}

        # Verify TaskProgress was initialized correctly
        mock_task_progress.assert_called_once_with(
            mock_task_id, f"run-dbt-commands-{orguser.org.slug}"
        )

        # Verify celery task was called with payload
        mock_run_dbt.assert_called_once_with(
            orguser.org.id, orguser.org.dbt.id, mock_task_id, payload.dict()
        )


def test_post_run_dbt_commands_task_locks(orguser: OrgUser, f_org_tasks):
    """tests that post_run_dbt_commands creates and cleans up task locks properly"""
    orguser.org.dbt = OrgDbt(gitrepo_url="A", target_type="B", default_schema="C")
    request = mock_request(orguser)

    mock_task_id = "test-task-id-789"
    mock_celery_task = Mock()
    mock_celery_task.id = "celery-task-id"

    # Track TaskLock creation and deletion
    created_locks = []

    def mock_task_lock_create(**kwargs):
        lock = Mock()
        lock.delete = Mock()
        created_locks.append(lock)
        return lock

    with patch("ddpui.api.dbt_api.uuid4", return_value=mock_task_id), patch(
        "ddpui.api.dbt_api.TaskProgress"
    ), patch(
        "ddpui.celeryworkers.tasks.run_dbt_commands.delay", return_value=mock_celery_task
    ), patch(
        "ddpui.api.dbt_api.TaskLock.objects.create", side_effect=mock_task_lock_create
    ) as mock_lock_create:
        response = post_run_dbt_commands(request)

        # Verify 3 task locks were created (for clean, deps, run)
        assert mock_lock_create.call_count == 3

        # Verify all locks were created with correct parameters
        for call in mock_lock_create.call_args_list:
            kwargs = call[1]
            assert kwargs["locked_by"] == orguser
            assert kwargs["celery_task_id"] == mock_task_id
            assert kwargs["orgtask"] in f_org_tasks

        # Verify all locks were deleted in finally block
        assert len(created_locks) == 3
        for lock in created_locks:
            lock.delete.assert_called_once()


def test_post_run_dbt_commands_exception_handling(orguser: OrgUser, f_org_tasks):
    """tests that task locks are cleaned up even when an exception occurs"""
    orguser.org.dbt = OrgDbt(gitrepo_url="A", target_type="B", default_schema="C")
    request = mock_request(orguser)

    mock_task_id = "test-task-id-error"
    created_locks = []

    def mock_task_lock_create(**kwargs):
        lock = Mock()
        lock.delete = Mock()
        created_locks.append(lock)
        return lock

    with patch("ddpui.api.dbt_api.uuid4", return_value=Mock(hex=mock_task_id)), patch(
        "ddpui.api.dbt_api.TaskProgress"
    ), patch(
        "ddpui.celeryworkers.tasks.run_dbt_commands.delay", side_effect=Exception("Celery error")
    ), patch(
        "ddpui.api.dbt_api.TaskLock.objects.create", side_effect=mock_task_lock_create
    ):
        # The function should raise the exception but still clean up locks
        with pytest.raises(Exception, match="Celery error"):
            post_run_dbt_commands(request)

        # Verify all locks were still deleted despite the exception
        assert len(created_locks) == 3
        for lock in created_locks:
            lock.delete.assert_called_once()


def test_post_run_dbt_commands_task_filtering(orguser: OrgUser, f_org_tasks):
    """tests that only the correct system tasks are used for locking"""
    orguser.org.dbt = OrgDbt(gitrepo_url="A", target_type="B", default_schema="C")
    request = mock_request(orguser)

    # Create an additional non-system task that should be ignored
    extra_task = Task.objects.create(
        type=TaskType.DBT,
        slug=TASK_DBTRUN,
        label="dbt run user",
        command="dbt run",
        is_system=False,
    )
    extra_org_task = OrgTask.objects.create(org=orguser.org, task=extra_task, generated_by="client")

    mock_task_id = "test-task-id-filtering"
    mock_celery_task = Mock()

    try:
        with patch("ddpui.api.dbt_api.uuid4", return_value=Mock(hex=mock_task_id)), patch(
            "ddpui.api.dbt_api.TaskProgress"
        ), patch(
            "ddpui.celeryworkers.tasks.run_dbt_commands.delay", return_value=mock_celery_task
        ), patch(
            "ddpui.api.dbt_api.TaskLock.objects.create"
        ) as mock_lock_create:
            post_run_dbt_commands(request)

            # Should only create 3 locks for system tasks, not the client task
            assert mock_lock_create.call_count == 3

            # Verify only system-generated org tasks were used
            for call in mock_lock_create.call_args_list:
                kwargs = call[1]
                assert kwargs["orgtask"].generated_by == "system"
                assert kwargs["orgtask"] != extra_org_task

    finally:
        # Cleanup
        extra_org_task.delete()
        extra_task.delete()


def test_get_elementary_setup_status_failure(orguser):
    """failure"""
    request = mock_request(orguser)
    with patch(
        "ddpui.api.dbt_api.elementary_service.elementary_setup_status",
        return_value={"error": "error-message"},
    ), pytest.raises(HttpError) as excinfo:
        get_elementary_setup_status(request)
        assert str(excinfo.value) == "error-message"


def test_get_elementary_setup_status_success(orguser):
    """success"""
    request = mock_request(orguser)
    with patch(
        "ddpui.api.dbt_api.elementary_service.elementary_setup_status",
        return_value={"status": "set-up"},
    ):
        response = get_elementary_setup_status(request)
        assert response == {"status": "set-up"}


def test_get_check_dbt_files_failure(orguser):
    """failure"""
    request = mock_request(orguser)
    with patch(
        "ddpui.api.dbt_api.elementary_service.check_dbt_files",
        return_value=("error-message", None),
    ), pytest.raises(HttpError) as excinfo:
        get_check_dbt_files(request)
        assert str(excinfo.value) == "error-message"


def test_get_check_dbt_files_success(orguser):
    """success"""
    request = mock_request(orguser)
    with patch(
        "ddpui.api.dbt_api.elementary_service.check_dbt_files",
        return_value=(None, {"status": "ok"}),
    ):
        response = get_check_dbt_files(request)
        assert response == {"status": "ok"}


def test_post_create_elementary_tracking_tables_failure(orguser):
    """failure"""
    request = mock_request(orguser)
    with patch(
        "ddpui.api.dbt_api.elementary_service.create_elementary_tracking_tables",
        return_value={"error": "error-message"},
    ), pytest.raises(HttpError) as excinfo:
        post_create_elementary_tracking_tables(request)
        assert str(excinfo.value) == "error-message"


def test_post_create_elementary_tracking_tables_success(orguser):
    """success"""
    request = mock_request(orguser)
    with patch(
        "ddpui.api.dbt_api.elementary_service.create_elementary_tracking_tables",
        return_value={"status": "ok"},
    ):
        response = post_create_elementary_tracking_tables(request)
        assert response == {"status": "ok"}


def test_post_create_elementary_profile_failure(orguser):
    """failure"""
    request = mock_request(orguser)
    with patch(
        "ddpui.api.dbt_api.elementary_service.create_elementary_profile",
        return_value={"error": "error-message"},
    ), pytest.raises(HttpError) as excinfo:
        post_create_elementary_profile(request)
        assert str(excinfo.value) == "error-message"


def test_post_create_elementary_profile_success(orguser):
    """success"""
    request = mock_request(orguser)
    with patch(
        "ddpui.api.dbt_api.elementary_service.create_elementary_profile",
        return_value={"status": "ok"},
    ):
        response = post_create_elementary_profile(request)
        assert response == {"status": "ok"}


def test_post_create_edr_sendreport_dataflow_failure(orguser):
    """failure"""
    request = mock_request(orguser)
    with patch(
        "ddpui.api.dbt_api.elementary_service.create_edr_sendreport_dataflow",
        return_value={"error": "error-message"},
    ), patch("ddpui.api.dbt_api.get_edr_send_report_task", return_value="orgtask"), pytest.raises(
        HttpError
    ) as excinfo:
        post_create_edr_sendreport_dataflow(request)
        assert str(excinfo.value) == "error-message"


def test_post_create_edr_sendreport_dataflow_success(orguser):
    """success"""
    request = mock_request(orguser)
    with patch(
        "ddpui.api.dbt_api.elementary_service.create_edr_sendreport_dataflow",
        return_value={"status": "ok"},
    ), patch("ddpui.api.dbt_api.get_edr_send_report_task", return_value="orgtask"):
        response = post_create_edr_sendreport_dataflow(request)
        assert response == {"status": "ok"}


# ==================== put_connect_git_remote tests ====================


def test_put_connect_git_remote_workspace_errors(seed_db, orguser: OrgUser):
    """Test workspace-related errors: no dbt workspace, project dir missing, repo dir missing"""
    request = mock_request(orguser)
    payload = OrgDbtConnectGitRemote(
        gitrepoUrl="https://github.com/user/repo.git",
        gitrepoAccessToken="ghp_test_token",
    )

    # Test 1: No dbt workspace
    request.orguser.org.dbt = None
    with pytest.raises(HttpError) as excinfo:
        put_connect_git_remote(request, payload)
    assert str(excinfo.value) == "Create a dbt workspace first"

    # Test 2: DBT repo directory doesn't exist
    orgdbt = OrgDbt.objects.create()
    request.orguser.org.dbt = orgdbt
    request.orguser.org.save()

    with patch(
        "ddpui.api.dbt_api.DbtProjectManager.get_dbt_project_dir",
        return_value="/nonexistent/path",
    ), patch("ddpui.api.dbt_api.Path") as mock_path:
        mock_path.return_value.exists.return_value = False
        with pytest.raises(HttpError) as excinfo:
            put_connect_git_remote(request, payload)
        assert str(excinfo.value) == "DBT repo directory does not exist"

    # Cleanup
    orgdbt.delete()


def test_put_connect_git_remote_git_errors(seed_db, orguser: OrgUser):
    """Test git-related errors: not initialized, verification fails (auth + not found)"""
    request = mock_request(orguser)
    payload = OrgDbtConnectGitRemote(
        gitrepoUrl="https://github.com/user/repo.git",
        gitrepoAccessToken="ghp_test_token",
    )

    orgdbt = OrgDbt.objects.create()
    request.orguser.org.dbt = orgdbt
    request.orguser.org.save()

    with patch(
        "ddpui.api.dbt_api.DbtProjectManager.get_dbt_project_dir",
        return_value="/existing/path",
    ), patch("ddpui.api.dbt_api.Path") as mock_path:
        mock_project_dir = Mock()
        mock_project_dir.exists.return_value = True
        mock_dbt_repo_dir = Mock()
        mock_dbt_repo_dir.exists.return_value = True
        mock_project_dir.__truediv__ = Mock(return_value=mock_dbt_repo_dir)
        mock_path.return_value = mock_project_dir

        # Test 1: Git not initialized
        with patch(
            "ddpui.api.dbt_api.GitManager",
            side_effect=GitManagerError(message="Not a git repository", error="details"),
        ), pytest.raises(HttpError) as excinfo:
            put_connect_git_remote(request, payload)
        assert "Git is not initialized" in str(excinfo.value)

        # Test 2: Verification fails - authentication error
        mock_git_manager = Mock()
        mock_git_manager.verify_remote_url.side_effect = GitManagerError(
            message="Authentication failed",
            error="The PAT token is invalid",
        )
        with patch("ddpui.api.dbt_api.GitManager", return_value=mock_git_manager), pytest.raises(
            HttpError
        ) as excinfo:
            put_connect_git_remote(request, payload)
        assert "Authentication failed" in str(excinfo.value)

        # Test 3: Verification fails - repo not found
        mock_git_manager.verify_remote_url.side_effect = GitManagerError(
            message="Repository not found",
            error="The repository URL is invalid",
        )
        with patch("ddpui.api.dbt_api.GitManager", return_value=mock_git_manager), pytest.raises(
            HttpError
        ) as excinfo:
            put_connect_git_remote(request, payload)
        assert "Repository not found" in str(excinfo.value)

    # Cleanup
    orgdbt.delete()


def test_put_connect_git_remote_masked_token_no_existing_pat(seed_db, orguser: OrgUser):
    """Test error when masked token provided but no existing PAT in secrets manager"""
    request = mock_request(orguser)
    payload = OrgDbtConnectGitRemote(
        gitrepoUrl="https://github.com/user/repo.git",
        gitrepoAccessToken="********",  # Masked token
    )

    orgdbt = OrgDbt.objects.create(gitrepo_access_token_secret=None)  # No existing PAT
    request.orguser.org.dbt = orgdbt
    request.orguser.org.save()

    with patch(
        "ddpui.api.dbt_api.DbtProjectManager.get_dbt_project_dir",
        return_value="/existing/path",
    ), patch("ddpui.api.dbt_api.Path") as mock_path:
        mock_project_dir = Mock()
        mock_project_dir.exists.return_value = True
        mock_dbt_repo_dir = Mock()
        mock_dbt_repo_dir.exists.return_value = True
        mock_project_dir.__truediv__ = Mock(return_value=mock_dbt_repo_dir)
        mock_path.return_value = mock_project_dir

        with pytest.raises(HttpError) as excinfo:
            put_connect_git_remote(request, payload)
        assert "Cannot use masked token" in str(excinfo.value)

    # Cleanup
    orgdbt.delete()


def test_put_connect_git_remote_set_remote_fails(seed_db, orguser: OrgUser):
    """Test 500 error when setting remote fails"""
    request = mock_request(orguser)
    payload = OrgDbtConnectGitRemote(
        gitrepoUrl="https://github.com/user/repo.git",
        gitrepoAccessToken="ghp_test_token",
    )

    orgdbt = OrgDbt.objects.create()
    request.orguser.org.dbt = orgdbt
    request.orguser.org.save()

    with patch(
        "ddpui.api.dbt_api.DbtProjectManager.get_dbt_project_dir",
        return_value="/existing/path",
    ), patch("ddpui.api.dbt_api.Path") as mock_path:
        mock_project_dir = Mock()
        mock_project_dir.exists.return_value = True
        mock_dbt_repo_dir = Mock()
        mock_dbt_repo_dir.exists.return_value = True
        mock_project_dir.__truediv__ = Mock(return_value=mock_dbt_repo_dir)
        mock_path.return_value = mock_project_dir

        mock_git_manager = Mock()
        mock_git_manager.verify_remote_url.return_value = True
        mock_git_manager.set_remote.side_effect = GitManagerError(
            message="Failed to set remote", error="git error"
        )

        with patch("ddpui.api.dbt_api.GitManager", return_value=mock_git_manager), pytest.raises(
            HttpError
        ) as excinfo:
            put_connect_git_remote(request, payload)
        assert excinfo.value.status_code == 500
        assert "Failed to set remote" in str(excinfo.value)

    # Cleanup
    orgdbt.delete()


def test_put_connect_git_remote_create(seed_db, orguser: OrgUser):
    """Test first-time connection: creates secret block, saves PAT, updates OrgDbt"""
    request = mock_request(orguser)
    request.orguser.org.slug = "test-org"
    payload = OrgDbtConnectGitRemote(
        gitrepoUrl="https://github.com/user/repo.git",
        gitrepoAccessToken="ghp_test_token",
    )

    orgdbt = OrgDbt.objects.create(gitrepo_access_token_secret=None)  # No existing PAT
    request.orguser.org.dbt = orgdbt
    request.orguser.org.save()

    with patch(
        "ddpui.api.dbt_api.DbtProjectManager.get_dbt_project_dir",
        return_value="/existing/path",
    ), patch("ddpui.api.dbt_api.Path") as mock_path:
        mock_project_dir = Mock()
        mock_project_dir.exists.return_value = True
        mock_dbt_repo_dir = Mock()
        mock_dbt_repo_dir.exists.return_value = True
        mock_project_dir.__truediv__ = Mock(return_value=mock_dbt_repo_dir)
        mock_path.return_value = mock_project_dir

        mock_git_manager = Mock()
        mock_git_manager.verify_remote_url.return_value = True
        mock_git_manager.set_remote.return_value = None

        mock_secret_name = "pat-secret-key"
        mock_oauth_url = "https://oauth2:ghp_test_token@github.com/user/repo.git"

        with patch(
            "ddpui.api.dbt_api.GitManager", return_value=mock_git_manager
        ) as mock_git_class, patch(
            "ddpui.api.dbt_api.prefect_service.upsert_secret_block",
            return_value={"block_id": "test-block-id"},
        ) as mock_upsert, patch(
            "ddpui.api.dbt_api.secretsmanager.save_github_pat",
            return_value=mock_secret_name,
        ) as mock_save_pat:
            # Mock the static method
            mock_git_class.generate_oauth_url_static.return_value = mock_oauth_url

            response = put_connect_git_remote(request, payload)

            # Verify success response
            assert response["success"] is True
            assert response["gitrepo_url"] == payload.gitrepoUrl

            # Verify prefect secret block was created with oauth URL
            mock_upsert.assert_called_once()
            call_args = mock_upsert.call_args[0][0]
            assert call_args.block_name == "test-org-git-pull-url"
            assert call_args.secret == mock_oauth_url

            # Verify PAT was saved (not updated)
            mock_save_pat.assert_called_once_with(payload.gitrepoAccessToken)

            # Verify OrgDbt was updated
            orgdbt.refresh_from_db()
            assert orgdbt.gitrepo_url == payload.gitrepoUrl
            assert orgdbt.gitrepo_access_token_secret == mock_secret_name

    # Cleanup
    OrgPrefectBlockv1.objects.filter(org=request.orguser.org).delete()
    orgdbt.delete()


def test_put_connect_git_remote_update_url_only(seed_db, orguser: OrgUser):
    """Test URL update with masked PAT: uses existing PAT, updates URL only"""
    request = mock_request(orguser)
    request.orguser.org.slug = "test-org"
    payload = OrgDbtConnectGitRemote(
        gitrepoUrl="https://github.com/user/new-repo.git",  # New URL
        gitrepoAccessToken="********",  # Masked token
    )

    orgdbt = OrgDbt.objects.create(
        gitrepo_url="https://github.com/user/old-repo.git",
        gitrepo_access_token_secret="existing-pat-secret",
    )
    request.orguser.org.dbt = orgdbt
    request.orguser.org.save()

    with patch(
        "ddpui.api.dbt_api.DbtProjectManager.get_dbt_project_dir",
        return_value="/existing/path",
    ), patch("ddpui.api.dbt_api.Path") as mock_path:
        mock_project_dir = Mock()
        mock_project_dir.exists.return_value = True
        mock_dbt_repo_dir = Mock()
        mock_dbt_repo_dir.exists.return_value = True
        mock_project_dir.__truediv__ = Mock(return_value=mock_dbt_repo_dir)
        mock_path.return_value = mock_project_dir

        mock_git_manager = Mock()
        mock_git_manager.verify_remote_url.return_value = True
        mock_git_manager.set_remote.return_value = None

        with patch("ddpui.api.dbt_api.GitManager", return_value=mock_git_manager), patch(
            "ddpui.api.dbt_api.secretsmanager.retrieve_github_pat",
            return_value="actual-pat-from-secrets",
        ) as mock_retrieve, patch(
            "ddpui.api.dbt_api.prefect_service.upsert_secret_block"
        ) as mock_upsert, patch(
            "ddpui.api.dbt_api.secretsmanager.save_github_pat"
        ) as mock_save, patch(
            "ddpui.api.dbt_api.secretsmanager.update_github_pat"
        ) as mock_update:
            response = put_connect_git_remote(request, payload)

            # Verify success
            assert response["success"] is True
            assert response["gitrepo_url"] == payload.gitrepoUrl

            # Verify existing PAT was retrieved
            mock_retrieve.assert_called_once_with("existing-pat-secret")

            # Verify secretsmanager was NOT called (masked token = no PAT update)
            mock_save.assert_not_called()
            mock_update.assert_not_called()
            mock_upsert.assert_not_called()

            # Verify OrgDbt URL was updated
            orgdbt.refresh_from_db()
            assert orgdbt.gitrepo_url == payload.gitrepoUrl

    # Cleanup
    orgdbt.delete()


def test_put_connect_git_remote_update_pat(seed_db, orguser: OrgUser):
    """Test PAT update: updates secretsmanager and prefect block"""
    request = mock_request(orguser)
    request.orguser.org.slug = "test-org"
    payload = OrgDbtConnectGitRemote(
        gitrepoUrl="https://gitlab.com/user/repo.git",  # Can also update URL
        gitrepoAccessToken="glpat_new_token",  # New PAT
    )

    orgdbt = OrgDbt.objects.create(
        gitrepo_url="https://github.com/user/old-repo.git",
        gitrepo_access_token_secret="existing-pat-secret",
    )
    request.orguser.org.dbt = orgdbt
    request.orguser.org.save()

    # Create existing prefect block
    OrgPrefectBlockv1.objects.create(
        org=request.orguser.org,
        block_type=SECRET,
        block_name="test-org-git-pull-url",
        block_id="existing-block-id",
    )

    with patch(
        "ddpui.api.dbt_api.DbtProjectManager.get_dbt_project_dir",
        return_value="/existing/path",
    ), patch("ddpui.api.dbt_api.Path") as mock_path:
        mock_project_dir = Mock()
        mock_project_dir.exists.return_value = True
        mock_dbt_repo_dir = Mock()
        mock_dbt_repo_dir.exists.return_value = True
        mock_project_dir.__truediv__ = Mock(return_value=mock_dbt_repo_dir)
        mock_path.return_value = mock_project_dir

        mock_git_manager = Mock()
        mock_git_manager.verify_remote_url.return_value = True
        mock_git_manager.set_remote.return_value = None

        mock_oauth_url = "https://oauth2:glpat_new_token@gitlab.com/user/repo.git"

        with patch(
            "ddpui.api.dbt_api.GitManager", return_value=mock_git_manager
        ) as mock_git_class, patch(
            "ddpui.api.dbt_api.prefect_service.upsert_secret_block",
            return_value={"block_id": "updated-block-id"},
        ) as mock_upsert, patch(
            "ddpui.api.dbt_api.secretsmanager.update_github_pat"
        ) as mock_update:
            # Mock the static method
            mock_git_class.generate_oauth_url_static.return_value = mock_oauth_url

            response = put_connect_git_remote(request, payload)

            # Verify success
            assert response["success"] is True
            assert response["gitrepo_url"] == payload.gitrepoUrl

            # Verify prefect secret block was updated with new oauth URL
            mock_upsert.assert_called_once()
            call_args = mock_upsert.call_args[0][0]
            assert call_args.secret == mock_oauth_url

            # Verify PAT was updated (not saved)
            mock_update.assert_called_once_with("existing-pat-secret", payload.gitrepoAccessToken)

            # Verify OrgDbt was updated
            orgdbt.refresh_from_db()
            assert orgdbt.gitrepo_url == payload.gitrepoUrl

    # Cleanup
    OrgPrefectBlockv1.objects.filter(org=request.orguser.org).delete()
    orgdbt.delete()

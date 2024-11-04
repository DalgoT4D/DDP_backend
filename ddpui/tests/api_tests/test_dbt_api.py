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
    post_dbt_workspace,
    put_dbt_github,
)
from ddpui.auth import ACCOUNT_MANAGER_ROLE
from ddpui.ddpprefect.schema import DbtProfile, OrgDbtGitHub, OrgDbtSchema
from ddpui.models.org import Org, OrgDbt
from ddpui.models.org_user import OrgUser, OrgUserRole
from ddpui.models.role_based_access import Permission, Role, RolePermission
from ddpui.tests.api_tests.test_user_org_api import mock_request, seed_db
from ddpui.utils.custom_logger import CustomLogger

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
        role=OrgUserRole.ACCOUNT_MANAGER,
        new_role=Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first(),
    )
    yield org_user
    org_user.delete()


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
    ) as delay:
        with patch("ddpui.api.dbt_api.dbt_service.check_repo_exists", return_value=True):
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

    mocked_task = Mock()
    mocked_task.id = "task-id"
    with patch(
        "ddpui.celeryworkers.tasks.clone_github_repo.delay", return_value=mocked_task
    ) as delay:
        with patch("ddpui.api.dbt_api.dbt_service.check_repo_exists", return_value=True):
            put_dbt_github(request, payload)
            delay.assert_called_once_with(
                "org-slug",
                "new-url",
                "new-access-token",
                os.getenv("CLIENTDBT_ROOT") + "/org-slug",
                None,
            )
            assert request.orguser.org.dbt.gitrepo_url == "new-url"
            assert request.orguser.org.dbt.gitrepo_access_token_secret == "new-access-token"


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

    with patch("ddpui.ddpdbt.dbt_service.delete_dbt_workspace") as mocked:
        dbt_delete(request)
        mocked.assert_called_once_with(request.orguser.org)


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
@patch.multiple("ddpui.api.dbt_api", runcmd=Mock(side_effect=Exception("runcmd failed")))
def test_post_dbt_git_pull_gitpull_failed(orguser: OrgUser):
    """fail - dbt not configured"""
    orguser = orguser
    orguser.org.dbt = OrgDbt(gitrepo_url="A", target_type="B", default_schema="C")
    request = mock_request(orguser)

    with patch(
        "ddpui.api.dbt_api.DbtProjectManager.get_dbt_project_dir", return_value="project_dir"
    ):
        with pytest.raises(HttpError) as excinfo:
            post_dbt_git_pull(request)
        assert str(excinfo.value) == "git pull failed in " + os.path.join("project_dir", "dbtrepo")


@patch.multiple("os.path", exists=Mock(return_value=True))
@patch.multiple("ddpui.api.dbt_api", runcmd=Mock(return_value=True))
def test_post_dbt_git_pull_succes(orguser: OrgUser):
    """fail - dbt not configured"""
    orguser = orguser
    orguser.org.dbt = OrgDbt(gitrepo_url="A", target_type="B", default_schema="C")
    request = mock_request(orguser)

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
    orguser = orguser
    orguser.org.dbt = OrgDbt(gitrepo_url="A", target_type="B", default_schema="C")
    request = mock_request(orguser)

    post_dbt_makedocs(request)
    mock_create_single_html.assert_called_once()

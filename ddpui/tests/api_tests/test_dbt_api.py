import os
import django

from unittest.mock import Mock, patch
import pytest
from ninja.errors import HttpError

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from django.contrib.auth.models import User

from ddpui.models.org import Org, OrgDbt
from ddpui.models.org_user import OrgUser, OrgUserRole
from ddpui.api.dbt_api import (
    post_dbt_workspace,
    put_dbt_github,
    dbt_delete,
    get_dbt_workspace,
    post_dbt_git_pull,
    post_dbt_makedocs,
)
from ddpui.ddpprefect.schema import DbtProfile, OrgDbtSchema, OrgDbtGitHub

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
        user=authuser, org=org_with_workspace, role=OrgUserRole.ACCOUNT_MANAGER
    )
    yield org_user
    org_user.delete()


def test_post_dbt_workspace(orguser):
    """
    passes an org with an orgdbt
    verifies that the orgdbt is deleted
    ensures that the celery setup task is called
    """
    request = Mock()
    request.orguser = orguser
    orgdbt = OrgDbt.objects.create()
    request.orguser.org.dbt = orgdbt
    request.orguser.org.save()

    dbtprofile = DbtProfile(
        name="fake-name", target_configs_schema="target_configs_schema"
    )
    payload = OrgDbtSchema(
        profile=dbtprofile,
        gitrepoUrl="gitrepoUrl",
    )

    mocked_task = Mock()
    mocked_task.id = "task-id"
    with patch(
        "ddpui.celeryworkers.tasks.setup_dbtworkspace.delay", return_value=mocked_task
    ) as delay:
        post_dbt_workspace(request, payload)
        delay.assert_called_once_with(orguser.org.id, payload.dict())
        assert orguser.org.dbt is None


def test_put_dbt_github(orguser):
    """
    verifies that the orgdbt is updated with the new parameters
    verifies that the celery task is called with the right parameters
    """
    request = Mock()
    request.orguser = orguser
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
        put_dbt_github(request, payload)
        delay.assert_called_once_with(
            "new-url",
            "new-access-token",
            os.getenv("CLIENTDBT_ROOT") + "/org-slug",
            None,
        )
        assert request.orguser.org.dbt.gitrepo_url == "new-url"
        assert request.orguser.org.dbt.gitrepo_access_token_secret == "new-access-token"


def test_dbt_delete_no_org(orguser):
    """ensures that delete_dbt_workspace is called"""
    request = Mock()
    orguser.org = None
    request.orguser = orguser

    with pytest.raises(HttpError) as excinfo:
        dbt_delete(request)
        assert str(excinfo.value) == "create an organization first"


def test_dbt_delete(orguser):
    """ensures that delete_dbt_workspace is called"""
    request = Mock()
    request.orguser = orguser

    with patch("ddpui.ddpdbt.dbt_service.delete_dbt_workspace") as mocked:
        dbt_delete(request)
        mocked.assert_called_once_with(request.orguser.org)


def test_get_dbt_workspace_error(orguser):
    """verify the return value"""
    request = Mock()
    request.orguser = orguser

    response = get_dbt_workspace(request)
    assert response["error"] == "no dbt workspace has been configured"


def test_get_dbt_workspace_success(orguser):
    """verify the return value"""
    request = Mock()
    request.orguser = orguser
    request.orguser.org.dbt = OrgDbt(
        gitrepo_url="A", target_type="B", default_schema="C"
    )

    response = get_dbt_workspace(request)
    assert response["gitrepo_url"] == "A"
    assert response["target_type"] == "B"
    assert response["default_schema"] == "C"


def test_post_dbt_git_pull_dbt_not_configured(orguser: OrgUser):
    """fail - dbt not configured"""
    request = Mock()
    request.orguser = orguser
    request.orguser.org.dbt = None

    with pytest.raises(HttpError) as excinfo:
        post_dbt_git_pull(request)
    assert str(excinfo.value) == "dbt is not configured for this client"


@patch("os.path.exists", return_value=False)
def test_post_dbt_git_pull_no_env(orguser: OrgUser):
    """fail - dbt not configured"""
    request = Mock()
    request.orguser = orguser

    with pytest.raises(HttpError) as excinfo:
        post_dbt_git_pull(request)
    assert str(excinfo.value) == "create the dbt env first"


@patch.multiple("os.path", exists=Mock(return_value=True))
@patch.multiple(
    "ddpui.api.dbt_api", runcmd=Mock(side_effect=Exception("runcmd failed"))
)
def test_post_dbt_git_pull_gitpull_failed(orguser: OrgUser):
    """fail - dbt not configured"""
    request = Mock()
    request.orguser = orguser
    request.orguser.org.dbt = OrgDbt(
        gitrepo_url="A", target_type="B", default_schema="C"
    )

    with pytest.raises(HttpError) as excinfo:
        post_dbt_git_pull(request)
    assert (
        str(excinfo.value)
        == "git pull failed in "
        + os.getenv("CLIENTDBT_ROOT")
        + "/"
        + request.orguser.org.slug
        + "/dbtrepo"
    )


@patch.multiple("os.path", exists=Mock(return_value=True))
@patch.multiple("ddpui.api.dbt_api", runcmd=Mock(return_value=True))
def test_post_dbt_git_pull_succes(orguser: OrgUser):
    """fail - dbt not configured"""
    request = Mock()
    request.orguser = orguser
    request.orguser.org.dbt = OrgDbt(
        gitrepo_url="A", target_type="B", default_schema="C"
    )

    response = post_dbt_git_pull(request)
    assert response == {"success": True}


def test_post_dbt_makedocs_dbt_not_configured(orguser: OrgUser):
    """fail - dbt not configured"""
    request = Mock()
    request.orguser = orguser
    request.orguser.org.dbt = None

    with pytest.raises(HttpError) as excinfo:
        post_dbt_makedocs(request)
    assert str(excinfo.value) == "dbt is not configured for this client"


@patch("os.path.exists", return_value=False)
def test_post_dbt_makedocs_no_env(orguser: OrgUser):
    """fail - dbt not configured"""
    request = Mock()
    request.orguser = orguser

    with pytest.raises(HttpError) as excinfo:
        post_dbt_makedocs(request)
    assert str(excinfo.value) == "create the dbt env first"


@patch("os.path.exists", side_effect=[True, False])
def test_post_dbt_makedocs_no_target(orguser: OrgUser):
    """fail - dbt docs not generated"""
    request = Mock()
    request.orguser = orguser

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
    "ddpui.api.dbt_api.Redis",
    mock_Redis=Mock(return_value=Mock(set=Mock(), expire=Mock())),
)
def test_post_dbt_makedocs(
    mock_Redis: Mock,
    mock_open: Mock,
    mock_create_single_html: Mock,
    mock_exists: Mock,
    orguser: OrgUser,
):
    """success"""
    request = Mock()
    request.orguser = orguser
    request.orguser.org.dbt = OrgDbt(
        gitrepo_url="A", target_type="B", default_schema="C"
    )

    post_dbt_makedocs(request)
    mock_create_single_html.assert_called_once()

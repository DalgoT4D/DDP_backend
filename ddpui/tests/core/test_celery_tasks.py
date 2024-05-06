import os
import django
import pytest
from unittest.mock import Mock, patch, call
from celery import Celery
from celery.result import AsyncResult
from celery.worker import WorkController
from celery.contrib.testing.tasks import ping

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from ddpui.models.org import Org, OrgDbt, OrgWarehouse
from ddpui.tests.api_tests.test_user_org_api import (
    seed_db,
    orguser,
    nonadminorguser,
    mock_request,
    authuser,
    org_without_workspace,
)
from ddpui.ddpprefect.schema import DbtProfile, OrgDbtSchema, OrgDbtGitHub
from ddpui.celeryworkers.tasks import setup_dbtworkspace
from ddpui.utils.taskprogress import TaskProgress


pytestmark = pytest.mark.django_db


def test_post_dbt_workspace_failed_warehouse_not_present(orguser):
    """a failure test case when trying to setup dbt workspace without the warehouse"""
    dbtprofile = DbtProfile(
        name="fake-name", target_configs_schema="target_configs_schema"
    )
    payload = OrgDbtSchema(
        profile=dbtprofile,
        gitrepoUrl="gitrepoUrl",
    )

    with patch.object(TaskProgress, "__init__", return_value=None), patch.object(
        TaskProgress, "add", return_value=Mock()
    ) as add_progress_mock:
        with pytest.raises(Exception) as exc:
            setup_dbtworkspace(orguser.org.id, payload.dict())
        assert (
            exc.value.args[0]
            == f"need to set up a warehouse first for org {orguser.org.name}"
        )
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
    warehouse = OrgWarehouse.objects.create(
        org=orguser.org,
        wtype="postgres",
        airbyte_destination_id="airbyte_destination_id",
    )
    dbtprofile = DbtProfile(
        name="fake-name", target_configs_schema="target_configs_schema"
    )
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
        add_progress_mock.assert_has_calls(
            [call({"message": "started", "status": "running"})]
        )
        gitclone_method_mock.assert_called_once()


def test_post_dbt_workspace_success(orguser, tmp_path):
    """a success test case for setting up dbt workspace"""
    OrgWarehouse.objects.create(
        org=orguser.org,
        wtype="postgres",
        airbyte_destination_id="airbyte_destination_id",
    )
    dbtprofile = DbtProfile(
        name="fake-name", target_configs_schema="target_configs_schema"
    )
    payload = OrgDbtSchema(
        profile=dbtprofile,
        gitrepoUrl="gitrepoUrl",
    )

    with patch.object(TaskProgress, "__init__", return_value=None), patch.object(
        TaskProgress, "add", return_value=Mock()
    ) as add_progress_mock, patch("os.getenv", return_value=tmp_path), patch(
        "ddpui.celeryworkers.tasks.clone_github_repo", return_value=True
    ) as gitclone_method_mock:
        assert OrgDbt.objects.filter(org=orguser.org).count() == 0
        setup_dbtworkspace(orguser.org.id, payload.dict())

        add_progress_mock.assert_has_calls(
            [call({"message": "started", "status": "running"})]
        )
        gitclone_method_mock.assert_called_once()
        assert OrgDbt.objects.filter(org=orguser.org).count() == 1
        add_progress_mock.assert_has_calls(
            [
                call({"message": "started", "status": "running"}),
                call({"message": "wrote OrgDbt entry", "status": "completed"}),
            ]
        )

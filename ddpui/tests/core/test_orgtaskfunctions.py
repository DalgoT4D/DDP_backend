import os
import django
from django.core.management import call_command
from django.apps import apps
from pathlib import Path
import yaml

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()


import pytest
from ddpui.models.org import Org, OrgDbt
from ddpui.core.orgtaskfunctions import (
    get_edr_send_report_task,
    fetch_elementary_profile_target,
)
from ddpui.models.tasks import Task
from ddpui.tests.api_tests.test_orgtask_api import org_with_dbt_workspace

pytestmark = pytest.mark.django_db


@pytest.fixture(scope="session")
def seed_master_tasks_db(django_db_setup, django_db_blocker):
    with django_db_blocker.unblock():
        # Run the loaddata command to load the fixture
        call_command("loaddata", "tasks.json")


# ================================================================================


def test_seed_master_tasks(seed_master_tasks_db):
    """a test to seed the database"""
    assert Task.objects.count() == 10


# ================================================================================


def test_get_edr_send_report_task(seed_master_tasks_db, tmpdir):
    """test get_edr_send_report_task"""

    org = Org.objects.create(name="del", slug="del")
    orgdbt = OrgDbt.objects.create(
        org=org,
        gitrepo_url="some_repo",
        project_dir=tmpdir,
        target_type="postgres",
        default_schema="staging",
        transform_type="git",
    )
    assert get_edr_send_report_task(org, orgdbt) is None
    assert get_edr_send_report_task(org, orgdbt, create=False) is None
    orgtask = get_edr_send_report_task(org, orgdbt, create=True)
    assert orgtask is not None
    assert orgtask.org == org
    assert orgtask.task.slug == "generate-edr"
    assert orgtask.uuid is not None
    assert orgtask.parameters["options"]["profiles-dir"] == "elementary_profiles"
    assert (
        orgtask.parameters["options"]["bucket-file-path"]
        == "reports/del.TODAYS_DATE.html"
    )
    assert orgtask.parameters["options"]["profile-target"] == "default"
    assert get_edr_send_report_task(org, orgdbt) is not None


def test_fetch_elementary_profile_target(tmpdir):
    """test fetch_elementary_profile_target"""

    org = Org.objects.create(name="del", slug="del")
    orgdbt = OrgDbt.objects.create(
        org=org,
        gitrepo_url="some_repo",
        project_dir=tmpdir,
        target_type="postgres",
        default_schema="staging",
        transform_type="git",
    )

    project_dir = Path(orgdbt.project_dir) / "dbtrepo"
    profiles_yml_dir = project_dir / "elementary_profiles"
    profiles_yml_dir.mkdir(parents=True, exist_ok=True)

    elementary_profiles_default = {
        "elementary": {"outputs": {"default": {"type": "postgres", "more": "possible"}}}
    }
    with open(project_dir / "elementary_profiles" / "profiles.yml", "w") as file:
        yaml.dump(elementary_profiles_default, file)

    assert fetch_elementary_profile_target(orgdbt) == "default"

    elementary_profiles_custom = {
        "elementary": {"outputs": {"custom": {"type": "postgres", "more": "possible"}}}
    }

    with open(project_dir / "elementary_profiles" / "profiles.yml", "w") as file:
        yaml.dump(elementary_profiles_custom, file)

    assert fetch_elementary_profile_target(orgdbt) == "custom"

    elementary_profiles_custom_multiple = {
        "elementary": {
            "outputs": {
                "another": {"type": "postgres", "more": "possible"},
                "custom": {"type": "postgres", "more": "possible"},
            }
        }
    }

    with open(project_dir / "elementary_profiles" / "profiles.yml", "w") as file:
        yaml.dump(elementary_profiles_custom_multiple, file)

    assert fetch_elementary_profile_target(orgdbt) == "another"

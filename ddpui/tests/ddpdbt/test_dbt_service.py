import os
from unittest.mock import patch
import django
import pytest

from ddpui.models.org import Org, OrgDbt, OrgPrefectBlock
from ddpui.ddpdbt.dbt_service import delete_dbt_workspace
from ddpui.ddpprefect import DBTCORE

pytestmark = pytest.mark.django_db

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()


def test_delete_dbt_workspace():
    """tests the delete_dbt_workspace function"""
    org = Org.objects.create(name="temp", slug="temp")

    org.dbt = OrgDbt.objects.create(
        gitrepo_url="gitrepo_url",
        project_dir="project-dir",
        target_type="tgt",
        default_schema="default_schema",
    )
    org.save()

    assert OrgDbt.objects.filter(gitrepo_url="gitrepo_url").count() == 1

    OrgPrefectBlock.objects.create(
        org=org, block_type=DBTCORE, block_id="block-id", block_name="block-id"
    )

    assert OrgPrefectBlock.objects.filter(block_id="block-id").count() == 1

    with patch("ddpui.ddpdbt.dbt_service.os.path.exists") as mock_exists, patch(
        "ddpui.ddpdbt.dbt_service.shutil.rmtree"
    ) as mock_rmtree, patch(
        "ddpui.ddpdbt.dbt_service.prefect_service.delete_dbt_core_block"
    ) as mock_delete_dbt_core_block:
        mock_exists.return_value = True
        delete_dbt_workspace(org)
        mock_exists.assert_called_once_with("project-dir")
        mock_rmtree.assert_called_once_with("project-dir")
        mock_delete_dbt_core_block.assert_called_once_with("block-id")

    assert org.dbt is None
    assert OrgDbt.objects.filter(gitrepo_url="gitrepo_url").count() == 0
    assert OrgPrefectBlock.objects.filter(block_id="block-id").count() == 0

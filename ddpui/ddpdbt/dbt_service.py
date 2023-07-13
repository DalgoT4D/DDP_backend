import os
import shutil
from ddpui.models.org_user import Org
from ddpui.models.org import OrgPrefectBlock, OrgDbt
from ddpui.ddpprefect import prefect_service
from ddpui.ddpprefect import DBTCORE
from ddpui.utils import secretsmanager


def delete_dbt_workspace(org: Org):
    """deletes the dbt workspace on disk as well as in prefect"""
    if org.dbt:
        dbt = org.dbt
        org.dbt = None
        org.save()
        if os.path.exists(dbt.project_dir):
            shutil.rmtree(dbt.project_dir)
        dbt.delete()

    for dbtblock in OrgPrefectBlock.objects.filter(org=org, block_type=DBTCORE):
        prefect_service.delete_dbt_core_block(dbtblock.block_id)
        dbtblock.delete()

    secretsmanager.delete_github_token(org)

    for orgdbt in OrgDbt.objects.filter(org=org):
        orgdbt.delete()

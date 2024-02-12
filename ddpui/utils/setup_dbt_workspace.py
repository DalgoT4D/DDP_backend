import os
from pathlib import Path

import slugify

from ddpui.models.org import Org, OrgDbt, OrgWarehouse
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui")


def setup_local_dbt_workspace(org_id: int, payload: dict) -> str:
    """sets up an org's dbt workspace, recreating it if it already exists"""
    breakpoint()
    org = Org.objects.filter(id=org_id).first()
    logger.info("found org %s", org.name)

    warehouse = OrgWarehouse.objects.filter(org=org).first()

    if org.slug is None:
        org.slug = slugify(org.name)
        org.save()

    # this client'a dbt setup happens here
    project_dir = Path(os.getenv("CLIENTDBT_ROOT")) / org.slug

    dbt = OrgDbt(
        gitrepo_url="",
        project_dir=str(project_dir),
        dbt_venv=os.getenv("DBT_VENV"),
        target_type=warehouse.wtype,
        default_schema=payload["default_schema"],
    )
    dbt.save()
    logger.info("created orgdbt for org %s", org.name)
    org.dbt = dbt
    org.save()
    logger.info("set org.dbt for org %s", org.name)

    logger.info("set dbt workspace completed for org %s", org.name)

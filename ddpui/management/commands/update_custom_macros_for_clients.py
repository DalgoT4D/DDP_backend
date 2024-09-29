import glob, shutil
import os
from pathlib import Path
from dotenv import load_dotenv
from django.core.management.base import BaseCommand

from ddpui.models.org import TransformType
from ddpui.models.dbt_workflow import OrgDbt
from ddpui.models.role_based_access import Role
from ddpui.auth import ACCOUNT_MANAGER_ROLE
from dbt_automation import assets

import logging

load_dotenv()

logger = logging.getLogger("management-script")


class Command(BaseCommand):
    """
    Scripts needed to move current clients to role based access
    """

    def handle(self, *args, **options):
        # for OrgDbt go into the macros directory copy custom macros from dbt_automation

        for org_dbt in OrgDbt.objects.all():
            project_name = "dbtrepo"
            project_dir: Path = Path(org_dbt.project_dir)
            dbtrepo_dir: Path = project_dir / project_name

            if org_dbt.transform_type == TransformType.UI and dbtrepo_dir.exists():
                # update all macros with .sql extension from assets
                assets_dir = assets.__path__[0]

                for sql_file_path in glob.glob(os.path.join(assets_dir, "*.sql")):
                    # Get the target path in the project_dir/macros directory
                    target_path = Path(dbtrepo_dir) / "macros" / Path(sql_file_path).name

                    # Update/create the .sql file to the target path
                    shutil.copy(sql_file_path, target_path)

                    # Log the creation of the file
                    logger.info("updated %s", target_path)

                logger.info(f"Updated the custom macros for orgdbt_id %s", org_dbt.id)
            else:
                logger.info("Dbt project folder for the orgdbt_id %s not found", org_dbt.id)

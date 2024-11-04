from pathlib import Path
from django.core.management.base import BaseCommand
from ddpui.models.org import OrgDataFlowv1, Org
from ddpui.core.orgdbt_manager import DbtProjectManager
from ddpui.ddpdbt.schema import DbtProjectParams


class Command(BaseCommand):
    """Docstring"""

    help = "Sets the name of the worker pool for all manual deployments."

    def add_arguments(self, parser):
        """Docstring"""
        parser.add_argument(
            "--orgslug",
            type=str,
            help="Org slug: use 'all' to run for all orgs at once",
            required=True,
        )
        parser.add_argument("--rollback", action="store_true")

    def handle(self, *args, **options):
        """Docstring"""
        orgs = Org.objects.all()
        if options["orgslug"] != "all":
            orgs = orgs.filter(slug=options["orgslug"])

        for org in orgs:
            orgdbt = org.dbt
            if not orgdbt:
                print(f"Org {org.slug} does not have dbt workspace setup")
                continue

            if options["rollback"]:
                print(f"Rolling back the relative paths for org {org.slug}")
                orgdbt.project_dir = str(DbtProjectManager.clients_base_dir() / org.slug)
                orgdbt.dbt_venv = DbtProjectManager.dbt_venv_base_dir()
                orgdbt.save()
                continue

            print(f"Updating the relative paths for org {org.slug}")

            # project dir
            relative_project_dir = DbtProjectManager.get_dbt_repo_relative_path(
                Path(orgdbt.project_dir) / "dbtrepo"
            )
            orgdbt.project_dir = relative_project_dir

            # dbt venv
            relative_dbt_venv = DbtProjectManager.get_dbt_venv_relative_path(
                Path(orgdbt.dbt_venv) / "venv"
            )
            orgdbt.dbt_venv = relative_dbt_venv

            # assert
            params: DbtProjectParams = DbtProjectManager.gather_dbt_project_params(org, orgdbt)
            try:
                assert Path(params.project_dir).exists(), "project dir does not exist"
                assert Path(params.dbt_env_dir).exists(), "dbt venv does not exist"
            except AssertionError as e:
                print(f"Skipping saving the relative paths Error: {e}")
                continue

            orgdbt.save()

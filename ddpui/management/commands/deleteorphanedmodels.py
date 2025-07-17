import os
from pathlib import Path
from django.core.management.base import BaseCommand, CommandError
from ddpui.models.org import Org, OrgDbt
from ddpui.models.dbt_workflow import OrgDbtModel


class Command(BaseCommand):
    """Removes SQL files without corresponding OrgDbt models"""

    help = "Removes SQL files without corresponding OrgDbt models"

    def add_arguments(self, parser):
        """Adds command line arguments"""
        parser.add_argument("org", help="Org name or slug")
        parser.add_argument("--delete", action="store_true")

    def handle(self, *args, **options):
        org = Org.objects.filter(name=options["org"]).first()
        if org is None:
            org = Org.objects.filter(slug=options["org"]).first()

        if org is None:
            raise CommandError("Org not found")

        orgdbt = OrgDbt.objects.filter(org=org).first()
        if orgdbt is None:
            raise CommandError("OrgDbt not found for " + org.slug)

        # these are relative to the OrgDbt.project_dir
        # which is relative to the os.environ['CLIENTDBT_ROOT']
        model_sql_files = OrgDbtModel.objects.filter(orgdbt=orgdbt, type="model").values_list(
            "sql_path", flat=True
        )
        model_sql_files = list(model_sql_files)
        # now read all files in the models/ folder
        clientdbt_root = os.getenv("CLIENTDBT_ROOT")
        if clientdbt_root is None:
            raise CommandError("CLIENTDBT_ROOT environment variable not set")
        project_dir = Path(clientdbt_root) / orgdbt.project_dir

        models_dir = project_dir / "models"
        if not models_dir.exists():
            self.stdout.write(f"Models directory not found: {models_dir}")
            return

        for sql_file in (models_dir).rglob("*.sql"):
            relative_pathname = str(Path(sql_file).relative_to(project_dir))
            if relative_pathname not in model_sql_files:
                if options["delete"]:
                    try:
                        self.stdout.write(f"Deleting {sql_file}")
                        os.unlink(sql_file)
                    except OSError as e:
                        self.stderr.write(f"Error deleting {sql_file}: {e}")
                else:
                    self.stdout.write(f"Will delete {sql_file}")

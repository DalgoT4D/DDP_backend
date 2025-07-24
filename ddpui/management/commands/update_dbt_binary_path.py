"""Updates the dbt binary path in Prefect deployments for dbt Core Operation tasks"""

import os
from pathlib import Path
from dotenv import load_dotenv
from django.core.management.base import BaseCommand, CommandError

from ddpui.models.org import Org, OrgDataFlowv1
from ddpui.ddpprefect.prefect_service import prefect_put, prefect_get
from ddpui.ddpprefect import DBTCORE

load_dotenv()


class Command(BaseCommand):
    """Updates the dbt binary path in Prefect deployments for dbt Core Operation tasks"""

    help = "Updates the dbt binary path in Prefect deployments for an organization"

    def add_arguments(self, parser):
        """Add arguments to the command"""
        parser.add_argument(
            "org_slug",
            type=str,
            help="The organization slug to update deployments for",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Show what would be changed without making actual changes",
        )

    def handle(self, *args, **options):
        """Handle the command execution"""
        org_slug = options["org_slug"]
        dry_run = options["dry_run"]

        # Validate organization exists
        try:
            org = Org.objects.get(slug=org_slug)
        except Org.DoesNotExist:
            raise CommandError(f"Organization with slug '{org_slug}' does not exist")

        if org.dbt is None:
            raise CommandError("dbt is not set up for org")

        dbt_binary_path = Path(os.getenv("DBT_VENV")) / org.dbt.dbt_venv / "bin/dbt"

        # Get all deployments for this organization
        deployments = OrgDataFlowv1.objects.filter(org=org)

        if not deployments.exists():
            self.stdout.write(
                self.style.WARNING(f"No deployments found for organization '{org_slug}'")
            )
            return

        self.stdout.write(
            self.style.SUCCESS(
                f"Found {deployments.count()} deployment(s) for organization '{org_slug}'"
            )
        )

        updated_count = 0
        for deployment in deployments:
            try:
                # Fetch deployment from Prefect
                prefect_deployment = prefect_get(f"deployments/{deployment.deployment_id}")
                deployment_params = prefect_deployment["parameters"]

                # Check if deployment has tasks in config
                if "config" not in deployment_params or "tasks" not in deployment_params["config"]:
                    self.stdout.write(
                        self.style.WARNING(
                            f"Deployment {deployment.deployment_name} ({deployment.deployment_id}) "
                            "has no tasks configuration, skipping"
                        )
                    )
                    continue

                modified = False

                # Iterate through tasks in deployment config
                for task in deployment_params["config"]["tasks"]:
                    if task["type"] == DBTCORE:
                        if "commands" not in task or not task["commands"]:
                            self.stdout.write(
                                self.style.WARNING(
                                    f"dbt Core Operation task in deployment {deployment.deployment_name} "
                                    "has no commands, skipping"
                                )
                            )
                            continue

                        # Update the first command (commands[0]) with new dbt binary path
                        old_command = task["commands"][0]

                        # Find the dbt binary part and replace it
                        command_parts = old_command.split()
                        if command_parts:
                            command_parts[0] = dbt_binary_path
                            new_command = " ".join(command_parts)

                            if old_command != new_command:
                                task["commands"][0] = new_command
                                modified = True

                                self.stdout.write(
                                    f"Deployment: {deployment.deployment_name} ({deployment.deployment_id})"
                                )
                                self.stdout.write(f"  Old command: {old_command}")
                                self.stdout.write(f"  New command: {new_command}")
                                self.stdout.write("")

                # Update deployment if modified and not dry run
                if modified:
                    if not dry_run:
                        prefect_put(
                            f"v1/deployments/{deployment.deployment_id}",
                            {"deployment_params": deployment_params},
                        )
                        self.stdout.write(
                            self.style.SUCCESS(
                                f"Updated deployment {deployment.deployment_name} ({deployment.deployment_id})"
                            )
                        )
                    else:
                        self.stdout.write(
                            self.style.WARNING(
                                f"[DRY RUN] Would update deployment {deployment.deployment_name} ({deployment.deployment_id})"
                            )
                        )
                    updated_count += 1

            except Exception as e:
                self.stdout.write(
                    self.style.ERROR(
                        f"Error processing deployment {deployment.deployment_name} ({deployment.deployment_id}): {str(e)}"
                    )
                )

        # Summary
        if dry_run:
            self.stdout.write(
                self.style.SUCCESS(
                    f"\n[DRY RUN] Would update {updated_count} deployment(s) for organization '{org_slug}'"
                )
            )
        else:
            self.stdout.write(
                self.style.SUCCESS(
                    f"\nSuccessfully updated {updated_count} deployment(s) for organization '{org_slug}'"
                )
            )

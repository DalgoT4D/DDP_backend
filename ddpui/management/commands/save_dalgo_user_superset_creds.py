"""Save Superset credentials for an organization"""

import sys
import getpass
from django.core.management.base import BaseCommand
from ddpui.models.org import Org
from ddpui.utils import secretsmanager


class Command(BaseCommand):
    """Save Superset credentials for an organization in secrets manager"""

    help = "Save Superset username and password for an organization"

    def add_arguments(self, parser):
        """adds command line arguments"""
        parser.add_argument("--orgslug", type=str, required=True, help="Organization slug")
        parser.add_argument("--username", type=str, required=True, help="Superset username")
        parser.add_argument(
            "--password",
            type=str,
            help="Superset password (will prompt if not provided)",
        )
        parser.add_argument(
            "--overwrite",
            action="store_true",
            help="Overwrite existing Superset credentials if they exist",
        )

    def handle(self, *args, **options):
        # Get the organization
        org = Org.objects.filter(slug=options["orgslug"]).first()
        if not org:
            self.stdout.write(
                self.style.ERROR(f"Organization with slug '{options['orgslug']}' not found")
            )
            sys.exit(1)

        # Check if credentials already exist
        if org.dalgouser_superset_creds_key and not options["overwrite"]:
            self.stdout.write(
                self.style.ERROR(
                    f"Organization '{org.name}' already has Superset credentials saved. "
                    "Use --overwrite to replace them."
                )
            )
            sys.exit(1)

        # Get password
        password = options.get("password")
        if not password:
            password = getpass.getpass("Enter Superset password: ")
            password_confirm = getpass.getpass("Confirm Superset password: ")
            if password != password_confirm:
                self.stdout.write(self.style.ERROR("Passwords do not match"))
                sys.exit(1)

        # Prepare credentials
        credentials = {
            "username": options["username"],
            "password": password,
        }

        # Save to secrets manager
        try:
            if org.dalgouser_superset_creds_key and options["overwrite"]:
                # Delete old secret first
                try:
                    aws_sm = secretsmanager.get_client()
                    aws_sm.delete_secret(SecretId=org.dalgouser_superset_creds_key)
                    self.stdout.write("Deleted existing credentials")
                except Exception:
                    # If deletion fails, continue anyway
                    pass

            secret_id = secretsmanager.save_dalgo_user_superset_credentials(credentials)

            # Update org with the secret key
            org.dalgouser_superset_creds_key = secret_id
            org.save()

            self.stdout.write(
                self.style.SUCCESS(
                    f"Successfully saved Superset credentials for organization '{org.name}'"
                )
            )
            self.stdout.write(f"Secret ID: {secret_id}")

        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Failed to save credentials: {str(e)}"))
            sys.exit(1)

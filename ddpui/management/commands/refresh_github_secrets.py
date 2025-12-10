from dotenv import load_dotenv
from django.core.management.base import BaseCommand

from ddpui.models.org_user import Org
from ddpui.utils import secretsmanager
from ddpui.utils.custom_logger import CustomLogger

load_dotenv()

logger = CustomLogger("ddpui")


class Command(BaseCommand):
    """
    This script refreshes the GitHub access token secrets in secrets manager for all orgs.
    It fetches the existing secret, deletes it, creates a new one with fresh name,
    and updates the org.dbt.gitrepo_access_token_secret reference.
    """

    help = "Refreshes GitHub access token secrets for all organizations"

    def add_arguments(self, parser):
        parser.add_argument("--org-slug", required=False, help="Process only this org")
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Show what would be done without making changes",
        )

    def handle(self, *args, **options):
        dry_run = options["dry_run"]
        org_slug = options.get("org_slug")

        if org_slug:
            orgs = Org.objects.filter(slug=org_slug)
            if not orgs.exists():
                self.stdout.write(self.style.ERROR(f"Org with slug '{org_slug}' not found"))
                return
        else:
            orgs = Org.objects.all()

        success_count = 0
        skip_count = 0
        error_count = 0

        for org in orgs:
            self.stdout.write(f"\nProcessing org: {org.slug}")

            # Check if org has dbt configured
            if not org.dbt:
                self.stdout.write(self.style.WARNING(f"  - No dbt workspace configured, skipping"))
                skip_count += 1
                continue

            # Check if org has a github access token secret
            old_secret_key = org.dbt.gitrepo_access_token_secret
            if not old_secret_key:
                self.stdout.write(
                    self.style.WARNING(f"  - No gitrepo_access_token_secret configured, skipping")
                )
                skip_count += 1
                continue

            self.stdout.write(f"  - Current value: {old_secret_key[:20]}...")

            try:
                # Check if the value is a secret key reference or the actual token
                is_secret_key = old_secret_key.startswith("gitrepoAccessToken")

                if dry_run:
                    if is_secret_key:
                        self.stdout.write(f"  - [DRY RUN] Value is a secret key reference")
                        self.stdout.write(f"  - [DRY RUN] Would fetch secret: {old_secret_key}")
                        self.stdout.write(f"  - [DRY RUN] Would delete old secret")
                    else:
                        self.stdout.write(
                            self.style.WARNING(
                                f"  - [DRY RUN] Value is the actual token (not a secret key)"
                            )
                        )
                    self.stdout.write(f"  - [DRY RUN] Would create new secret")
                    self.stdout.write(
                        f"  - [DRY RUN] Would update org.dbt.gitrepo_access_token_secret"
                    )
                    success_count += 1
                    continue

                if is_secret_key:
                    # Value is a secret key reference - fetch from secrets manager
                    self.stdout.write(f"  - Value is a secret key reference")
                    secret_value = secretsmanager.retrieve_github_pat(old_secret_key)
                    if not secret_value:
                        self.stdout.write(
                            self.style.ERROR(f"  - Could not retrieve secret content, skipping")
                        )
                        error_count += 1
                        continue

                    self.stdout.write(f"  - Retrieved secret content successfully")

                    # Delete the old secret from secrets manager
                    secretsmanager.delete_github_pat(old_secret_key)
                    self.stdout.write(f"  - Deleted old secret: {old_secret_key}")
                else:
                    # Value is the actual token stored in DB - use it directly
                    self.stdout.write(
                        self.style.WARNING(
                            f"  - Value is the actual token (not a secret key), will migrate to secrets manager"
                        )
                    )
                    secret_value = old_secret_key

                # Create a new secret with the token value
                new_secret_key = secretsmanager.save_github_pat(secret_value)
                self.stdout.write(f"  - Created new secret: {new_secret_key}")

                # Update the org.dbt reference with the new secret key
                org.dbt.gitrepo_access_token_secret = new_secret_key
                org.dbt.save()
                self.stdout.write(
                    self.style.SUCCESS(f"  - Updated org.dbt.gitrepo_access_token_secret")
                )

                success_count += 1

            except Exception as e:
                self.stdout.write(self.style.ERROR(f"  - Error processing org: {str(e)}"))
                error_count += 1
                continue

        self.stdout.write("\n" + "=" * 50)
        self.stdout.write(f"Summary:")
        self.stdout.write(self.style.SUCCESS(f"  Success: {success_count}"))
        self.stdout.write(self.style.WARNING(f"  Skipped: {skip_count}"))
        self.stdout.write(self.style.ERROR(f"  Errors: {error_count}"))

        if dry_run:
            self.stdout.write(self.style.WARNING("\n[DRY RUN] No changes were made"))

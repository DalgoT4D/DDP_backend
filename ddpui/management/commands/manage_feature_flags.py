"""
Management command to manage feature flags for organizations
"""
from django.core.management.base import BaseCommand
from ddpui.models.org import Org
from ddpui.utils.feature_flags import (
    FEATURE_FLAGS,
    enable_feature_flag,
    disable_feature_flag,
    is_feature_flag_enabled,
    enable_all_global_feature_flags,
    get_all_feature_flags_for_org,
)


class Command(BaseCommand):
    help = "Manage feature flags for organizations"

    def add_arguments(self, parser):
        parser.add_argument("flag_name", type=str, nargs="?", help="Name of the feature flag")
        parser.add_argument(
            "--org-slug", type=str, help="Organization slug (if not provided, will be global)"
        )
        parser.add_argument("--enable", action="store_true", help="Enable the feature flag")
        parser.add_argument("--disable", action="store_true", help="Disable the feature flag")
        parser.add_argument("--list-all", action="store_true", help="List all feature flags")
        parser.add_argument(
            "--enable-all-global",
            action="store_true",
            help="Enable all available feature flags globally",
        )

    def handle(self, *args, **options):
        flag_name = options["flag_name"]
        org_slug = options.get("org_slug")

        # Get organization if slug is provided
        org = None
        if org_slug:
            try:
                org = Org.objects.get(slug=org_slug)
                self.stdout.write(f"Working with organization: {org.name} ({org.slug})")
            except Org.DoesNotExist:
                self.stdout.write(
                    self.style.ERROR(f'Organization with slug "{org_slug}" not found')
                )
                return
        else:
            self.stdout.write("Working with global feature flags")

        # Validate flag name (not needed for list-all)
        if flag_name not in FEATURE_FLAGS and not options["list_all"] and flag_name:
            self.stdout.write(
                self.style.WARNING(
                    f'Flag "{flag_name}" not in predefined flags: {list(FEATURE_FLAGS.keys())}'
                )
            )
            self.stdout.write("Continuing anyway...")

        # Handle different actions
        if options["enable_all_global"]:
            self.enable_all_global_flags()
        elif options["list_all"]:
            self.list_all_flags(org)
        elif not flag_name:
            self.stdout.write(
                self.style.ERROR(
                    "Please provide a flag_name or use --list-all or --enable-all-global"
                )
            )
        elif options["enable"]:
            self.enable_flag(flag_name, org)
        elif options["disable"]:
            self.disable_flag(flag_name, org)
        else:
            self.stdout.write(
                self.style.ERROR(
                    "Please specify an action: --enable, --disable, --list-all, or --enable-all-global"
                )
            )

    def enable_flag(self, flag_name, org):
        """Enable a feature flag"""
        result = enable_feature_flag(flag_name, org)
        if result:
            self.stdout.write(self.style.SUCCESS(f'✓ Enabled flag "{flag_name}"'))
        else:
            self.stdout.write(self.style.ERROR(f'Failed to enable flag "{flag_name}"'))

    def disable_flag(self, flag_name, org):
        """Disable a feature flag"""
        # First check if flag exists
        current_status = is_feature_flag_enabled(flag_name, org)

        if current_status is None:
            # Flag doesn't exist at all
            self.stdout.write(self.style.ERROR(f'Flag "{flag_name}" does not exist'))
            return

        if not current_status:
            # Flag exists but is already disabled
            self.stdout.write(self.style.WARNING(f'Flag "{flag_name}" was already disabled'))
            return

        # Flag exists and is enabled, so disable it
        result = disable_feature_flag(flag_name, org)
        if result is False:
            self.stdout.write(self.style.SUCCESS(f'✓ Disabled flag "{flag_name}"'))
        else:
            self.stdout.write(self.style.ERROR(f'Failed to disable flag "{flag_name}"'))

    def list_all_flags(self, org):
        """List all feature flags for organization or globally"""
        # Use the utility function for both org and global flags
        flags_dict = get_all_feature_flags_for_org(org)
        scope = f'organization "{org.slug}"' if org else "global scope"

        if not flags_dict:
            self.stdout.write(f"No feature flags found for {scope}")
            return

        self.stdout.write("\nFeature Flags:")
        self.stdout.write("-" * 50)

        for flag_name, flag_value in flags_dict.items():
            status = "ENABLED" if flag_value else "DISABLED"
            color = self.style.SUCCESS if flag_value else self.style.ERROR
            description = FEATURE_FLAGS.get(flag_name, "No description")
            self.stdout.write(f"{flag_name:<20} {color(status):<20} {description}")

        # Show usage examples
        self.stdout.write("\n" + "=" * 60)
        self.stdout.write("Usage examples:")
        if org:
            self.stdout.write(
                f"python manage.py manage_feature_flags EXPLORE_DATA --org-slug {org.slug} --enable"
            )
            self.stdout.write(
                f"python manage.py manage_feature_flags EXPLORE_DATA --org-slug {org.slug} --disable"
            )
        else:
            self.stdout.write("python manage.py manage_feature_flags EXPLORE_DATA --enable")
            self.stdout.write("python manage.py manage_feature_flags EXPLORE_DATA --disable")
        self.stdout.write("python manage.py manage_feature_flags --list-all")

    def enable_all_global_flags(self):
        """Enable all available feature flags globally"""
        self.stdout.write("Enabling all available feature flags globally...")
        self.stdout.write("-" * 50)

        # Use the utility function
        results = enable_all_global_feature_flags()

        enabled_count = 0
        failed_count = 0

        for flag_name, result in results.items():
            if result:
                self.stdout.write(self.style.SUCCESS(f"✓ Enabled {flag_name}"))
                enabled_count += 1
            else:
                self.stdout.write(self.style.ERROR(f"✗ Failed to enable {flag_name}"))
                failed_count += 1

        self.stdout.write("-" * 50)
        self.stdout.write(f"Summary: {enabled_count} enabled, {failed_count} failed")

        if enabled_count > 0:
            self.stdout.write(
                self.style.SUCCESS(f"✓ Successfully enabled {enabled_count} global feature flags")
            )

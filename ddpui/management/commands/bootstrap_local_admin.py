from pathlib import Path

from django.conf import settings
from django.contrib.auth.models import User
from django.core.management import BaseCommand, call_command
from django.utils.text import slugify

from ddpui.models.org import Org
from ddpui.models.org_plans import OrgPlans, OrgPlanType
from ddpui.models.org_preferences import OrgPreferences
from ddpui.models.org_user import OrgUser, UserAttributes
from ddpui.models.role_based_access import Permission, Role, RolePermission
from ddpui.models.userpreferences import UserPreferences
from ddpui.utils.constants import DALGO_WITH_SUPERSET


class Command(BaseCommand):
    help = "Seeds local roles/permissions and creates or updates the local admin account."

    def add_arguments(self, parser):
        parser.add_argument("--email", default="admin@gmail.com")
        parser.add_argument("--password", default="Admin@1234")
        parser.add_argument("--org-name", default="Admin Dev")
        parser.add_argument("--org-slug", default="admin-dev")
        parser.add_argument("--role", default="super-admin")

    def handle(self, *args, **options):
        self._ensure_rbac_seeded()

        email = options["email"].strip().lower()
        password = options["password"]
        org_name = options["org_name"].strip()
        org_slug = (options["org_slug"] or slugify(org_name)).strip().lower()
        role_slug = options["role"].strip()

        role = Role.objects.filter(slug=role_slug).first()
        if role is None:
            self.stderr.write(
                self.style.ERROR(
                    f"Role '{role_slug}' does not exist even after seeding RBAC fixtures."
                )
            )
            raise SystemExit(1)

        org, org_created = Org.objects.get_or_create(
            slug=org_slug,
            defaults={"name": org_name},
        )
        org_changed = False
        if org.name != org_name:
            org.name = org_name
            org_changed = True
        if org_changed:
            org.save(update_fields=["name", "updated_at"])

        user = User.objects.filter(email=email).first()
        if user is None:
            user = User.objects.filter(username=email).first()

        user_created = False
        if user is None:
            user = User.objects.create_user(username=email, email=email, password=password)
            user_created = True

        user.username = email
        user.email = email
        user.is_active = True
        user.is_staff = True
        user.is_superuser = True
        user.set_password(password)
        user.save()

        user_attributes, _ = UserAttributes.objects.get_or_create(user=user)
        user_attributes.email_verified = True
        user_attributes.can_create_orgs = True
        user_attributes.is_platform_admin = True
        user_attributes.save()

        orguser, orguser_created = OrgUser.objects.get_or_create(
            user=user,
            org=org,
            defaults={"new_role": role, "email_verified": True},
        )
        orguser_changed = False
        if orguser.new_role_id != role.id:
            orguser.new_role = role
            orguser_changed = True
        if not orguser.email_verified:
            orguser.email_verified = True
            orguser_changed = True
        if orguser_changed:
            orguser.save()

        OrgPlans.objects.get_or_create(
            org=org,
            defaults={
                "base_plan": OrgPlanType.INTERNAL.value,
                "superset_included": False,
                "subscription_duration": "Monthly",
                "features": DALGO_WITH_SUPERSET,
                "can_upgrade_plan": True,
            },
        )
        OrgPreferences.objects.get_or_create(org=org)
        UserPreferences.objects.get_or_create(
            orguser=orguser, defaults={"enable_email_notifications": True}
        )

        self.stdout.write(
            self.style.SUCCESS(
                "Local admin bootstrap complete: "
                f"org_created={org_created}, user_created={user_created}, orguser_created={orguser_created}"
            )
        )
        self.stdout.write(f"Email: {email}")
        self.stdout.write(f"Org slug: {org_slug}")
        self.stdout.write(f"Role: {role_slug}")

    def _ensure_rbac_seeded(self):
        if Role.objects.exists() and Permission.objects.exists() and RolePermission.objects.exists():
            return

        seed_dir = Path(settings.BASE_DIR) / "seed"
        fixtures = [
            seed_dir / "001_roles.json",
            seed_dir / "002_permissions.json",
            seed_dir / "003_role_permissions.json",
        ]

        for fixture in fixtures:
            call_command("loaddata", str(fixture))

"""Creates an Org and an OrgUser by calling the same functions the API uses"""

import sys
from django.core.management.base import BaseCommand
from django.contrib.auth.models import User
from django.utils.text import slugify

from ddpui.models.org_user import OrgUser, UserAttributes
from ddpui.models.org_plans import OrgPlanType
from ddpui.models.role_based_access import Role
from ddpui.core import orgfunctions, orguserfunctions
from ddpui.schemas.org_schema import CreateOrgSchema


class Command(BaseCommand):
    """Creates an Org and an admin OrgUser"""

    help = "Creates an Org, OrgPlan, and OrgUser — mirrors the POST /v1/organizations/ API"

    def add_arguments(self, parser):
        parser.add_argument("orgname", type=str, help="Name of the Org")
        parser.add_argument("email", type=str, help="Email address of the OrgUser")
        parser.add_argument("password", type=str, help="Password for the User")
        parser.add_argument(
            "--role",
            type=str,
            default="admin",
            help="Role slug for the OrgUser (default: admin)",
        )
        parser.add_argument(
            "--plan",
            type=str,
            default="internal",
            choices=["free-trial", "dalgo", "internal"],
            help="OrgPlan type (default: internal)",
        )

    def handle(self, *args, **options):
        role = Role.objects.filter(slug=options["role"]).first()
        if role is None:
            print(f"Role '{options['role']}' not found — run: python manage.py loaddata seed/*.json")
            sys.exit(1)

        plan_map = {
            "free-trial": OrgPlanType.FREE_TRIAL,
            "dalgo": OrgPlanType.DALGO,
            "internal": OrgPlanType.INTERNAL,
        }
        base_plan = plan_map[options["plan"]]

        # create / fetch User
        if not User.objects.filter(email=options["email"]).exists():
            User.objects.create_user(
                email=options["email"], username=options["email"], password=options["password"]
            )
            print(f"User {options['email']} created")
        else:
            print(f"User {options['email']} already exists")

        user = User.objects.get(email=options["email"])

        ua, created = UserAttributes.objects.get_or_create(user=user)
        ua.email_verified = True
        ua.can_create_orgs = True
        ua.save()
        print(f"UserAttributes {'created' if created else 'updated'} for {user.email}")

        # create / fetch a temporary OrgUser stub so we can pass it to the API functions
        orguser = OrgUser.objects.filter(user=user, org=None).first()
        if orguser is None:
            orguser = OrgUser.objects.filter(user=user).first()
        if orguser is None:
            orguser = OrgUser.objects.create(user=user, new_role=role, email_verified=True)
            print(f"OrgUser stub created for {user.email}")

        # mirror POST /v1/organizations/
        payload = CreateOrgSchema(
            name=options["orgname"],
            slug=slugify(options["orgname"]),
            base_plan=base_plan.value,
            can_upgrade_plan=True,
            superset_included=False,
            subscription_duration="Monthly",
        )

        org, error = orgfunctions.create_organization(payload)
        if error:
            print(f"Error creating org: {error}")
            sys.exit(1)
        print(f"Org '{org.name}' created (airbyte workspace + prefect server block ready)")

        orguserfunctions.ensure_orguser_for_org(orguser, org)
        print(f"OrgUser linked: {user.email} → {org.name} ({role.name})")

        org_plan, error = orgfunctions.create_org_plan(payload, org)
        if error:
            print(f"Error creating org plan: {error}")
        else:
            print(f"OrgPlan created: {base_plan.value}")

        print("\nDone. Next steps (via UI or API):")
        print("  - Add a warehouse (POST /organizations/warehouse/)")
        print("  - Configure dbt workspace if needed")

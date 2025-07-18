"""
This is a temporary management command created to fix a bug in the original createorganduser command.

The original error:
- The original command in createorganduser.py was trying to set both role=3 (old field) and new_role=role (new field)
- However, the OrgUser model has been updated to remove the old 'role' field entirely
- This caused a TypeError: OrgUser() got unexpected keyword arguments: 'role'

What this fixed version does:
- Only uses the new 'new_role' field which is a ForeignKey to the Role model
- Properly handles role assignment using the Role model's slug
- Available role slugs are: super-admin, account-manager, pipeline-manager, analyst, guest

Example usage:
python manage.py temp_create_org_user "Org Name" "user@email.com" password --role super-admin

Note: This is a temporary fix. The original createorganduser.py should be updated to remove the old role field.
"""

import os
import sys
import getpass
from django.core.management.base import BaseCommand

from django.contrib.auth.models import User
from django.utils.text import slugify
from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser, UserAttributes
from ddpui.models.role_based_access import Role
from ddpui.core.orgfunctions import create_organization
from ddpui.models.org import OrgSchema


class Command(BaseCommand):
    """Creates an Org and an admin OrgUser"""

    help = "Creates an Org and an OrgUser"

    def add_arguments(self, parser):
        """adds command line arguments"""
        parser.add_argument("orgname", type=str, help="Name of the Org")
        parser.add_argument("email", type=str, help="Email address of the OrgUser")
        parser.add_argument(
            "password",
            type=str,
            help="Password if creating a new User; can also supply via PASSWORD env var",
            nargs="?",
        )
        parser.add_argument(
            "--role",
            type=str,
            help="Role slug (super-admin, account-manager, pipeline-manager, analyst, guest). Default is account-manager",
            default="account-manager",
        )

    def handle(self, *args, **options):
        # First ensure the requested role exists in the database
        # This requires that seed/*.json has been loaded with python manage.py loaddata
        role = Role.objects.filter(slug=options["role"]).first()
        if not role:
            self.stdout.write(
                f"Role with slug '{options['role']}' does not exist, please run 'python manage.py loaddata seed/*.json'"
            )
            sys.exit(1)

        # Create or fetch the organization
        # Uses the orgfunctions.create_organization which handles Airbyte workspace setup
        org = Org.objects.filter(name=options["orgname"]).first()
        if not org:
            org_schema = OrgSchema(name=options["orgname"], slug=slugify(options["orgname"]))
            org, error = create_organization(org_schema)
            if error:
                self.stdout.write(error)
                sys.exit(1)
            self.stdout.write(f"Org {options['orgname']} created")
        else:
            self.stdout.write(f"Org {options['orgname']} already exists")

        # Create or fetch the Django User
        # Password can be provided via command line arg, env var, or interactive prompt
        user = User.objects.filter(email=options["email"]).first()
        if not user:
            password = options["password"]
            if not password:
                password = os.getenv("PASSWORD")
            if not password:
                password = getpass.getpass("Enter the password for the OrgUser: ")
                password_confirm = getpass.getpass("Re-enter the password for the OrgUser: ")
                if password != password_confirm:
                    self.stdout.write("Passwords do not match")
                    sys.exit(1)
            if not password:
                self.stdout.write("Password is required")
                sys.exit(1)
            user = User.objects.create_user(
                email=options["email"], username=options["email"], password=password
            )
            self.stdout.write(f"User {options['email']} created")
        else:
            self.stdout.write(f"User {options['email']} already exists")

        # Create the OrgUser - this links the User to the Org with a specific role
        # Key difference from original command: Only uses new_role field, not the deprecated role field
        orguser = OrgUser.objects.filter(org=org, user=user).first()
        if not orguser:
            orguser = OrgUser.objects.create(org=org, user=user, new_role=role, email_verified=True)
            self.stdout.write(
                f"OrgUser {user.email} created for Org {org.name} with role {role.name}"
            )
        else:
            self.stdout.write(f"OrgUser {user.email} already exists for Org {org.name}")

        # Set up additional user attributes
        # These are required for proper system functionality
        ua = UserAttributes.objects.filter(user=user).first()
        if not ua:
            ua = UserAttributes.objects.create(user=user)
            self.stdout.write("UserAttributes created for User")
        ua.email_verified = True
        ua.can_create_orgs = True
        ua.save()
        self.stdout.write(f"UserAttributes updated for User {user.email} with email_verified=True")

""" Creates and Org and an OrgUser """

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
            help="Role of the OrgUser; default is 'account-manager'",
            default="account-manager",
        )

    def handle(self, *args, **options):
        # ensure the Role exists
        role = Role.objects.filter(slug=options["role"]).first()
        if not role:
            print(
                f"Role with slug '{options['role']}' does not exist, please run 'python manage.py loaddata seed/*.json'"
            )
            sys.exit(1)

        # fetch / create the Org
        if not Org.objects.filter(name=options["orgname"]).exists():
            create_organization(
                OrgSchema(name=options["orgname"], slug=slugify(options["orgname"]))
            )
            print(f"Org {options['orgname']} created")
        else:
            print(f"Org {options['orgname']} already exists")
        org = Org.objects.filter(name=options["orgname"]).first()

        # fetch / create the User
        if not User.objects.filter(email=options["email"]).exists():
            password = options["password"]
            if not password:
                password = os.getenv("PASSWORD")
            if not password:
                password = getpass.getpass("Enter the password for the OrgUser: ")
                password_confirm = getpass.getpass("Re-enter the password for the OrgUser: ")
                if password != password_confirm:
                    print("Passwords do not match")
                    sys.exit(1)
            if not password:
                print("Password is required")
                sys.exit(1)
            User.objects.create_user(
                email=options["email"], username=options["email"], password=password
            )
            print(f"User {options['email']} created")
        else:
            print(f"User {options['email']} already exists")
        user = User.objects.filter(email=options["email"]).first()

        if not OrgUser.objects.filter(org=org, user=user).exists():
            OrgUser.objects.create(org=org, user=user, role=3, new_role=role, email_verified=True)
            print(f"OrgUser {user.email} created for Org {org.name} with role {role.name}")
        else:
            print(f"OrgUser {user.email} already exists for Org {org.name}")
        orguser = OrgUser.objects.filter(org=org, user=user).first()

        # ensure the UserAttributes exists with email_verified=True
        if not UserAttributes.objects.filter(user=user).exists():
            UserAttributes.objects.create(user=user)
            print("UserAttributes created for User")
        ua = UserAttributes.objects.filter(user=user).first()
        ua.email_verified = True
        ua.can_create_orgs = True
        ua.save()
        print(f"UserAttributes updated for User {user.email} with email_verified=True")

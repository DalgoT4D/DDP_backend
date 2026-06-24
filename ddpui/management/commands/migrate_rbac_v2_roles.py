"""Access Control v2 — collapse the 4 customer roles into 3 on an existing database.

  account-manager ─┐
                   ├─► admin   (slug renamed on the same row; pipeline-manager users repointed here)
  pipeline-manager ┘            then the pipeline-manager role row is deleted
  analyst  ──────────► analyst  (infra write/run slugs removed; infra view kept)
  guest    ──────────► member   (set to content-view-only)
  super-admin ───────► super-admin (untouched)

Fresh installs get the 3-role world straight from the seed fixtures (001_roles.json /
003_role_permissions.json), so this command is only for transforming pre-v2 databases
(staging / prod). Run it once per environment:

    python manage.py migrate_rbac_v2_roles            # apply
    python manage.py migrate_rbac_v2_roles --dry-run  # report only

It is idempotent — re-running after the collapse is a no-op. It deliberately does NOT
touch Redis: clear the role-permission cache afterwards with
`python manage.py clear_role_permissions` (the next request rebuilds it).
"""

from django.core.management.base import BaseCommand
from django.db import transaction

from ddpui.models.org_user import Invitation, OrgUser
from ddpui.models.role_based_access import Permission, Role, RolePermission
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui")

# analyst loses every infra write/run slug, keeps all infra view slugs
ANALYST_INFRA_WRITE_REMOVE = [
    "can_sync_sources",
    "can_create_dbt_docs",
    "can_create_dbt_model",
    "can_create_dbt_workspace",
    "can_edit_dbt_model",
    "can_edit_dbt_operation",
    "can_edit_dbt_workspace",
    "can_delete_dbt_model",
    "can_delete_dbt_operation",
    "can_delete_dbt_workspace",
    "can_create_orgtask",
    "can_delete_orgtask",
    "can_run_orgtask",
]

# member is content-view-only: viewing the four content resources + basics. No write of
# any kind and no infra visibility. Set EXACTLY (delete-all-then-add) so prod drift — e.g.
# can_create_dashboards granted to guests by migration 0137 — cannot leak through.
MEMBER_SLUGS = [
    "can_view_dashboards",
    "can_view_charts",
    "can_view_metrics",
    "can_view_kpis",
    "can_view_alerts",
    "can_view_orgusers",
    "can_view_flags",
    "can_request_llm_analysis_feature",
    "public",
]


def _set_role_permissions_exactly(role, slugs):
    """Replace a role's permission rows with exactly `slugs`."""
    RolePermission.objects.filter(role=role).delete()
    for slug in slugs:
        perm = Permission.objects.filter(slug=slug).first()
        if perm:
            RolePermission.objects.create(role=role, permission=perm)


def collapse_roles():
    """Apply the v2 role collapse to the current database. Idempotent."""
    account_manager = Role.objects.filter(slug="account-manager").first()
    if account_manager is None:
        logger.info("RBAC v2: account-manager role not present; nothing to collapse")
        return False

    # 1. account-manager → admin (same pk/level, new slug + name)
    account_manager.slug = "admin"
    account_manager.name = "Admin"
    account_manager.level = 4
    account_manager.save()

    # 2. guest → member
    guest = Role.objects.filter(slug="guest").first()
    if guest:
        guest.slug = "member"
        guest.name = "Member"
        guest.level = 1
        guest.save()

    # 3. merge pipeline-manager into admin: repoint its users + pending invites, then drop it
    pipeline_manager = Role.objects.filter(slug="pipeline-manager").first()
    if pipeline_manager:
        OrgUser.objects.filter(new_role=pipeline_manager).update(new_role=account_manager)
        # invited_new_role is on_delete=CASCADE — repoint before delete or invites vanish
        Invitation.objects.filter(invited_new_role=pipeline_manager).update(
            invited_new_role=account_manager
        )
        RolePermission.objects.filter(role=pipeline_manager).delete()
        pipeline_manager.delete()

    # 4. analyst: strip infra write/run slugs (keep content + infra view)
    analyst = Role.objects.filter(slug="analyst").first()
    if analyst:
        RolePermission.objects.filter(
            role=analyst, permission__slug__in=ANALYST_INFRA_WRITE_REMOVE
        ).delete()

    # 5. member: content-view-only, set exactly
    member = Role.objects.filter(slug="member").first()
    if member:
        _set_role_permissions_exactly(member, MEMBER_SLUGS)

    return True


class Command(BaseCommand):
    help = (
        "Collapse the 4 pre-v2 customer roles into 3 (admin / analyst / member) on an existing DB."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Report what would change without writing to the database.",
        )

    def handle(self, *args, **options):
        if options["dry_run"]:
            existing = set(Role.objects.values_list("slug", flat=True))
            if "account-manager" not in existing:
                self.stdout.write(
                    self.style.WARNING("Already collapsed (no account-manager role); no changes.")
                )
                return
            self.stdout.write(
                self.style.WARNING(
                    "DRY RUN — would: rename account-manager→admin, guest→member, "
                    "merge pipeline-manager→admin (repoint users + invites, then delete), "
                    "strip analyst infra write/run slugs, set member to view-only."
                )
            )
            return

        with transaction.atomic():
            changed = collapse_roles()

        if changed:
            self.stdout.write(self.style.SUCCESS("RBAC v2 role collapse applied."))
            self.stdout.write(
                "Now clear the Redis role cache: python manage.py clear_role_permissions"
            )
            logger.info("RBAC v2 role collapse applied")
        else:
            self.stdout.write(self.style.WARNING("Nothing to collapse (already v2)."))

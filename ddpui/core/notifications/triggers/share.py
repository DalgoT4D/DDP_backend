"""Direct-share notification triggers.

Fires from ``api/access_api.py``. Two shapes:

- ``notify_share_recipients`` — bulk add (POST /grants): new grants and
  view→edit upgrades notify their recipients. Downgrades and no-ops are silent.
- ``notify_row_level_change`` — single-row update (PATCH /grants/{id}): the
  targeted principal is notified on BOTH upgrade and downgrade (no-op silent).
  This is a deliberate flow-difference: the row dropdown is a targeted action,
  so the recipient should learn either direction; a bulk-re-share often includes
  incidental downgrades that don't warrant notification traffic.

All routes go through ``create_notification`` so the in-app row + email + org
Discord all fire. Group principals fan out to current OrgUserGroupMember
orguser ids. Notification failures are logged and swallowed — the API call is
not blocked by mail or Discord hiccups.
"""

from ddpui.core.notifications.notifications_functions import create_notification
from ddpui.core.notifications.triggers.access import resource_title, resource_url
from ddpui.models.org_user import OrgUser, OrgUserGroupMember
from ddpui.models.resource_share import LEVEL_RANK, ResourceShare
from ddpui.schemas.notifications_api_schemas import NotificationDataSchema
from ddpui.utils.custom_logger import CustomLogger


logger = CustomLogger("ddpui.notifications.triggers.share")


def snapshot_direct_levels(org, rtype: str, resource_id) -> dict:
    """Map ``(principal_type, principal_id) → access_level`` for direct grants
    on this resource, pre-mutation. Cascade rows and invitation-only rows are
    excluded — they're not part of the ``new / upgrade`` classifier.
    """
    rows = ResourceShare.objects.filter(
        org=org,
        resource_type=rtype,
        resource_id=str(resource_id),
        parent__isnull=True,
        invitation__isnull=True,
        principal_type__isnull=False,
    ).values_list("principal_type", "principal_id", "access_level")
    return {(pt, pid): lvl for pt, pid, lvl in rows}


def classify_share_recipients(before: dict, written: list, sender_orguser_id: int) -> dict:
    """Bucket ``written`` grant rows into ``new`` / ``upgrade`` recipients,
    grouped by target level. Returns ``{"new": {level: {orguser_ids}}, "upgrade": ...}``.

    Rules:
    - Cascade rows (``parent`` set) and invitation-only rows are skipped.
    - No-op re-saves and downgrades are skipped — nothing to notify.
    - Group principals expand to current OrgUserGroupMember orguser ids
      (invitation-only members are skipped).
    - The sender is filtered out (never notify yourself).
    - A user in both ``new`` and ``upgrade`` (rare) stays in ``new`` only —
      new access dominates over a level bump.
    """
    from ddpui.models.org_user import OrgUserGroupMember

    result: dict = {"new": {}, "upgrade": {}}
    for row in written:
        if row.parent_id is not None or row.invitation_id is not None:
            continue
        if row.principal_type is None or row.principal_id is None:
            continue
        key = (row.principal_type, row.principal_id)
        prev = before.get(key)
        if prev is None:
            klass = "new"
        elif LEVEL_RANK[row.access_level] > LEVEL_RANK[prev]:
            klass = "upgrade"
        else:
            continue

        if row.principal_type == "user":
            recipient_ids = [row.principal_id]
        elif row.principal_type == "group":
            recipient_ids = list(
                OrgUserGroupMember.objects.filter(
                    group_id=row.principal_id, orguser__isnull=False
                ).values_list("orguser_id", flat=True)
            )
        else:
            continue

        bucket = result[klass].setdefault(row.access_level, set())
        bucket.update(recipient_ids)

    new_all: set = set().union(*result["new"].values()) if result["new"] else set()
    for level in list(result["upgrade"].keys()):
        result["upgrade"][level] -= new_all
        if not result["upgrade"][level]:
            del result["upgrade"][level]
    for level in list(result["new"].keys()):
        result["new"][level].discard(sender_orguser_id)
        if not result["new"][level]:
            del result["new"][level]
    for level in list(result["upgrade"].keys()):
        result["upgrade"][level].discard(sender_orguser_id)
        if not result["upgrade"][level]:
            del result["upgrade"][level]

    return result


def notify_share_recipients(sender: OrgUser, rtype: str, resource, classified: dict) -> None:
    """Fire one ``create_notification`` per (class, level) bucket. Notification
    failure is logged but never fails the API call."""
    if not classified["new"] and not classified["upgrade"]:
        return
    title = resource_title(resource)
    url = resource_url(rtype, resource.pk)
    sender_email = sender.user.email

    for level, ids in classified["new"].items():
        try:
            create_notification(
                NotificationDataSchema(
                    author=sender_email,
                    message=(
                        f"{sender_email} shared {rtype} '{title}' with you at "
                        f"{level} access.\n{url}"
                    ),
                    email_subject=f"Shared with you: {title}",
                    urgent=False,
                    recipients=list(ids),
                )
            )
        except Exception as err:
            logger.error(f"share notification (new@{level}) failed: {err}")

    for level, ids in classified["upgrade"].items():
        try:
            create_notification(
                NotificationDataSchema(
                    author=sender_email,
                    message=(
                        f"{sender_email} upgraded your access on {rtype} '{title}' "
                        f"to {level}.\n{url}"
                    ),
                    email_subject=f"Access upgraded: {title}",
                    urgent=False,
                    recipients=list(ids),
                )
            )
        except Exception as err:
            logger.error(f"share notification (upgrade@{level}) failed: {err}")


def notify_row_level_change(
    sender: OrgUser,
    rtype: str,
    resource,
    row: ResourceShare,
    before_level: str,
) -> None:
    """Notify the principal on a targeted single-row level change (PATCH /grants).

    Fires on both upgrade (view → edit) and downgrade (edit → view). Cascade
    rows and invitation-only rows are silent (backend rejects cascade edits;
    invitation-only rows have no orguser to notify yet). Same-level saves and
    the sender-is-recipient case are silent."""
    if row.parent_id is not None or row.invitation_id is not None:
        return
    if row.principal_type is None or row.principal_id is None:
        return

    new_rank = LEVEL_RANK.get(row.access_level)
    old_rank = LEVEL_RANK.get(before_level)
    if new_rank is None or old_rank is None or new_rank == old_rank:
        return

    if row.principal_type == "user":
        recipient_ids = {row.principal_id}
    elif row.principal_type == "group":
        recipient_ids = set(
            OrgUserGroupMember.objects.filter(
                group_id=row.principal_id, orguser__isnull=False
            ).values_list("orguser_id", flat=True)
        )
    else:
        return

    recipient_ids.discard(sender.id)
    if not recipient_ids:
        return

    title = resource_title(resource)
    url = resource_url(rtype, resource.pk)
    sender_email = sender.user.email
    is_upgrade = new_rank > old_rank
    verb = "upgraded" if is_upgrade else "downgraded"
    message = (
        f"{sender_email} {verb} your access on {rtype} '{title}' " f"to {row.access_level}.\n{url}"
    )
    subject = f"Access {verb}: {title}"
    try:
        create_notification(
            NotificationDataSchema(
                author=sender_email,
                message=message,
                email_subject=subject,
                urgent=False,
                recipients=list(recipient_ids),
            )
        )
    except Exception as err:
        logger.error(f"row-level share notification failed: {err}")

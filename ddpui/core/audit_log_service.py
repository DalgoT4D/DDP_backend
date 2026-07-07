"""
Audit log service for creating immutable records of user-initiated actions.

KEY DESIGN:
- Audit logs are written in a BACKGROUND THREAD
- This means the user's request is not slowed down
- If the background write fails, it's logged but doesn't break the user's action

Example usage in an API endpoint:

    from ddpui.core.audit_log_service import create_audit_log
    from ddpui.models.audit_log import AuditLogResourceType, AuditLogAction

    def delete_dashboard(request, dashboard_id):
        # 1. Do the actual delete
        dashboard_service.delete(dashboard_id)

        # 2. Log it (this returns immediately, write happens in background)
        create_audit_log(
            org=request.orguser.org,
            orguser=request.orguser,
            resource_type=AuditLogResourceType.DASHBOARD,
            resource_id=str(dashboard_id),
            resource_name="My Dashboard",
            action=AuditLogAction.DELETE,
        )

        return {"success": True}
"""

import threading

import django.db

from ddpui.models.audit_log import AuditLog
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui.audit_log_service")


def create_audit_log(
    *,
    org,
    orguser,
    resource_type: str,
    resource_id: str,
    action: str,
    resource_name: str = "",
    field_changes: dict | None = None,
) -> None:
    """
    Writes an audit log entry in a background thread.

    This function NEVER raises an exception - if something goes wrong,
    it logs the error but doesn't break the caller's code.

    Args:
        org: The organization (Org model instance)
        orguser: The user performing the action (OrgUser, can be None)
        resource_type: Type of resource (use AuditLogResourceType values)
        resource_id: ID of the specific resource (as string)
        action: The action performed (use AuditLogAction values)
        resource_name: Human-readable name (optional, e.g., "Sales Dashboard")
        field_changes: Dict of changes (optional, e.g., {"name": {"old": "A", "new": "B"}})

    Example:
        create_audit_log(
            org=orguser.org,
            orguser=orguser,
            resource_type=AuditLogResourceType.DASHBOARD,
            resource_id="123",
            resource_name="Sales Dashboard",
            action=AuditLogAction.DELETE,
        )
    """
    try:
        # Start a background thread to do the actual write
        # daemon=True means the thread will be killed when main program exits
        t = threading.Thread(
            target=_write_audit_log,
            kwargs={
                "org_id": org.id,
                "orguser_id": orguser.id if orguser else None,
                "orguser_email": orguser.user.email if orguser else "",
                "resource_type": resource_type,
                "resource_id": resource_id,
                "resource_name": resource_name,
                "action": action,
                "field_changes": field_changes or {},
            },
            daemon=True,
        )
        t.start()
        # Note: we don't wait for the thread - we return immediately
    except Exception as err:
        # If thread fails to start, log it but don't crash
        logger.error("audit_log_service: failed to start write thread", exc_info=err)


def _write_audit_log(
    *,
    org_id: int,
    orguser_id: int | None,
    orguser_email: str,
    resource_type: str,
    resource_id: str,
    resource_name: str,
    action: str,
    field_changes: dict,
) -> None:
    """
    Internal function - does the actual database write.
    Runs in a background thread.

    IMPORTANT: We close the database connection in the 'finally' block.
    This is because Django gives each thread its own connection, and
    background threads don't automatically clean up their connections.
    """
    try:
        AuditLog.objects.create(
            org_id=org_id,
            orguser_id=orguser_id,
            orguser_email=orguser_email,
            resource_type=resource_type,
            resource_id=resource_id,
            resource_name=resource_name,
            action=action,
            field_changes=field_changes,
        )
    except Exception as err:
        # If DB write fails, log it but don't crash
        logger.error("audit_log_service: failed to write audit log", exc_info=err)
    finally:
        # Always close the connection to return it to the pool
        django.db.connection.close()


def compute_changes(
    before: dict, after: dict, exclude_fields: list[str] | None = None
) -> dict:
    """
    Compares two dictionaries and returns what changed.

    Use this when logging UPDATE actions to show what fields changed.

    Args:
        before: State before the change (e.g., {"name": "Old", "color": "red"})
        after: State after the change (e.g., {"name": "New", "color": "red"})
        exclude_fields: Fields to ignore (use for secrets like passwords!)

    Returns:
        Dict of changes: {"field": {"old": value, "new": value}}

    Examples:
        >>> compute_changes(
        ...     {"name": "Old", "value": 100},
        ...     {"name": "New", "value": 100}
        ... )
        {"name": {"old": "Old", "new": "New"}}

        >>> compute_changes(
        ...     {"name": "Test", "password": "secret1"},
        ...     {"name": "Test", "password": "secret2"},
        ...     exclude_fields=["password"]
        ... )
        {}  # password change is not reported
    """
    exclude = set(exclude_fields or [])
    changes = {}

    # Get all keys from both dicts
    all_keys = set(before.keys()) | set(after.keys())

    for key in all_keys:
        # Skip excluded fields (like passwords, tokens, etc.)
        if key in exclude:
            continue

        old_value = before.get(key)
        new_value = after.get(key)

        # Only record if value actually changed
        if old_value != new_value:
            changes[key] = {"old": old_value, "new": new_value}

    return changes

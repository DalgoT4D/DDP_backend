"""Management command to purge old audit log entries.

Reads AUDIT_LOG_RETENTION_DAYS from environment (default: 365) and deletes
all AuditLog entries older than that. Designed to be run manually or via
Celery Beat on a monthly schedule.
"""

import os
from datetime import timedelta

from django.core.management.base import BaseCommand
from django.utils import timezone

from ddpui.models.audit_log import AuditLog
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui")


class Command(BaseCommand):
    help = "Purge audit log entries older than AUDIT_LOG_RETENTION_DAYS (default: 365)"

    def add_arguments(self, parser):
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Show how many entries would be deleted without actually deleting them",
        )
        parser.add_argument(
            "--days",
            type=int,
            help="Override AUDIT_LOG_RETENTION_DAYS for this run",
        )

    def handle(self, *args, **options):
        dry_run = options["dry_run"]

        # Get retention days from argument, environment, or default
        retention_days = options.get("days")
        if retention_days is None:
            retention_days = int(os.getenv("AUDIT_LOG_RETENTION_DAYS", "365"))

        cutoff_date = timezone.now() - timedelta(days=retention_days)

        # Count entries to be deleted
        old_entries = AuditLog.objects.filter(timestamp__lt=cutoff_date)
        count = old_entries.count()

        if count == 0:
            self.stdout.write(
                self.style.SUCCESS(f"No audit log entries older than {retention_days} days")
            )
            return

        if dry_run:
            self.stdout.write(
                self.style.WARNING(
                    f"DRY RUN - Would delete {count} audit log entries older than {retention_days} days "
                    f"(before {cutoff_date.strftime('%Y-%m-%d %H:%M:%S')})"
                )
            )
        else:
            deleted_count, _ = old_entries.delete()
            self.stdout.write(
                self.style.SUCCESS(
                    f"Deleted {deleted_count} audit log entries older than {retention_days} days"
                )
            )
            logger.info(
                f"Purged {deleted_count} audit log entries older than {retention_days} days "
                f"(cutoff: {cutoff_date.isoformat()})"
            )

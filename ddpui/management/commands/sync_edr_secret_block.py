"""Scaffold the consolidated EDR Prefect Secret block from Django env vars,
and clean up the legacy per-field blocks it replaces.

The EDR flow (elementary send-report) needs three pieces of config on the
Prefect worker: an S3 access key, secret, and bucket name. Historically these
lived in three separate Prefect Secret blocks (`edr-aws-access-key`,
`edr-aws-access-secret`, `edr-s3-bucket`) — this command consolidates them
into a single JSON-encoded Secret block so the runner does one Secret.load()
instead of three, then deletes the three legacy blocks.

Env vars consumed:
  S3_AWS_ACCESS_KEY_ID
  S3_AWS_SECRET_ACCESS_KEY
  ELEMENTARY_S3_BUCKET

Prefect Secret block written — name defaults to `edr-s3-creds`, value is JSON:
  {
    "aws_access_key_id":     "<S3_AWS_ACCESS_KEY_ID>",
    "aws_secret_access_key": "<S3_AWS_SECRET_ACCESS_KEY>",
    "s3_bucket":             "<ELEMENTARY_S3_BUCKET>"
  }

Legacy blocks deleted from Prefect (idempotent — missing = no-op):
  edr-aws-access-key
  edr-aws-access-secret
  edr-s3-bucket

Idempotent — safe to re-run. Overwrites the consolidated block value each run
and swallows "already deleted" errors on the legacy blocks. Not tracked in
Django's OrgPrefectBlockv1 (these are global infra blocks), so only Prefect
state changes.
"""

import json
import os

from django.core.management.base import BaseCommand

from ddpui.ddpprefect import prefect_service
from ddpui.ddpprefect.schema import PrefectSecretBlockEdit


# Hardcoded — must match the name the proxy runner reads
# (prefect_flows.py and prefect_flows_runner.py:generate-edr branch).
# Don't parameterize: a mismatched name would silently break EDR runs.
CONSOLIDATED_BLOCK_NAME = "edr-s3-creds"

REQUIRED_ENV_VARS = (
    "S3_AWS_ACCESS_KEY_ID",
    "S3_AWS_SECRET_ACCESS_KEY",
    "ELEMENTARY_S3_BUCKET",
)

LEGACY_BLOCK_NAMES = (
    "edr-aws-access-key",
    "edr-aws-access-secret",
    "edr-s3-bucket",
)


class Command(BaseCommand):
    help = "Upsert the consolidated EDR Prefect Secret block and delete the 3 legacy blocks it replaces"

    def add_arguments(self, parser):
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Print what would happen; no writes to Prefect",
        )

    def handle(self, *args, **options):
        values = {name: os.getenv(name) for name in REQUIRED_ENV_VARS}
        missing = [name for name, val in values.items() if not val]
        if missing:
            self.stderr.write(self.style.ERROR(f"Missing env vars: {', '.join(missing)}"))
            return

        payload = {
            "aws_access_key_id": values["S3_AWS_ACCESS_KEY_ID"],
            "aws_secret_access_key": values["S3_AWS_SECRET_ACCESS_KEY"],
            "s3_bucket": values["ELEMENTARY_S3_BUCKET"],
        }
        block_name = CONSOLIDATED_BLOCK_NAME
        dry_run = options["dry_run"]

        # 1. Upsert consolidated block
        if dry_run:
            self.stdout.write(
                f"[dry-run] would upsert Prefect Secret block '{block_name}' "
                f"with keys: {list(payload.keys())}"
            )
        else:
            try:
                response = prefect_service.upsert_secret_block(
                    PrefectSecretBlockEdit(block_name=block_name, secret=json.dumps(payload))
                )
                self.stdout.write(
                    self.style.SUCCESS(
                        f"Upserted '{block_name}' (block_id={response.get('block_id')})"
                    )
                )
            except Exception as err:  # pylint: disable=broad-exception-caught
                self.stderr.write(self.style.ERROR(f"Failed to upsert '{block_name}': {err}"))
                return

        # 2. Delete legacy per-field blocks
        for legacy_name in LEGACY_BLOCK_NAMES:
            if dry_run:
                self.stdout.write(f"[dry-run] would delete legacy block '{legacy_name}' if present")
                continue

            try:
                block = prefect_service.get_secret_block_by_name(legacy_name)
            except Exception:  # pylint: disable=broad-exception-caught
                # Block doesn't exist — nothing to delete.
                self.stdout.write(f"  [skip] {legacy_name}: not present")
                continue

            block_id = block.get("block_id") or block.get("id")
            if not block_id:
                self.stdout.write(
                    f"  [skip] {legacy_name}: lookup returned no block_id ({block!r})"
                )
                continue

            try:
                prefect_service.delete_secret_block(block_id)
                self.stdout.write(f"  [del]  {legacy_name} (block_id={block_id})")
            except Exception as err:  # pylint: disable=broad-exception-caught
                self.stderr.write(
                    self.style.WARNING(f"  [warn] {legacy_name}: delete failed ({err})")
                )

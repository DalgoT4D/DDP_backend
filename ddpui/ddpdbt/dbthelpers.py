import json
import os
from pathlib import Path
import yaml
from ninja.errors import HttpError

from ddpui.ddpdbt.schema import DbtProjectParams
from ddpui.ddpprefect import SECRET, prefect_service
from ddpui.ddpprefect.schema import PrefectSecretBlockEdit
from ddpui.core.dbtfunctions import build_profile_dict, preprocess_airbyte_creds_for_dbt
from ddpui.core.git_manager import GitManager
from ddpui.core.orgdbt_manager import DbtProjectManager
from ddpui.models.org import Org, OrgWarehouse, OrgPrefectBlockv1
from ddpui.utils import secretsmanager
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddphelpers")


def create_or_update_dbt_profile_secret_blk(
    org: Org,
    warehouse: OrgWarehouse,
    airbyte_creds: dict,
) -> OrgPrefectBlockv1:
    """Upsert the org's dbt-profile Prefect Secret block — the runner-flow
    artifact read by proxy/prefect_flows_runner.py at flow-run start.

    Block name is deterministic: `dbt-profile-<org.slug>`.
    Value is JSON-encoded:
      {
        "wtype":          warehouse.wtype,
        "default_schema": <derived — see below>,
        "creds":          dbt_creds,
        "extras":         wh_extras,
      }
    - default_schema:  if `org.dbt` is set, uses `org.dbt.default_schema`.
                       Otherwise derived from `airbyte_creds` — postgres uses
                       creds["schema"], bigquery uses creds["dataset_id"].
    - dbt_creds:       airbyte destination fields mapped to dbt-postgres /
                       dbt-bigquery field names (via
                       preprocess_airbyte_creds_for_dbt).
    - wh_extras:       warehouse-specific settings that aren't credentials:
                       bigquery → {"location": ..., "priority": ...}
                       postgres → {}

    Works whether or not `org.dbt` is set. When `org.dbt` is not set, the
    function logs an info message and still upserts the block (default_schema
    is derived from airbyte_creds instead).

    Persistence:
      - Prefect Secret block via prefect_service.upsert_secret_block.
      - OrgPrefectBlockv1 row (block_type=SECRET) via update_or_create.
      - `warehouse.dbt_profile_secret_block` — authoritative source of truth
        going forward (used by post-sync ops + runner flows).
      - `org.dbt.dbt_profile_secret_block` — mirrored FK, kept only for
        backwards compatibility. Marked with a FUTURE-TODO to be removed once
        all consumers read from the warehouse. Only written when `org.dbt` is
        set (skipped for non-dbt orgs).

    Errors: exceptions from prefect_service.upsert_secret_block or the DB
    saves are NOT swallowed — they propagate up to the caller.

    Returns the OrgPrefectBlockv1 row on success.
    """
    default_schema = None
    if not org.dbt:
        logger.info(
            "org.dbt is not set for org=%s; still continuing to create/update the dbt-profile secret block",
            org.slug,
        )
        # set default schema from airbyte creds based on the warehouse type
        if warehouse.wtype == "postgres":
            if "schema" in airbyte_creds:
                logger.info(
                    "Setting default schema from airbyte creds for postgres: %s",
                    airbyte_creds["schema"],
                )
                default_schema = airbyte_creds["schema"]
        elif warehouse.wtype == "bigquery":
            if "dataset_id" in airbyte_creds:
                logger.info(
                    "Setting default schema from airbyte creds for bigquery: %s",
                    airbyte_creds["dataset_id"],
                )
                default_schema = airbyte_creds["dataset_id"]
    else:
        default_schema = org.dbt.default_schema

    dbt_creds, wh_extras = preprocess_airbyte_creds_for_dbt(warehouse, airbyte_creds)

    dbt_profile_secret_block_name = f"dbt-profile-{org.slug}"
    block_value = {
        "wtype": warehouse.wtype,
        "default_schema": default_schema,
        "creds": dbt_creds,
        "extras": wh_extras,
    }

    dbt_profile_secret_response = prefect_service.upsert_secret_block(
        PrefectSecretBlockEdit(
            block_name=dbt_profile_secret_block_name,
            secret=json.dumps(block_value),
        )
    )
    dbt_profile_secret_block_row, _ = OrgPrefectBlockv1.objects.update_or_create(
        block_name=dbt_profile_secret_response["block_name"],
        defaults={
            "org": org,
            "block_type": SECRET,
            "block_id": dbt_profile_secret_response["block_id"],
        },
    )

    # FUTURE-TODO: remove the org.dbt assignment since we are moving it to warehouse.dbt_profile_secret_block
    if org.dbt:
        org.dbt.dbt_profile_secret_block = dbt_profile_secret_block_row
        org.dbt.save(update_fields=["dbt_profile_secret_block"])

    warehouse.dbt_profile_secret_block = dbt_profile_secret_block_row
    warehouse.save(update_fields=["dbt_profile_secret_block"])

    logger.info(
        "Upserted dbt-profile secret block %s for org=%s",
        dbt_profile_secret_response["block_name"],
        org.slug,
    )

    return dbt_profile_secret_block_row


def write_dbt_profiles_yml(org: Org) -> Path:
    """Write <project_dir>/profiles/profiles.yml for the org, matching the shape
    proxy/prefect_flows_runner.py:dbtjob_v2_runner writes at flow-run time.

    Same source of truth (airbyte destination creds) + same preprocessing
    + same build_profile_dict, so backend-generated and runner-generated
    profiles.yml are structurally identical.

    Returns the absolute Path of the written profiles.yml.
    """
    if not org.dbt:
        raise HttpError(400, "dbt is not configured for this org")

    warehouse = OrgWarehouse.objects.filter(org=org).first()
    if not warehouse:
        raise HttpError(400, "warehouse not found for org")

    airbyte_creds = secretsmanager.retrieve_warehouse_credentials(warehouse)
    if not airbyte_creds:
        raise HttpError(400, "warehouse credentials not found")

    dbt_project_params = DbtProjectManager.gather_dbt_project_params(org, org.dbt)

    dbt_creds, wh_extras = preprocess_airbyte_creds_for_dbt(warehouse, airbyte_creds)

    dbt_project_filename = Path(dbt_project_params.project_dir) / "dbt_project.yml"
    if not dbt_project_filename.exists():
        raise HttpError(400, f"{dbt_project_filename} is missing")
    with open(dbt_project_filename, "r", encoding="utf-8") as f:
        dbt_project = yaml.safe_load(f)
    if "profile" not in dbt_project:
        raise HttpError(400, "could not find 'profile:' in dbt_project.yml")
    profile_name = dbt_project["profile"]

    profile_dict = build_profile_dict(
        profile_name=profile_name,
        wtype=warehouse.wtype,
        schema=org.dbt.default_schema,
        creds=dbt_creds,
        extras=wh_extras,
    )

    profile_dirname = Path(dbt_project_params.project_dir) / "profiles"
    os.makedirs(profile_dirname, exist_ok=True)

    # Mirror the runner's SSL cert handling (prefect_flows_runner.py:258-265).
    if warehouse.wtype == "postgres" and dbt_creds.get("sslrootcert_content"):
        cert_path = dbt_creds.get("sslrootcert") or os.path.join(
            dbt_project_params.project_dir, "..", "sslrootcert.pem"
        )
        os.makedirs(os.path.dirname(cert_path), exist_ok=True)
        with open(cert_path, "w", encoding="utf-8") as f:
            f.write(dbt_creds["sslrootcert_content"])
        target_key = profile_dict[profile_name]["target"]
        profile_dict[profile_name]["outputs"][target_key]["sslrootcert"] = cert_path

    profile_filename = profile_dirname / "profiles.yml"
    logger.info("writing dbt profile to %s", profile_filename)
    with open(profile_filename, "w", encoding="utf-8") as f:
        yaml.safe_dump(profile_dict, f)
    return profile_filename

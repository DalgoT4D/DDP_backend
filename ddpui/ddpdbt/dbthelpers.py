import json
import os
from pathlib import Path
import yaml
from ninja.errors import HttpError

from ddpui.ddpdbt.schema import DbtProjectParams
from ddpui.ddpprefect import DBTCLIPROFILE, SECRET, prefect_service
from ddpui.ddpprefect.schema import PrefectSecretBlockEdit
from ddpui.core.dbtfunctions import build_profile_dict, preprocess_airbyte_creds_for_dbt
from ddpui.core.orgdbt_manager import DbtProjectManager
from ddpui.models.org import Org, OrgWarehouse, OrgPrefectBlockv1
from ddpui.utils import secretsmanager
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddphelpers")


def create_or_update_org_cli_block(org: Org, warehouse: OrgWarehouse, airbyte_creds: dict):
    """Create/update the block in db and also in prefect"""
    profile_name = None
    target = None
    dbt_project_params: DbtProjectParams = None
    try:
        dbt_project_params = DbtProjectManager.gather_dbt_project_params(org, org.dbt)

        dbt_project_filename = str(Path(dbt_project_params.project_dir) / "dbt_project.yml")
        if not os.path.exists(dbt_project_filename):
            raise HttpError(400, dbt_project_filename + " is missing")

        with open(dbt_project_filename, "r", encoding="utf-8") as dbt_project_file:
            dbt_project = yaml.safe_load(dbt_project_file)
            if "profile" not in dbt_project:
                raise HttpError(400, "could not find 'profile:' in dbt_project.yml")

        profile_name = dbt_project["profile"]
        target = dbt_project_params.target
    except Exception as err:
        logger.error(
            "Failed to fetch the dbt profile - looks like transformation has not been setup. Using 'default' as profile name and continuing"
        )
        logger.error(err)

    dbt_creds, wh_extras = preprocess_airbyte_creds_for_dbt(
        warehouse, airbyte_creds, dbt_project_params
    )
    # CLI-block writer takes location / priority as separate kwargs (Prefect's
    # block API expects them that way); pull them back out from wh_extras.
    bqlocation = wh_extras.get("location")
    priority = wh_extras.get("priority")

    orgdbt = org.dbt

    if not orgdbt:
        logger.error("Org %s does not have a dbt workspace setup", org.slug)
        return (None, None), "dbt workspace not setup for the org"

    # set defaults to target and profile
    # cant create a cli profile without these two
    # idea is these should be updated when we setup transformation or update the warehouse
    if not profile_name:
        profile_name = "default"

    if not target:
        target = "default"

    logger.info("Found org=%s profile_name=%s target=%s", org.slug, profile_name, target)
    cli_profile_block: OrgPrefectBlockv1 = orgdbt.cli_profile_block
    if cli_profile_block:
        logger.info(
            f"Updating the cli profile block : {cli_profile_block.block_name} for org={org.slug} with profile={profile_name} target={target}"
        )
        try:
            prefect_service.update_dbt_cli_profile_block(
                block_name=cli_profile_block.block_name,
                wtype=warehouse.wtype,
                credentials=dbt_creds,
                bqlocation=bqlocation,
                profilename=profile_name,
                target=target,
                priority=priority,
            )
        except Exception as error:
            logger.error(
                "Failed to update the cli profile block %s , err=%s",
                cli_profile_block.block_name,
                str(error),
            )
            return (None, None), "Failed to update the cli profile block"
        logger.info(f"Successfully updated the cli profile block : {cli_profile_block.block_name}")
    else:
        logger.info(
            "Creating a new cli profile block for %s with profile=%s & target=%s ",
            org.slug,
            profile_name,
            target,
        )
        new_block_name = f"{org.slug}-{profile_name}"

        try:
            cli_block_response = prefect_service.create_dbt_cli_profile_block(
                block_name=new_block_name,
                profilename=profile_name,
                target=target,
                wtype=warehouse.wtype,
                bqlocation=bqlocation,
                credentials=dbt_creds,
                priority=priority,
            )
        except Exception as error:
            logger.error(
                "Failed to create a new cli profile block %s , err=%s",
                new_block_name,
                str(error),
            )
            return (None, None), "Failed to create the cli profile block"

        # save the cli profile block in django db
        cli_profile_block, created = OrgPrefectBlockv1.objects.get_or_create(
            org=org,
            block_type=DBTCLIPROFILE,
            block_id=cli_block_response["block_id"],
            block_name=cli_block_response["block_name"],
        )

        if created:
            logger.info(
                f"Successfully created the cli profile block : {cli_profile_block.block_name}"
            )
            orgdbt.cli_profile_block = cli_profile_block
            orgdbt.save()

    # Mirror creds to the runner-flow Secret block. Non-fatal on failure so
    # the CLI-profile-block path (authoritative until cutover) still wins.
    create_or_update_dbt_profile_secret_blk(org, warehouse, dbt_creds, wh_extras)

    return (cli_profile_block, dbt_project_params), None


def create_or_update_dbt_profile_secret_blk(
    org: Org,
    warehouse: OrgWarehouse,
    dbt_creds: dict,
    profile_extras: dict,
):
    """Upsert the org's dbt-profile Prefect Secret block — the runner-flow
    artifact read by proxy/prefect_flows_runner.py at flow-run start.

    Block name is deterministic: `dbt-profile-<org.slug>`.
    Value is JSON-encoded:
      {
        "wtype":          warehouse.wtype,
        "default_schema": org.dbt.default_schema,
        "creds":          dbt_creds,
        "extras":         profile_extras,
      }
    - dbt_creds:       airbyte destination fields mapped to dbt-postgres /
                       dbt-bigquery field names (via
                       preprocess_airbyte_creds_for_dbt).
    - profile_extras:  profile-shaping settings that aren't credentials:
                       bigquery → {"location": ..., "priority": ...}
                       postgres → {}

    Also upserts an OrgPrefectBlockv1 row and sets
    `org.dbt.dbt_profile_secret_block`. Requires `org.dbt` to be set; the block
    is a dbt-lifecycle artifact.

    Errors are logged and swallowed — the caller's CLI-profile-block path stays
    authoritative during the transition.

    Returns the OrgPrefectBlockv1 row on success, None on failure / no orgdbt.
    """
    if not org.dbt:
        logger.info("Skipping dbt-profile secret block for org=%s — no OrgDbt", org.slug)
        return None

    dbt_profile_secret_block_name = f"dbt-profile-{org.slug}"
    block_value = {
        "wtype": warehouse.wtype,
        "default_schema": org.dbt.default_schema,
        "creds": dbt_creds,
        "extras": profile_extras,
    }
    try:
        dbt_profile_secret_response = prefect_service.upsert_secret_block(
            PrefectSecretBlockEdit(
                block_name=dbt_profile_secret_block_name,
                secret=json.dumps(block_value),
            )
        )
    except Exception as error:  # pylint: disable=broad-exception-caught
        logger.error(
            "Failed to upsert dbt-profile secret block %s for org=%s , err=%s",
            dbt_profile_secret_block_name,
            org.slug,
            str(error),
        )
        return None

    dbt_profile_secret_block_row, _ = OrgPrefectBlockv1.objects.update_or_create(
        block_name=dbt_profile_secret_response["block_name"],
        defaults={
            "org": org,
            "block_type": SECRET,
            "block_id": dbt_profile_secret_response["block_id"],
        },
    )
    org.dbt.dbt_profile_secret_block = dbt_profile_secret_block_row
    org.dbt.save(update_fields=["dbt_profile_secret_block"])
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

    dbt_creds, wh_extras = preprocess_airbyte_creds_for_dbt(
        warehouse, airbyte_creds, dbt_project_params
    )

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

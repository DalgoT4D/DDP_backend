import json
import os
from pathlib import Path
import yaml
from ninja.errors import HttpError

from ddpui.ddpdbt.schema import DbtProjectParams
from ddpui.ddpprefect import DBTCLIPROFILE, SECRET, prefect_service
from ddpui.ddpprefect.schema import PrefectSecretBlockEdit
from ddpui.core.dbtfunctions import map_airbyte_destination_spec_to_dbtcli_profile
from ddpui.core.orgdbt_manager import DbtProjectManager
from ddpui.models.org import Org, OrgWarehouse, OrgPrefectBlockv1
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddphelpers")


def create_or_update_org_cli_block(org: Org, warehouse: OrgWarehouse, airbyte_creds: dict):
    """Create/update the block in db and also in prefect"""
    bqlocation = None
    priority = None  # whether to run in "batch" mode or "interactive" mode for bigquery
    if warehouse.wtype == "bigquery":
        if "dataset_location" in airbyte_creds:
            bqlocation = airbyte_creds["dataset_location"]
            del airbyte_creds["dataset_location"]

        if "transformation_priority" in airbyte_creds:
            priority = airbyte_creds["transformation_priority"]
            del airbyte_creds["transformation_priority"]

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

    dbt_creds = map_airbyte_destination_spec_to_dbtcli_profile(airbyte_creds, dbt_project_params)

    dbt_creds.pop("ssl_mode", None)
    dbt_creds.pop("ssl", None)

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
    create_or_update_wh_secret_block(
        org, warehouse, dbt_creds, bqlocation=bqlocation, priority=priority
    )

    return (cli_profile_block, dbt_project_params), None


def create_or_update_wh_secret_block(
    org: Org,
    warehouse: OrgWarehouse,
    dbt_creds: dict,
    bqlocation: str = None,
    priority: str = None,
):
    """Upsert the warehouse's Prefect Secret block — the runner-flow artifact
    read by proxy/prefect_flows_runner.py at flow-run start.

    Block name is deterministic: `dalgo-wh-<org.slug>`.
    Value is JSON-encoded {"creds": ..., "extras": ...}:
      creds  = airbyte destination fields the runner uses to shape profiles.yml
               (host, port, user, password, database, SSL bits; or keyfile_json
               for BigQuery).
      extras = profile-shaping settings that aren't credentials:
                 bigquery → {"location": bqlocation, "priority": priority}
                 postgres → {} (all settings live in creds)

    Also upserts an OrgPrefectBlockv1 row and sets `warehouse.secret_block`.
    Errors are logged and swallowed — the caller's CLI-profile-block path stays
    authoritative during the transition.

    Returns the OrgPrefectBlockv1 row on success, None on failure.
    """
    wh_extras = {}
    if warehouse.wtype == "bigquery":
        if bqlocation:
            wh_extras["location"] = bqlocation
        if priority:
            wh_extras["priority"] = priority

    wh_secret_block_name = f"dalgo-wh-{org.slug}"
    try:
        wh_secret_response = prefect_service.upsert_secret_block(
            PrefectSecretBlockEdit(
                block_name=wh_secret_block_name,
                secret=json.dumps({"creds": dbt_creds, "extras": wh_extras}),
            )
        )
    except Exception as error:  # pylint: disable=broad-exception-caught
        logger.error(
            "Failed to upsert warehouse secret block %s for org=%s , err=%s",
            wh_secret_block_name,
            org.slug,
            str(error),
        )
        return None

    wh_block_row, _ = OrgPrefectBlockv1.objects.update_or_create(
        block_name=wh_secret_response["block_name"],
        defaults={
            "org": org,
            "block_type": SECRET,
            "block_id": wh_secret_response["block_id"],
        },
    )
    warehouse.secret_block = wh_block_row
    warehouse.save(update_fields=["secret_block"])
    logger.info(
        "Upserted warehouse secret block %s for org=%s",
        wh_secret_response["block_name"],
        org.slug,
    )
    return wh_block_row

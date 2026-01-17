"""functions to set up elementary"""

import os
from pathlib import Path
import subprocess
from uuid import uuid4
from datetime import datetime
import yaml
import boto3
import boto3.exceptions
from ninja.errors import HttpError
from django.utils import timezone as djantotimezone

from ddpui import settings
from ddpui.models.org import Org, OrgPrefectBlockv1
from ddpui.models.org_user import OrgUser
from ddpui.models.tasks import OrgDataFlowv1

from ddpui.models.tasks import OrgTask, DataflowOrgTask
from ddpui.models.tasks import TaskProgressHashPrefix
from ddpui.models.flow_runs import PrefectFlowRun
from ddpui.utils.taskprogress import TaskProgress
from ddpui.utils.constants import TASK_GENERATE_EDR
from ddpui.core.pipelinefunctions import setup_edr_send_report_task_config
from ddpui.ddpdbt.schema import DbtProjectParams
from ddpui.core.orgdbt_manager import DbtProjectManager
from ddpui.ddpprefect import prefect_service
from ddpui.utils.helpers import generate_hash_id, compare_semver
from ddpui.ddpprefect.schema import (
    PrefectDataFlowCreateSchema3,
)
from ddpui.ddpprefect import DBTCLIPROFILE, TRANSFORM_TASK_QUEUE
from ddpui.utils.timezone import as_ist
from ddpui.utils.redis_client import RedisClient
from ddpui.utils.custom_logger import CustomLogger
from ddpui.celeryworkers.tasks import run_dbt_commands

logger = CustomLogger("ddpui")


def make_edr_report_s3_path(org: Org):
    """make s3 path for elementary report"""
    todays_date = datetime.today().strftime("%Y-%m-%d")
    bucket_file_path = f"reports/{org.slug}.{todays_date}.html"
    return bucket_file_path


def elementary_setup_status(org: Org) -> dict:
    """returns if elementary setup is complete"""
    if org.dbt is None:
        return {"error": "dbt is not configured for this client"}

    project_dir = Path(DbtProjectManager.get_dbt_project_dir(org.dbt))

    if not os.path.exists(project_dir / "elementary_profiles/profiles.yml"):
        return {"status": "not-set-up"}

    orgtask = OrgTask.objects.filter(org=org, task__slug=TASK_GENERATE_EDR).first()
    if orgtask:
        dataflow_orgtask = DataflowOrgTask.objects.filter(orgtask=orgtask).first()
        if dataflow_orgtask and dataflow_orgtask.dataflow:
            logger.info(f"Generate edr deployment found for org {org.slug}")
            return {"status": "set-up"}

    return {"status": "not-set-up"}


def get_elementary_target_schema(dbt_project_yml: str) -> dict | None:
    """{'schema': 'elementary'} or {'+schema': 'elementary'}"""
    with open(dbt_project_yml, "r", encoding="utf-8") as dbt_project_yml_f:  # skipcq: PTC-W6004
        dbt_project_obj = yaml.safe_load(dbt_project_yml_f)
        if "elementary" not in dbt_project_obj["models"]:
            return None
        if "schema" in dbt_project_obj["models"]["elementary"]:
            return {"schema": dbt_project_obj["models"]["elementary"]["schema"]}
        if "+schema" in dbt_project_obj["models"]["elementary"]:
            return {"+schema": dbt_project_obj["models"]["elementary"]["+schema"]}
        return None


def get_elementary_package_version(packages_yml: str) -> dict | None:
    """{'package': 'elementary-data/elementary', 'version': '0.15.2'}"""
    with open(packages_yml, "r", encoding="utf-8") as packages_yml_f:  # skipcq: PTC-W6004
        packages_obj = yaml.safe_load(packages_yml_f)
        if (
            packages_obj is not None
            and "packages" in packages_obj
            and isinstance(packages_obj["packages"], list)
        ):
            for package in packages_obj["packages"]:
                if package["package"] == "elementary-data/elementary":
                    return package
    return None


def check_dbt_files(org: Org):
    """checks for the existence of the required lines in dbt_project.yml and packages.yml"""
    if org.dbt is None:
        return "dbt is not configured for this client"

    dbt_project_params = DbtProjectManager.gather_dbt_project_params(org, org.dbt)

    dbt_project_yml = Path(dbt_project_params.project_dir) / "dbt_project.yml"
    packages_yml = Path(dbt_project_params.project_dir) / "packages.yml"

    if not dbt_project_yml.exists():
        return str(dbt_project_yml) if settings.DEBUG else "dbt_project.yml not found", None

    if not packages_yml.exists():
        return str(packages_yml) if settings.DEBUG else "packages.yml not found", None

    elementary_package = get_elementary_package_version(packages_yml)
    elementary_target_schema = get_elementary_target_schema(dbt_project_yml)
    latest_elementary_package_version = os.getenv("LATEST_ELEMENTARY_PACKAGE_VERSION", "0.16.1")

    retval = {"exists": {}, "missing": {}}

    if elementary_package is not None:
        retval["exists"]["elementary_package"] = elementary_package
        if (
            latest_elementary_package_version
            and compare_semver(elementary_package["version"], latest_elementary_package_version) < 0
        ):
            retval["exists"]["elementary_package"][
                "needs_upgrade"
            ] = latest_elementary_package_version
    else:
        retval["missing"][
            "elementary_package"
        ] = f"""
            # Add this to packages.yml file
            packages:
            - package: elementary-data/elementary
                version: {latest_elementary_package_version}
                ## Docs: https://docs.elementary-data.com
        """

    if elementary_target_schema is not None:
        retval["exists"]["elementary_target_schema"] = elementary_target_schema
    else:
        retval["missing"][
            "elementary_target_schema"
        ] = """models:
        # Add this to dbt_project.yml file
        ## see docs: https://docs.elementary-data.com/
        elementary:
            ## elementary models will be created in the schema '<your_schema>_elementary'
            +schema: "elementary"
            ## To disable elementary for dev, uncomment this:
            # enabled: "{{ target.name in ['prod','analytics'] }}"

        # Required from dbt 1.8 and above for certain Elementary features
        flags:
        require_explicit_package_overrides_for_builtin_materializations: False
        source_freshness_run_project_hooks: True
        """

    # logger.info(retval)
    return None, retval


def create_elementary_tracking_tables(org: Org):
    """creates elementary tracking tables"""

    from ddpui.celeryworkers.tasks import run_dbt_commands

    if org.dbt is None:
        return {"error": "dbt is not configured for this client"}

    task_id = str(uuid4())

    hashkey = f"{TaskProgressHashPrefix.RUNDBTCMDS.value}-{org.slug}"
    taskprogress = TaskProgress(task_id, hashkey)

    taskprogress.add({"message": "Added dbt commands in queue", "status": "queued"})

    # executes clean, deps, run
    run_dbt_commands.delay(
        org.id,
        org.dbt.id,
        task_id,
        {
            # run parameters
            "options": {
                "select": "elementary",
            }
        },
    )
    return {"task_id": task_id, "hashkey": hashkey}


def extract_profile_from_generate_elementary_cli_profile(lines: list[str]):
    """skips the first few lines of the output until the profile yaml begins"""
    buffer = ""
    gather = False
    for line in lines:
        if line == "elementary:":
            gather = True
        if gather:
            buffer += line + "\n"

    if buffer == "":
        logger.error(
            "macro elementary.generate_elementary_cli_profile returned nothing\n" + "\n".join(lines)
        )
        return {"error": "macro elementary.generate_elementary_cli_profile returned nothing"}, None

    elementary_profile = yaml.safe_load(buffer)
    logger.info(elementary_profile)  # safe since there are no secrets here
    return None, elementary_profile


def create_elementary_profile(org: Org):
    """creates elementary's dbt profile"""
    if org.dbt is None:
        return {"error": "dbt is not configured for this client"}

    dbt_project_params = DbtProjectManager.gather_dbt_project_params(org, org.dbt)

    # read profiles.yml is created either from disk or from the prefect block
    dbt_profile_file = Path(dbt_project_params.project_dir) / "profiles/profiles.yml"
    dbt_profile = {}
    if not os.path.exists(dbt_profile_file):
        # fetch from the cli profile block in prefect
        logger.info("fetching dbt profile from prefect block")
        dbt_cli_profile: OrgPrefectBlockv1 = org.dbt.cli_profile_block if org.dbt else None
        if dbt_cli_profile is None:
            raise HttpError(400, f"{dbt_profile_file} is missing")

        dbt_profile = prefect_service.get_dbt_cli_profile_block(dbt_cli_profile.block_name)[
            "profile"
        ]

        # write the dbt profile to disk also since elementary cli will reference it while generating its profile
        profile_dirname = Path(dbt_project_params.project_dir) / "profiles"
        os.makedirs(profile_dirname, exist_ok=True)
        profile_filename = profile_dirname / "profiles.yml"
        logger.info("writing dbt profile to " + str(profile_filename))
        with open(profile_filename, "w", encoding="utf-8") as f:
            yaml.safe_dump(dbt_profile, f)
        logger.info("wrote dbt profile to %s", profile_filename)
    else:
        with open(dbt_profile_file, "r", encoding="utf-8") as dbt_profile_file_f:
            dbt_profile = yaml.safe_load(dbt_profile_file_f)
            logger.info("read dbt profile from %s", dbt_profile_file)

    # now we have to fix up the auth section by copying the dbt profile's auth section
    r = subprocess.check_output(
        [
            dbt_project_params.dbt_binary,
            "run-operation",
            "elementary.generate_elementary_cli_profile",
            f"--profiles-dir={Path(dbt_project_params.project_dir) / 'profiles'}",
        ],
        cwd=dbt_project_params.project_dir,
        text=True,
    )

    error, elementary_profile = extract_profile_from_generate_elementary_cli_profile(r.split("\n"))
    if error:
        return error

    # get the profile from dbt_project.yaml
    dbt_project_filename = str(Path(dbt_project_params.project_dir) / "dbt_project.yml")
    if not os.path.exists(dbt_project_filename):
        raise HttpError(400, dbt_project_filename + " is missing")

    with open(dbt_project_filename, "r", encoding="utf-8") as dbt_project_file:
        dbt_project = yaml.safe_load(dbt_project_file)
        if "profile" not in dbt_project:
            raise HttpError(400, "could not find 'profile:' in dbt_project.yml")

    dbt_profile_name = dbt_project["profile"]
    dbt_profiles_target = dbt_project_params.target

    target = elementary_profile["elementary"].get("target", "default")
    if elementary_profile["elementary"]["outputs"][target]["type"] == "bigquery":
        elementary_profile["elementary"]["outputs"][target]["schema"] = elementary_profile[
            "elementary"
        ]["outputs"][target]["dataset"]
    elementary_schema = elementary_profile["elementary"]["outputs"][target]["schema"]
    elementary_profile["elementary"]["outputs"][target] = (
        dbt_profile.get(dbt_profile_name, {}).get("outputs", {}).get(dbt_profiles_target, {})
    )

    # set schema to what the elementary.generate_elementary_cli_profile macro computed
    elementary_profile["elementary"]["outputs"][target]["schema"] = elementary_schema

    elementary_profile_dir = Path(dbt_project_params.project_dir) / "elementary_profiles"

    if not elementary_profile_dir.exists():
        elementary_profile_dir.mkdir()

    elementary_profile_file = elementary_profile_dir / "profiles.yml"
    with open(elementary_profile_file, "w", encoding="utf-8") as elementary_profile_file_f:
        yaml.dump(elementary_profile, elementary_profile_file_f)

    logger.info("wrote elementary profile to %s", elementary_profile_file)

    return {"status": "success"}


def fetch_elementary_report(org: Org):
    """fetch previously generated elementary report"""
    if org.dbt is None:
        return "dbt is not configured for this client", None

    project_dir = Path(DbtProjectManager.get_dbt_project_dir(org.dbt))

    if not os.path.exists(project_dir / "elementary_profiles"):
        return "set up elementary profile first", None

    s3 = boto3.client(
        "s3",
        "ap-south-1",
        aws_access_key_id=os.getenv("ELEMENTARY_AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("ELEMENTARY_AWS_SECRET_ACCESS_KEY"),
    )
    bucket_file_path = make_edr_report_s3_path(org)
    try:
        s3response = s3.get_object(
            Bucket=os.getenv("ELEMENTARY_S3_BUCKET"),
            Key=bucket_file_path,
        )
        logger.info("fetched s3response")
    except boto3.exceptions.botocore.exceptions.ClientError:
        return "report has not been generated", None
    except Exception:
        return "error fetching elementary report", None

    report_html = s3response["Body"].read().decode("utf-8")
    htmlfilename = str(project_dir / "elementary-report.html")
    with open(htmlfilename, "w", encoding="utf-8") as indexfile:
        indexfile.write(report_html)
        indexfile.close()
    logger.info("wrote elementary report to %s", htmlfilename)

    redis = RedisClient.get_instance()
    token = uuid4()
    redis_key = f"elementary-report-{token.hex}"
    redis.set(redis_key, htmlfilename.encode("utf-8"), 600)
    logger.info("created redis key %s", redis_key)

    return None, {
        "token": token.hex,
        "created_on_utc": s3response["LastModified"].isoformat(),  # e.g. 2024-06-07T00:44:08+00:00
        "created_on_ist": as_ist(
            s3response["LastModified"]
        ).isoformat(),  # e.g. 2024-06-07T06:14:08+05:30
    }


def refresh_elementary_report_via_prefect(orguser: OrgUser) -> dict:
    """refreshes the elementary report for the current date using the prefect deployment"""
    org: Org = orguser.org
    orgtask = OrgTask.objects.filter(org=org, task__slug=TASK_GENERATE_EDR).first()
    if orgtask is None:
        return {"error": "orgtask generate-edr not found for " + org.slug}
    datafloworgtask = DataflowOrgTask.objects.filter(orgtask=orgtask).first()
    if datafloworgtask is None:
        return {"error": "datafloworgtask not found for " + org.slug}
    odf = datafloworgtask.dataflow

    if odf is None:
        return {"error": "pipeline not found"}

    locks = prefect_service.lock_tasks_for_deployment(odf.deployment_id, orguser)

    try:
        res = prefect_service.create_deployment_flow_run(odf.deployment_id)
        for tasklock in locks:
            tasklock.flow_run_id = res["flow_run_id"]
            tasklock.save()
        PrefectFlowRun.objects.create(
            deployment_id=odf.deployment_id,
            flow_run_id=res["flow_run_id"],
            name=res.get("name", ""),
            start_time=None,
            expected_start_time=djantotimezone.now(),
            total_run_time=-1,
            status="Scheduled",
            state_name="Scheduled",
            retries=0,
            orguser=orguser,
        )

    except Exception as error:
        for task_lock in locks:
            logger.info("deleting TaskLock %s", task_lock.orgtask.task.slug)
            task_lock.delete()
        logger.exception(error)
        raise HttpError(400, "failed to start a run") from error

    return res


def get_dbt_version(org: Org):
    """get dbt version"""
    try:
        dbt_project_params = DbtProjectManager.gather_dbt_project_params(org, org.dbt)
        dbt_version_command = [str(dbt_project_params.dbt_binary), "--version"]
        dbt_output = subprocess.check_output(dbt_version_command, text=True)
        for line in dbt_output.splitlines():
            if "installed:" in line:
                return line.split(":")[1].strip()
        return "Not available"
    except Exception as err:
        logger.error("Error getting dbt version: %s", err)
        return "Not available"


def get_edr_version(org: Org):
    """get elementary report version"""
    try:
        dbt_project_params = DbtProjectManager.gather_dbt_project_params(org, org.dbt)
        elementary_version_command = [
            os.path.join(dbt_project_params.venv_binary, "edr"),
            "--version",
        ]
        elementary_output = subprocess.check_output(elementary_version_command, text=True)
        for line in elementary_output.splitlines():
            if line.startswith("Elementary version"):
                return line.split()[-1].strip()[:-1]
        return "Not available"
    except Exception as err:
        logger.error("Error getting elementary version: %s", err)
        return "Not available"


def create_edr_sendreport_dataflow(org: Org, org_task: OrgTask, cron: str):
    """create the DataflowOrgTask for the orgtask"""
    dbt_project_params: DbtProjectParams = None
    try:
        dbt_project_params = DbtProjectManager.gather_dbt_project_params(org, org.dbt)
    except Exception as error:
        logger.error(error)
        return None

    if org_task.task.slug != TASK_GENERATE_EDR:
        return {"error": "This is not TASK_GENERATE_EDR task"}

    datafloworgtask = DataflowOrgTask.objects.filter(orgtask=org_task).first()
    if datafloworgtask is not None:
        return {"error": "datafloworgtask already exists for for " + org.slug}

    task_config = setup_edr_send_report_task_config(
        org_task, dbt_project_params.project_dir, dbt_project_params.venv_binary
    )

    hash_code = generate_hash_id(8)
    deployment_name = f"pipeline-{org_task.org.slug}-{org_task.task.slug}-{hash_code}"
    logger.info(f"creating deployment {deployment_name}")

    dataflow = prefect_service.create_dataflow_v1(
        PrefectDataFlowCreateSchema3(
            deployment_name=deployment_name,
            flow_name=deployment_name,
            orgslug=org_task.org.slug,
            deployment_params={
                "config": {
                    "tasks": [task_config.to_json()],
                    "org_slug": org_task.org.slug,
                }
            },
            cron=cron,
        ),
        org.get_queue_config()[TRANSFORM_TASK_QUEUE],
    )

    logger.info(
        f"creating OrgDataFlowv1 named {dataflow['deployment']['name']} with deployment_id {dataflow['deployment']['id']}"
    )
    orgdataflow = OrgDataFlowv1.objects.create(
        org=org,
        name=dataflow["deployment"]["name"],
        deployment_name=dataflow["deployment"]["name"],
        deployment_id=dataflow["deployment"]["id"],
        dataflow_type="manual",  # we dont want it to show in flows/pipelines page
        cron=cron,
    )
    DataflowOrgTask.objects.create(dataflow=orgdataflow, orgtask=org_task)
    return {"status": "success", "dataflow": orgdataflow.name}

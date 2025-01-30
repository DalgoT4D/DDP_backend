""" functions to set up elementary """

import os
from pathlib import Path
import subprocess
from uuid import uuid4
from datetime import datetime
import yaml
import boto3
import boto3.exceptions
from ninja.errors import HttpError
from ddpui import settings
from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser
from ddpui.models.tasks import OrgDataFlowv1

from ddpui.models.tasks import OrgTask
from ddpui.models.tasks import TaskProgressHashPrefix
from ddpui.utils.taskprogress import TaskProgress
from ddpui.celeryworkers.tasks import run_dbt_commands
from ddpui.core.pipelinefunctions import setup_edr_send_report_task_config
from ddpui.ddpdbt.schema import DbtProjectParams
from ddpui.core.orgdbt_manager import DbtProjectManager
from ddpui.ddpprefect import prefect_service
from ddpui.utils.helpers import generate_hash_id
from ddpui.ddpprefect.schema import (
    PrefectDataFlowCreateSchema3,
)
from ddpui.ddpprefect import MANUL_DBT_WORK_QUEUE
from ddpui.utils.timezone import as_ist
from ddpui.utils.redis_client import RedisClient
from ddpui.utils.custom_logger import CustomLogger

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

    if not os.path.exists(project_dir / "elementary_profiles"):
        return {"status": "not-set-up"}

    return {"status": "set-up"}


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

    def get_elementary_target_schema(dbt_project_yml: str):
        """{'schema': 'elementary'} or {'+schema': 'elementary'}"""
        with open(dbt_project_yml, "r", encoding="utf-8") as dbt_project_yml_f:
            dbt_project_obj = yaml.safe_load(dbt_project_yml_f)
            if "elementary" not in dbt_project_obj["models"]:
                return None
            if "schema" in dbt_project_obj["models"]["elementary"]:
                return {"schema": dbt_project_obj["models"]["elementary"]["schema"]}
            if "+schema" in dbt_project_obj["models"]["elementary"]:
                return {"+schema": dbt_project_obj["models"]["elementary"]["+schema"]}
            return None

    def get_elementary_package_version(packages_yml: str):
        """{'package': 'elementary-data/elementary', 'version': '0.15.2'}"""
        with open(packages_yml, "r", encoding="utf-8") as packages_yml_f:
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

    elementary_package = get_elementary_package_version(packages_yml)
    elementary_target_schema = get_elementary_target_schema(dbt_project_yml)

    retval = {"exists": {}, "missing": {}}

    if elementary_package is not None:
        retval["exists"]["elementary_package"] = elementary_package
    else:
        retval["missing"][
            "elementary_package"
        ] = """packages:
  - package: elementary-data/elementary
    version: 0.16.1
    ## Docs: https://docs.elementary-data.com
"""

    if elementary_target_schema is not None:
        retval["exists"]["elementary_target_schema"] = elementary_target_schema
    else:
        retval["missing"][
            "elementary_target_schema"
        ] = """models:
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
    if org.dbt is None:
        return {"error": "dbt is not configured for this client"}

    task_id = str(uuid4())

    taskprogress = TaskProgress(task_id, f"{TaskProgressHashPrefix.RUNDBTCMDS}-{org.slug}")

    taskprogress.add({"message": "Added dbt commands in queue", "status": "queued"})

    # executes clean, deps, run
    run_dbt_commands.delay(
        org.id,
        task_id,
        {
            # run parameters
            "options": {
                "select": "elementary",
            }
        },
    )
    return {"task_id": task_id}


def create_elementary_profile(org: Org):
    """creates elementary's dbt profile"""
    if org.dbt is None:
        return {"error": "dbt is not configured for this client"}

    dbt_project_params = DbtProjectManager.gather_dbt_project_params(org, org.dbt)

    r = subprocess.check_output(
        [
            dbt_project_params.dbt_binary,
            "run-operation",
            "elementary.generate_elementary_cli_profile",
            "--profiles-dir=profiles",
        ],
        cwd=dbt_project_params.project_dir,
        text=True,
    )

    buffer = ""
    gather = False
    for line in r.split("\n"):
        if line == "elementary:":
            gather = True
        if gather:
            buffer += line + "\n"

    if buffer == "":
        logger.error("macro elementary.generate_elementary_cli_profile returned nothing\n" + r)
        return {"error": "macro elementary.generate_elementary_cli_profile returned nothing"}

    elementary_profile = yaml.safe_load(buffer)
    logger.info(elementary_profile)

    # now we have to fix up the auth section by copying the dbt profile's auth section
    dbt_profile_file = Path(dbt_project_params.project_dir) / "profiles/profiles.yml"
    with open(dbt_profile_file, "r", encoding="utf-8") as dbt_profile_file_f:
        dbt_profile = yaml.safe_load(dbt_profile_file_f)
        logger.info("read dbt profile from %s", dbt_profile_file)

    target = elementary_profile["elementary"].get("target", "default")
    elementary_profile["elementary"]["outputs"][target] = dbt_profile[target]["outputs"][target]

    # set schema to elementary
    elementary_profile["elementary"]["outputs"][target]["schema"] = "elementary"

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
    odf = OrgDataFlowv1.objects.filter(
        org=org, name__startswith=f"pipeline-{org.slug}-generate-edr"
    ).first()

    if odf is None:
        return {"error": "pipeline not found"}

    locks = prefect_service.lock_tasks_for_deployment(odf.deployment_id, orguser)

    try:
        res = prefect_service.create_deployment_flow_run(odf.deployment_id)
        for tasklock in locks:
            tasklock.flow_run_id = res["flow_run_id"]
            tasklock.save()

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

    if (
        OrgDataFlowv1.objects.filter(
            name__startswith=f"pipeline-{org_task.org.slug}-{org_task.task.slug}-"
        ).count()
        > 0
    ):
        return {"error": "An edr pipeline for this org already exists"}

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
        MANUL_DBT_WORK_QUEUE,
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
    return {"status": "success", "dataflow": orgdataflow.name}

from dotenv import load_dotenv
from django.core.management.base import BaseCommand

from ddpui.utils.custom_logger import CustomLogger
from ddpui.models.org import OrgDataFlowv1
from ddpui.utils.constants import TRANSFORM_TASKS_SEQ
from ddpui.ddpprefect import prefect_service
from ddpui.core.pipelinefunctions import fix_transform_tasks_seq_dataflow

logger = CustomLogger("ddpui")

load_dotenv()


class Command(BaseCommand):
    """
    Make sures the seq of transform tasks in a pipeline is correct
    """

    help = "Make sures the seq of transform tasks in a pipeline is correct"

    def add_arguments(self, parser):  # skipcq: PYL-R0201
        parser.add_argument(
            "--org",
            type=str,
            help="Org slug: use 'all' to run for all orgs at once",
            required=False,
        )
        parser.add_argument(
            "--deployment_id",
            type=str,
            help="deployment id of the pipeline",
            required=False,
        )
        parser.add_argument(
            "--fix",
            action="store_true",
            help="Fix the seq of transform tasks",
            required=False,
        )

    def handle(self, *args, **options):
        """
        1. Fetch all the dataflows to be checked based on the arguments passed
        2. For each dataflow, fetch deployment params and check the seq of transform tasks
        3. If the seq is incorrect, push it to an array
        4. Print/log all the incorrect deployment & their org slugs
        5. If argument to fix is passed, fix the seq of transform tasks
        """

        org_slug = options["org"]
        deployment_id = options["deployment_id"]
        query = OrgDataFlowv1.objects

        if org_slug:
            query = query.filter(org__slug=org_slug)

        if deployment_id:
            query = query.filter(deployment_id=deployment_id)

        deployments_to_fix = []

        for dataflow in query.all():
            try:
                deployment = prefect_service.get_deployment(dataflow.deployment_id)
            except Exception as e:
                logger.error(
                    f"Error getting deployment for {dataflow.deployment_id} | {dataflow.org.slug}: {str(e)}"
                )
                continue

            params = deployment.get("parameters", {})
            config = params.get("config", {})
            tasks = config.get("tasks", [])
            transform_tasks = [
                task for task in tasks if task["slug"] in TRANSFORM_TASKS_SEQ
            ]

            if len(transform_tasks) <= 1:
                logger.info(
                    f"Skipping, found only {len(tasks)} transform tasks for {dataflow.deployment_id} | {dataflow.org.slug}"
                )
                continue

            is_seq_correct = True  # assume its correctly sorted in ascending order
            transform_task_slugs = [
                task["slug"] for task in sorted(transform_tasks, key=lambda x: x["seq"])
            ]
            order = [TRANSFORM_TASKS_SEQ[slug] for slug in transform_task_slugs]
            for i in range(1, len(order)):
                if order[i] < order[i - 1]:
                    is_seq_correct = False
                    break

            if not is_seq_correct:
                deployments_to_fix.append(dataflow.deployment_id)

        logger.info(f"Found {len(deployments_to_fix)} deployments to fix")
        logger.info(deployments_to_fix)

        if options["fix"] and len(deployments_to_fix) > 0:
            for deployment_id in deployments_to_fix:
                fix_transform_tasks_seq_dataflow(deployment_id)

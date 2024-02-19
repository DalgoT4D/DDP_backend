import uuid
from dotenv import load_dotenv
from django.core.management.base import BaseCommand

from ddpui.models.org_user import Org
from ddpui.models.org import OrgDataFlowv1
from ddpui.models.tasks import OrgTask, DataflowOrgTask
from ddpui.ddpprefect import prefect_service
from ddpui.ddpprefect.schema import PrefectDataFlowUpdateSchema3
import logging

load_dotenv()

logger = logging.getLogger("migration")


class Command(BaseCommand):
    """
    This script deletes an org and all associated entities
    Not only in the Django database, but also in Airbyte and in Prefect
    """

    help = "Deletes an organization"

    def add_arguments(self, parser):
        parser.add_argument(
            "--slug",
            required=False,
            default="all",
            help="By default it will run for all organization. To run for an org; pass the slug here",
        )

    def set_uuids_to_orgtasks(self, org: Org):
        """Sets uuids to those orgtasks that have currently a null"""
        logger.info(f"setting uuids for org {org.slug}")
        for org_task in OrgTask.objects.filter(org=org).all():
            if org_task.uuid is None:
                org_task.uuid = uuid.uuid4()
                org_task.save()
                logger.info(f"updated uuid for org_task {org_task.id}")

    def update_deployment_params_with_orgtask_uuid(self, org: Org):
        """
        Updates deployment params with orgtask uuid
        1) Fetched all dataflows for the org
        2) For each dataflow, fetched the deployment from prefect and its params
        3) Use slug to match deployment param task with orgtask
        4) Push the update task config in deployment params with the uuid of orgtask
        5) Update the deployment with updated params in prefect
        """
        logger.info(
            f"updating deployment params with uuids of orgtask for org {org.slug}"
        )
        for dataflow in OrgDataFlowv1.objects.filter(org=org).all():
            logger.info(
                f"processing dataflow with deployment_id {dataflow.deployment_id}"
            )
            # fetch deployment params
            deployment = None
            try:
                deployment = prefect_service.get_deployment(dataflow.deployment_id)
            except Exception as error:
                logger.info(
                    f"Something went wrong in fetching the deployment with id '{dataflow.deployment_id}'"
                )
                logger.exception(error)
                logger.info("skipping to next loop")
                continue

            # add uuid to each task config in params
            params = deployment["parameters"]
            if "config" in params and "tasks" in params["config"]:
                for task_config in params["config"]["tasks"]:
                    if "slug" in task_config:
                        org_task = OrgTask.objects.filter(
                            org=org,
                            task__slug=task_config["slug"],
                            parameters__in=["", "{}"],
                        )
                        if "connection_id" in task_config:
                            org_task = org_task.filter(
                                connection_id=task_config["connection_id"]
                            )

                        org_task = org_task.first()

                        if org_task is not None and org_task.uuid:
                            task_config["orgtask_uuid"] = str(org_task.uuid)
                            logger.info(
                                f"updated task config with uuid for {task_config['slug']}"
                            )
                logger.info(
                    f"updated deployment params for {dataflow.name}|{dataflow.deployment_id}"
                )
            else:
                raise Exception(
                    f"no tasks key found in deployment params for {dataflow.deployment_id}"
                )

            # update deployment with updated params
            try:
                payload = PrefectDataFlowUpdateSchema3(
                    name=dataflow.name,  # wont be updated
                    connections=[],  # wont be updated
                    dbtTransform="ignore",  # wont be updated
                    cron=dataflow.cron if dataflow.cron else "",
                    transformTasks=[],  # wont be updated
                    deployment_params=params,
                )
                prefect_service.update_dataflow_v1(dataflow.deployment_id, payload)
                logger.info(
                    f"updated deployment params for the deployment with id {dataflow.deployment_id}"
                )
            except Exception as error:
                logger.info(
                    f"Something went wrong in updating the deployment params with id '{dataflow.deployment_id}'"
                )
                logger.exception(error)
                logger.info("skipping to next loop")
                continue

            # assert if the uuid is present in each task config of each deployment
            try:
                deployment = prefect_service.get_deployment(dataflow.deployment_id)
                params = deployment["parameters"]
                if "config" in params and "tasks" in params["config"]:
                    for task_config in params["config"]["tasks"]:
                        if "orgtask_uuid" not in task_config:
                            logger.info(
                                f"uuid not found in task config {task_config['slug']} of deployment with id {dataflow.deployment_id}"
                            )

            except Exception as error:
                logger.info(
                    f"Something went wrong in fetching the deployment with id '{dataflow.deployment_id}' for assertion"
                )
                logger.exception(error)
                logger.info("skipping to next loop")
                continue

    def update_task_seq_from_deployment_to_datafloworgtask(self, org: Org):
        """
        Update seq from deployment parameters from each task config in deployment to datafloworgtask; using the orgtask_uuid embeded
        """
        logger.info(
            f"updating deployment params with uuids of orgtask for org {org.slug}"
        )
        for dataflow in OrgDataFlowv1.objects.filter(org=org).all():
            logger.info(
                f"processing dataflow with deployment_id {dataflow.deployment_id}"
            )
            # fetch deployment params
            deployment = None
            try:
                deployment = prefect_service.get_deployment(dataflow.deployment_id)
            except Exception as error:
                logger.info(
                    f"Something went wrong in fetching the deployment with id '{dataflow.deployment_id}'"
                )
                logger.exception(error)
                logger.info("skipping to next loop")
                continue

            # update seq from deployment params in our django db
            params = deployment["parameters"]
            if "config" in params and "tasks" in params["config"]:
                for task_config in params["config"]["tasks"]:
                    if "orgtask_uuid" in task_config:
                        org_task = OrgTask.objects.filter(
                            org=org, uuid=task_config["orgtask_uuid"]
                        ).first()

                        if org_task:
                            DataflowOrgTask.objects.filter(
                                orgtask=org_task, dataflow=dataflow
                            ).update(seq=task_config["seq"])
                        else:
                            logger.error(
                                f"Did not find orgtask with uuid {task_config['orgtask_uuid']} in task config of deployment for slug {task_config['slug']} in {dataflow.deployment_id}"
                            )

                    else:
                        logger.error(
                            f"Did not find orgtask_uuid in task config of deployment for slug {task_config['slug']} in {dataflow.deployment_id}"
                        )
                logger.info(
                    f"updated deployment params seq for {dataflow.name}|{dataflow.deployment_id}"
                )
            else:
                raise Exception(
                    f"no tasks key found in deployment params for {dataflow.deployment_id}"
                )

    def handle(self, *args, **options):
        """Docstring"""
        slug = options["slug"]
        query = Org.objects
        if slug != "all":
            query = query.filter(slug=slug)

        logger.info("STARTED")

        for org in query.all():
            self.set_uuids_to_orgtasks(org)
            self.update_deployment_params_with_orgtask_uuid(org)
            self.update_task_seq_from_deployment_to_datafloworgtask(org)

        logger.info("FINISHED")

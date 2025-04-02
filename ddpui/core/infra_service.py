"""
This module talks to the infra service for creation various infrastructure blocks
"""

import time
import os
from io import BytesIO

from ddpui.utils.custom_logger import CustomLogger
from ddpui.utils.http import dalgo_post, dalgo_get, dalgo_delete
from celery.states import SUCCESS, FAILURE, REVOKED, REJECTED, IGNORED

INFRA_SERVICE_API_URL = os.getenv("INFRA_SERVICE_API_URL")
INFRA_SERVICE_API_KEY = os.getenv("INFRA_SERVICE_API_KEY")
CELERY_TERMINAL_STATES = [SUCCESS, FAILURE, REVOKED, REJECTED, IGNORED]
CELERY_ERROR_STATES = [FAILURE, REVOKED, REJECTED]
headers = {"Authorization": INFRA_SERVICE_API_KEY}

logger = CustomLogger("ddpui")


def poll_llm_infra_service_task(task_id: str, poll_interval: int = 5) -> dict:
    """
    Polls the llm service task and returns the result
    """
    # poll this task
    while True:
        response = dalgo_get(f"{INFRA_SERVICE_API_URL}/api/task/{task_id}", headers=headers)
        if response["status"] in CELERY_TERMINAL_STATES:
            break
        logger.info(f"Polling : Task {task_id} is in state {response['status']}")
        time.sleep(poll_interval)

    if response["status"] in CELERY_ERROR_STATES:
        logger.error(f"Error occured while polling llm service job {str(response['error'])}")
        raise Exception(response["error"] if response["error"] else "error occured in llm service")

    return response["result"]


def create_warehouse_in_rds(dbname: str, poll_interval: int = 5) -> dict:
    """
    Creates a warehouse
    Returns the creds
    - dbname
    - host
    - port
    - user
    - password
    """

    response = dalgo_post(
        f"{INFRA_SERVICE_API_URL}/api/infra/postgres/db", headers=headers, json={"dbname": dbname}
    )

    task_id = response["task_id"]

    if not task_id:
        raise Exception("Failed to submit the task for creating superset db")

    return poll_llm_infra_service_task(task_id, poll_interval)


def create_superset_instance(poll_interval: int = 5) -> dict:
    """
    Creates a superset instance with basic auth
    Returns
    - superset_url (eg: https://superset3.dalgo.in/)
    """
    response = dalgo_post(f"{INFRA_SERVICE_API_URL}/api/infra/superset", headers=headers)

    task_id = response["task_id"]

    if not task_id:
        raise Exception("Failed to submit the task")

    return poll_llm_infra_service_task(task_id, poll_interval)

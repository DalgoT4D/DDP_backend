"""
This module talks to the llm service
"""

import time
import os
from io import BytesIO

from ddpui.utils.custom_logger import CustomLogger
from ddpui.utils.http import dalgo_post, dalgo_get, dalgo_delete
from celery.states import SUCCESS, FAILURE, REVOKED, REJECTED, IGNORED

LLM_SERVICE_API_URL = os.getenv("LLM_SERVICE_API_URL")
LLM_SERVICE_API_KEY = os.getenv("LLM_SERVICE_API_KEY")
CELERY_TERMINAL_STATES = [SUCCESS, FAILURE, REVOKED, REJECTED, IGNORED]
CELERY_ERROR_STATES = [FAILURE, REVOKED, REJECTED]
headers = {"Authorization": LLM_SERVICE_API_KEY}

logger = CustomLogger("ddpui")


def poll_llm_service_task(task_id: str, poll_interval: int = 5) -> dict:
    """
    Polls the llm service task and returns the result
    """
    # poll this task
    while True:
        response = dalgo_get(f"{LLM_SERVICE_API_URL}/api/task/{task_id}", headers=headers)
        if response["status"] in CELERY_TERMINAL_STATES:
            break
        logger.info(f"Polling : Task {task_id} is in state {response['status']}")
        time.sleep(poll_interval)

    if response["status"] in CELERY_ERROR_STATES:
        logger.error(f"Error occured while polling llm service job {str(response['error'])}")
        raise Exception(response["error"] if response["error"] else "error occured in llm service")

    return response["result"]


def upload_text_as_file(file_text: str, file_name: str) -> str:
    """
    returns the relative file_path
    """

    files = {"file": (f"{file_name}.txt", BytesIO(file_text.encode("utf-8")))}
    response = dalgo_post(
        f"{LLM_SERVICE_API_URL}/api/file/upload",
        files=files,
        headers=headers,
    )

    return (response["file_path"], response["session_id"])


def upload_json_as_file(json_string: str, file_name: str) -> str:
    """
    returns the relative file_path
    """

    files = {
        "file": (
            f"{file_name}.json",
            BytesIO(json_string.encode("utf-8")),
            "application/json",
        )
    }
    response = dalgo_post(
        f"{LLM_SERVICE_API_URL}/api/file/upload",
        files=files,
        headers=headers,
    )

    return (response["file_path"], response["session_id"])


def file_search_query_and_poll(
    assistant_prompt: str,
    queries: list[str],
    session_id: str,
    poll_interval: int = 5,
) -> dict:
    """
    Submits the user prompts to llm service and waits till the task is completed
    Returns the task result
    {
        result: [],
        session_id: "session_id"
    }
    """

    response = dalgo_post(
        f"{LLM_SERVICE_API_URL}/api/file/query",
        headers=headers,
        json={
            "assistant_prompt": assistant_prompt,
            "queries": queries,
            "session_id": session_id,
        },
    )

    task_id = response["task_id"]

    if not task_id:
        raise Exception("Failed to submit the task")

    return poll_llm_service_task(task_id, poll_interval)


def close_file_search_session(session_id: str, poll_interval: int = 5) -> None:
    """
    Closes the file search session
    """

    response = dalgo_delete(
        f"{LLM_SERVICE_API_URL}/api/file/search/session/{session_id}",
        headers=headers,
    )

    task_id = response["task_id"]

    if not task_id:
        raise Exception("Failed to submit the task")

    poll_llm_service_task(task_id, poll_interval)


def train_vanna_on_warehouse(
    training_sql: str, pg_vector_creds: dict, warehouse_creds: dict, warehouse_type: str
):
    """
    Creates embeddings in vanna based on the results of a training plan (sql)
    """
    response = dalgo_post(
        f"{LLM_SERVICE_API_URL}/api/vanna/train",
        headers=headers,
        json={
            "training_sql": training_sql,
            "warehouse_creds": warehouse_creds,
            "pg_vector_creds": pg_vector_creds,
            "warehouse_type": warehouse_type,
        },
    )

    task_id = response["task_id"]

    if not task_id:
        raise Exception("Failed to submit the task")

    return poll_llm_service_task(task_id)


def ask_vanna_for_sql(
    user_prompt: str, pg_vector_creds: dict, warehouse_creds: dict, warehouse_type: str
):
    """
    Creates embeddings in vanna based on the results of a training plan (sql)
    """
    response = dalgo_post(
        f"{LLM_SERVICE_API_URL}/api/vanna/ask",
        headers=headers,
        json={
            "user_prompt": user_prompt,
            "warehouse_creds": warehouse_creds,
            "pg_vector_creds": pg_vector_creds,
            "warehouse_type": warehouse_type,
        },
    )

    task_id = response["task_id"]

    if not task_id:
        raise Exception("Failed to submit the task")

    return poll_llm_service_task(task_id)


def check_if_rag_is_trained(
    pg_vector_creds: dict, warehouse_creds: dict, warehouse_type: str
) -> bool:
    """
    Checks if there any embeddings created
    """
    response = dalgo_post(
        f"{LLM_SERVICE_API_URL}/api/vanna/train/check",
        headers=headers,
        json={
            "warehouse_creds": warehouse_creds,
            "pg_vector_creds": pg_vector_creds,
            "warehouse_type": warehouse_type,
        },
    )

    task_id = response["task_id"]

    if not task_id:
        raise Exception("Failed to submit the task")

    result = poll_llm_service_task(task_id)

    logger.info(f"Is rag trained ? : {result}")

    return result in ["true", "True", True]

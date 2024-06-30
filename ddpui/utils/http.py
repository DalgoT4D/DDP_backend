"""
Helper functions for Dalgo to interact with APIs of different service via HTTP
"""

# TODO: go away with request calls in prefect_service and airbyte_service to use this

import requests
from ninja.errors import HttpError

from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui")


def dalgo_get(endpoint: str, **kwargs) -> dict:
    """make a GET request"""
    headers = kwargs.pop("headers", {})
    timeout = kwargs.pop("timeout", None)

    try:
        res = requests.get(
            endpoint,
            headers=headers,
            timeout=timeout,
            **kwargs,
        )
    except Exception as error:
        logger.exception(error)
        raise HttpError(500, "connection error") from error
    try:
        res.raise_for_status()
    except Exception as error:
        logger.exception(error)
        raise HttpError(res.status_code, res.text) from error
    return res.json()


def dalgo_post(endpoint: str, json: dict = None, files: dict = None, **kwargs) -> dict:
    """make a POST request"""
    headers = kwargs.pop("headers", {})
    timeout = kwargs.pop("timeout", None)

    try:
        res = requests.post(
            endpoint,
            headers=headers,
            timeout=timeout,
            json=json,
            files=files,
            **kwargs,
        )
    except Exception as error:
        logger.exception(error)
        raise HttpError(500, "connection error") from error
    try:
        res.raise_for_status()
    except Exception as error:
        logger.exception(error)
        raise HttpError(res.status_code, res.text) from error
    return res.json()


def dalgo_put(endpoint: str, json: dict, **kwargs) -> dict:
    """make a PUT request"""
    headers = kwargs.pop("headers", {})
    timeout = kwargs.pop("timeout", None)

    try:
        res = requests.put(
            endpoint,
            headers=headers,
            timeout=timeout,
            json=json,
            **kwargs,
        )
    except Exception as error:
        logger.exception(error)
        raise HttpError(500, "connection error") from error
    try:
        res.raise_for_status()
    except Exception as error:
        logger.exception(error)
        raise HttpError(res.status_code, res.text) from error
    return res.json()


def dalgo_delete(endpoint: str, **kwargs) -> None:
    """makes a DELETE request to the proxy"""
    # we send headers and timeout separately from kwargs, just to be explicit about it
    headers = kwargs.pop("headers", {})
    timeout = kwargs.pop("timeout", None)

    try:
        res = requests.delete(
            endpoint,
            headers=headers,
            timeout=timeout,
            **kwargs,
        )
    except Exception as error:
        logger.exception(error)
        raise HttpError(500, "connection error") from error
    try:
        res.raise_for_status()
    except Exception as error:
        logger.exception(error)
        raise HttpError(res.status_code, res.text) from error

    return res.json()

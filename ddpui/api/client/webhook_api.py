from ninja import NinjaAPI

# from ninja.errors import HttpError
from ddpui.utils.custom_logger import CustomLogger

prefectapi = NinjaAPI(urls_namespace="prefect")
# http://127.0.0.1:8000/api/docs


logger = CustomLogger("ddpui")


@prefectapi.post("/notification/")
def post_notification(request, payload: dict):  # pylint: disable=unused-argument
    """webhook endpoint for notifications"""
    logger.info(payload)
    return {"status": "ok"}

"""Org logo upload/delete API endpoints"""

from ninja import Router, File
from ninja.files import UploadedFile
from ninja.errors import HttpError

from ddpui.auth import has_permission
from ddpui.models.org_user import OrgUser
from ddpui.utils.custom_logger import CustomLogger
from ddpui.utils.response_wrapper import api_response, ApiResponse
from ddpui.core.org_logo.org_logo_service import OrgLogoService
from ddpui.core.org_logo.exceptions import (
    OrgLogoNotFoundError,
    OrgLogoValidationError,
    OrgLogoS3Error,
)
from ddpui.schemas.org_schema import OrgLogoResponse, OrgLogoUrlPayload

logger = CustomLogger("ddpui.org_logo_api")

org_logo_router = Router()


@org_logo_router.get("/", response=ApiResponse[OrgLogoResponse])
@has_permission(["can_view_orgusers"])
def get_org_logo(request):
    """Get the current org logo"""
    orguser: OrgUser = request.orguser

    try:
        org = OrgLogoService.get_logo(orguser.org)
        return api_response(
            success=True,
            data=OrgLogoResponse.from_model(org),
        )
    except OrgLogoNotFoundError as e:
        raise HttpError(404, str(e)) from e


@org_logo_router.post("/upload/", response=ApiResponse[OrgLogoResponse])
@has_permission(["can_edit_org_notification_settings"])
def upload_logo_file(request, file: UploadedFile = File(...)):
    """Upload an image file as the org logo"""
    orguser: OrgUser = request.orguser

    try:
        org = OrgLogoService.upload_logo_from_file(
            file_bytes=file.read(),
            content_type=file.content_type or "",
            filename=file.name or "",
            org=orguser.org,
        )
        return api_response(
            success=True,
            data=OrgLogoResponse.from_model(org),
            message="Logo uploaded successfully",
        )
    except OrgLogoValidationError as e:
        raise HttpError(400, str(e)) from e
    except OrgLogoS3Error as e:
        raise HttpError(502, str(e)) from e


@org_logo_router.post("/url/", response=ApiResponse[OrgLogoResponse])
@has_permission(["can_edit_org_notification_settings"])
def upload_logo_from_url(request, payload: OrgLogoUrlPayload):
    """Store an external image URL directly as the org logo — no S3 upload"""
    orguser: OrgUser = request.orguser

    try:
        org = OrgLogoService.upload_logo_from_url(
            image_url=payload.image_url,
            org=orguser.org,
        )
        return api_response(
            success=True,
            data=OrgLogoResponse.from_model(org),
            message="Logo URL saved successfully",
        )
    except OrgLogoValidationError as e:
        raise HttpError(400, str(e)) from e
    except Exception as e:
        logger.error(f"Failed to save logo URL for {orguser.org.slug}: {e}")
        raise HttpError(500, "Failed to save logo URL") from e


@org_logo_router.delete("/", response=ApiResponse)
@has_permission(["can_edit_org_notification_settings"])
def delete_logo(request):
    """Delete the org logo from S3 and clear the fields on Org"""
    orguser: OrgUser = request.orguser

    try:
        OrgLogoService.delete_logo(orguser.org)
        return api_response(success=True, message="Logo deleted successfully")
    except OrgLogoNotFoundError as e:
        raise HttpError(404, str(e)) from e
    except OrgLogoS3Error as e:
        raise HttpError(502, str(e)) from e

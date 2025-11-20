"""
Organization Settings API endpoints - Account Manager only access
"""

from ninja import Router, File, UploadedFile
from ninja.errors import HttpError
from django.shortcuts import get_object_or_404
from django.db import transaction, connection
from django.db.utils import ProgrammingError
from django.core.management.color import no_style
from django.db import models
from django.http import HttpResponse
from django.utils import timezone
from functools import wraps

from ddpui.auth import has_permission, CustomJwtAuthMiddleware


from ddpui.models.org_settings import (
    OrgSettings,
    OrgSettingsSchema,
    CreateOrgSettingsSchema,
    UpdateOrgSettingsSchema,
)
from ddpui.models.org_user import OrgUser
from ddpui.core.notifications_service import create_notification
from ddpui.schemas.notifications_api_schemas import NotificationDataSchema
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui")
router = Router()


def send_ai_chat_enabled_notification(org, author_email):
    """
    Send a notification to all org users when AI chat feature is enabled.

    Args:
        org: Organization object
        author_email: Email of the person who enabled the feature
    """
    try:
        # Get all users in the organization
        org_users = OrgUser.objects.filter(org=org)
        recipient_ids = list(org_users.values_list("id", flat=True))

        if not recipient_ids:
            logger.warning(
                f"No users found in organization {org.slug} to send AI enable notification"
            )
            return

        # Create notification data
        notification_data = NotificationDataSchema(
            author=author_email,
            message="AI Chat feature is enabled",
            email_subject="AI Chat Feature Enabled - Dalgo",
            urgent=False,
            recipients=recipient_ids,
        )

        # Send the notification
        error, result = create_notification(notification_data)

        if error:
            logger.error(f"Error sending AI enable notification: {error}")
        else:
            logger.info(
                f"Successfully sent AI enable notification to {len(recipient_ids)} users in org {org.slug}"
            )

    except Exception as e:
        logger.error(f"Exception while sending AI enable notification: {e}")


def ensure_org_settings_table_exists():
    """Create the org_settings table if it doesn't exist"""
    try:
        # Try a simple query to check if table exists
        OrgSettings.objects.exists()
        return True
    except ProgrammingError:
        try:
            # Create the table using Django's schema generation
            from django.db import connection
            from django.core.management.sql import sql_create_index
            from django.core.management.color import no_style

            style = no_style()

            # Get the SQL to create the table
            with connection.schema_editor() as schema_editor:
                schema_editor.create_model(OrgSettings)

            logger.info("Created org_settings table successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to create org_settings table: {e}")
            return False


@router.get("/test")
def test_org_settings_route(request):
    """Test endpoint to verify org settings routing is working"""
    return {
        "message": "Org settings API is working",
        "user_id": request.user.id if hasattr(request, "user") else None,
    }


@router.get("/", response=dict)
@has_permission(["can_manage_org_settings"])
def get_org_settings(request):
    """
    Get organization settings for the current organization.
    Only Account Managers can access this.
    """
    try:
        orguser = request.orguser
        if not orguser or not orguser.org:
            raise HttpError(400, "Organization not found")

        # Get or create org settings
        org_settings, created = OrgSettings.objects.get_or_create(
            org=orguser.org,
            defaults={
                "organization_name": orguser.org.name,
                "website": getattr(orguser.org, "website", None),
                "ai_data_sharing_enabled": False,
                "ai_logging_acknowledged": False,
            },
        )

        if created:
            logger.info(f"Created new org settings for org {orguser.org.slug}")
        else:
            logger.info(f"Found existing org settings for org {orguser.org.slug}")

        org_data = OrgSettingsSchema(
            organization_name=org_settings.organization_name,
            website=org_settings.website,
            organization_logo_filename=org_settings.organization_logo_filename,
            ai_data_sharing_enabled=org_settings.ai_data_sharing_enabled,
            ai_logging_acknowledged=org_settings.ai_logging_acknowledged,
            ai_settings_accepted_by_email=org_settings.ai_settings_accepted_by.email
            if org_settings.ai_settings_accepted_by
            else None,
            ai_settings_accepted_at=org_settings.ai_settings_accepted_at.isoformat()
            if org_settings.ai_settings_accepted_at
            else None,
        )

        return {"success": True, "res": org_data.dict()}

    except ProgrammingError as e:
        if 'relation "ddpui_org_settings" does not exist' in str(e):
            logger.info("OrgSettings table does not exist. Attempting to create it...")
            if ensure_org_settings_table_exists():
                # Table created successfully, retry the operation
                try:
                    org_settings, created = OrgSettings.objects.get_or_create(
                        org=orguser.org,
                        defaults={
                            "organization_name": orguser.org.name,
                            "website": getattr(orguser.org, "website", None),
                            "ai_data_sharing_enabled": False,
                            "ai_logging_acknowledged": False,
                        },
                    )

                    retry_org_data = OrgSettingsSchema(
                        organization_name=org_settings.organization_name,
                        website=org_settings.website,
                        organization_logo_filename=org_settings.organization_logo_filename,
                        ai_data_sharing_enabled=org_settings.ai_data_sharing_enabled,
                        ai_logging_acknowledged=org_settings.ai_logging_acknowledged,
                        ai_settings_accepted_by_email=org_settings.ai_settings_accepted_by.email
                        if org_settings.ai_settings_accepted_by
                        else None,
                        ai_settings_accepted_at=org_settings.ai_settings_accepted_at.isoformat()
                        if org_settings.ai_settings_accepted_at
                        else None,
                    )

                    return {"success": True, "res": retry_org_data.dict()}
                except Exception as retry_error:
                    logger.error(f"Error after creating table: {retry_error}")
                    raise HttpError(500, f"Failed after table creation: {str(retry_error)}")
            else:
                raise HttpError(
                    500, "Unable to create required database table. Please contact administrator."
                )
        else:
            logger.error(f"Database error retrieving org settings: {e}")
            logger.exception("Full traceback for database error:")
            raise HttpError(500, f"Database error: {str(e)}")
    except Exception as e:
        logger.error(f"Error retrieving org settings: {e}")
        logger.exception("Full traceback for org settings retrieval error:")
        raise HttpError(500, f"Failed to retrieve organization settings: {str(e)}")


@router.put("/", response=dict)
@has_permission(["can_manage_org_settings"])
@transaction.atomic
def update_org_settings(request, payload: UpdateOrgSettingsSchema):
    """
    Update organization settings.
    Only Account Managers can update settings.
    """
    try:
        orguser = request.orguser
        if not orguser or not orguser.org:
            raise HttpError(400, "Organization not found")

        # Get or create org settings
        org_settings, created = OrgSettings.objects.get_or_create(
            org=orguser.org,
            defaults={
                "organization_name": orguser.org.name,
                "website": getattr(orguser.org, "website", None),
                "ai_data_sharing_enabled": False,
                "ai_logging_acknowledged": False,
            },
        )

        # Track if any AI settings are being changed and if AI chat is being enabled
        ai_settings_changed = False
        ai_chat_being_enabled = False

        # Update only provided fields
        if payload.organization_name is not None:
            org_settings.organization_name = payload.organization_name

        if payload.website is not None:
            org_settings.website = payload.website

        if payload.ai_data_sharing_enabled is not None:
            # Check if AI chat is being enabled (changing from False to True)
            if not org_settings.ai_data_sharing_enabled and payload.ai_data_sharing_enabled:
                ai_chat_being_enabled = True

            if org_settings.ai_data_sharing_enabled != payload.ai_data_sharing_enabled:
                ai_settings_changed = True
            org_settings.ai_data_sharing_enabled = payload.ai_data_sharing_enabled

        if payload.ai_logging_acknowledged is not None:
            if org_settings.ai_logging_acknowledged != payload.ai_logging_acknowledged:
                ai_settings_changed = True
            org_settings.ai_logging_acknowledged = payload.ai_logging_acknowledged

        # Update tracking fields if AI settings changed
        if ai_settings_changed:
            org_settings.ai_settings_accepted_by = orguser.user
            org_settings.ai_settings_accepted_at = timezone.now()

        org_settings.save()

        # Send notification if AI chat feature was enabled
        if ai_chat_being_enabled:
            send_ai_chat_enabled_notification(orguser.org, orguser.user.email)
            logger.info(f"Sent AI chat enabled notification for org {orguser.org.slug}")

        logger.info(f"Updated org settings for org {orguser.org.slug} by user {orguser.user.email}")

        update_org_data = OrgSettingsSchema(
            organization_name=org_settings.organization_name,
            website=org_settings.website,
            organization_logo_filename=org_settings.organization_logo_filename,
            ai_data_sharing_enabled=org_settings.ai_data_sharing_enabled,
            ai_logging_acknowledged=org_settings.ai_logging_acknowledged,
            ai_settings_accepted_by_email=org_settings.ai_settings_accepted_by.email
            if org_settings.ai_settings_accepted_by
            else None,
            ai_settings_accepted_at=org_settings.ai_settings_accepted_at.isoformat()
            if org_settings.ai_settings_accepted_at
            else None,
        )

        return {"success": True, "res": update_org_data.dict()}

    except ProgrammingError as e:
        if 'relation "ddpui_org_settings" does not exist' in str(e):
            logger.info("OrgSettings table does not exist. Attempting to create it...")
            if ensure_org_settings_table_exists():
                # Table created successfully, retry the operation
                try:
                    # Retry the update operation
                    org_settings, created = OrgSettings.objects.get_or_create(
                        org=orguser.org,
                        defaults={
                            "organization_name": orguser.org.name,
                            "website": getattr(orguser.org, "website", None),
                            "ai_data_sharing_enabled": False,
                            "ai_logging_acknowledged": False,
                        },
                    )

                    # Apply the updates
                    ai_settings_changed = False
                    ai_chat_being_enabled = False

                    if payload.organization_name is not None:
                        org_settings.organization_name = payload.organization_name

                    if payload.website is not None:
                        org_settings.website = payload.website

                    if payload.ai_data_sharing_enabled is not None:
                        # Check if AI chat is being enabled (changing from False to True)
                        if (
                            not org_settings.ai_data_sharing_enabled
                            and payload.ai_data_sharing_enabled
                        ):
                            ai_chat_being_enabled = True

                        if org_settings.ai_data_sharing_enabled != payload.ai_data_sharing_enabled:
                            ai_settings_changed = True
                        org_settings.ai_data_sharing_enabled = payload.ai_data_sharing_enabled

                    if payload.ai_logging_acknowledged is not None:
                        if org_settings.ai_logging_acknowledged != payload.ai_logging_acknowledged:
                            ai_settings_changed = True
                        org_settings.ai_logging_acknowledged = payload.ai_logging_acknowledged

                    if ai_settings_changed:
                        org_settings.ai_settings_accepted_by = orguser.user
                        org_settings.ai_settings_accepted_at = timezone.now()

                    org_settings.save()

                    # Send notification if AI chat feature was enabled
                    if ai_chat_being_enabled:
                        send_ai_chat_enabled_notification(orguser.org, orguser.user.email)
                        logger.info(
                            f"Sent AI chat enabled notification for org {orguser.org.slug} (retry path)"
                        )

                    retry_update_org_data = OrgSettingsSchema(
                        organization_name=org_settings.organization_name,
                        website=org_settings.website,
                        organization_logo_filename=org_settings.organization_logo_filename,
                        ai_data_sharing_enabled=org_settings.ai_data_sharing_enabled,
                        ai_logging_acknowledged=org_settings.ai_logging_acknowledged,
                        ai_settings_accepted_by_email=org_settings.ai_settings_accepted_by.email
                        if org_settings.ai_settings_accepted_by
                        else None,
                        ai_settings_accepted_at=org_settings.ai_settings_accepted_at.isoformat()
                        if org_settings.ai_settings_accepted_at
                        else None,
                    )

                    return {"success": True, "res": retry_update_org_data.dict()}
                except Exception as retry_error:
                    logger.error(f"Error after creating table: {retry_error}")
                    raise HttpError(500, f"Failed after table creation: {str(retry_error)}")
            else:
                raise HttpError(
                    500, "Unable to create required database table. Please contact administrator."
                )
        else:
            logger.error(f"Database error updating org settings: {e}")
            logger.exception("Full traceback for database error:")
            raise HttpError(500, f"Database error: {str(e)}")
    except Exception as e:
        logger.error(f"Error updating org settings: {e}")
        logger.exception("Full traceback for org settings update error:")
        raise HttpError(500, f"Failed to update organization settings: {str(e)}")


@router.post("/", response=dict)
@has_permission(["can_manage_org_settings"])
@transaction.atomic
def create_org_settings(request, payload: CreateOrgSettingsSchema):
    """
    Create organization settings.
    Only Account Managers can create settings.
    """
    try:
        orguser = request.orguser
        if not orguser or not orguser.org:
            raise HttpError(400, "Organization not found")

        # Check if settings already exist
        if OrgSettings.objects.filter(org=orguser.org).exists():
            raise HttpError(400, "Organization settings already exist. Use PUT to update.")

        # Create new org settings
        org_settings = OrgSettings.objects.create(
            org=orguser.org,
            organization_name=payload.organization_name or orguser.org.name,
            website=payload.website,
            # organization_logo is set separately via upload endpoint
            ai_data_sharing_enabled=payload.ai_data_sharing_enabled,
            ai_logging_acknowledged=payload.ai_logging_acknowledged,
        )

        logger.info(f"Created org settings for org {orguser.org.slug} by user {orguser.user.email}")

        create_org_data = OrgSettingsSchema(
            organization_name=org_settings.organization_name,
            website=org_settings.website,
            organization_logo_filename=org_settings.organization_logo_filename,
            ai_data_sharing_enabled=org_settings.ai_data_sharing_enabled,
            ai_logging_acknowledged=org_settings.ai_logging_acknowledged,
            ai_settings_accepted_by_email=org_settings.ai_settings_accepted_by.email
            if org_settings.ai_settings_accepted_by
            else None,
            ai_settings_accepted_at=org_settings.ai_settings_accepted_at.isoformat()
            if org_settings.ai_settings_accepted_at
            else None,
        )

        return {"success": True, "res": create_org_data.dict()}

    except Exception as e:
        logger.error(f"Error creating org settings: {e}")
        logger.exception("Full traceback for org settings creation error:")
        raise HttpError(500, f"Failed to create organization settings: {str(e)}")


@router.patch("/ai-data-sharing", response=dict)
@has_permission(["can_manage_org_settings"])
@transaction.atomic
def update_ai_data_sharing(request):
    """
    Toggle AI data sharing setting.
    Only Account Managers can update this setting.
    """
    try:
        orguser = request.orguser
        if not orguser or not orguser.org:
            raise HttpError(400, "Organization not found")

        # Get or create org settings
        org_settings, created = OrgSettings.objects.get_or_create(
            org=orguser.org,
            defaults={
                "organization_name": orguser.org.name,
                "website": getattr(orguser.org, "website", None),
                "ai_data_sharing_enabled": False,
                "ai_logging_acknowledged": False,
            },
        )

        # Toggle the setting
        org_settings.ai_data_sharing_enabled = not org_settings.ai_data_sharing_enabled
        org_settings.save()

        logger.info(
            f"Toggled AI data sharing to {org_settings.ai_data_sharing_enabled} "
            f"for org {orguser.org.slug} by user {orguser.user.email}"
        )

        patch_org_data = OrgSettingsSchema(
            organization_name=org_settings.organization_name,
            website=org_settings.website,
            organization_logo_filename=org_settings.organization_logo_filename,
            ai_data_sharing_enabled=org_settings.ai_data_sharing_enabled,
            ai_logging_acknowledged=org_settings.ai_logging_acknowledged,
            ai_settings_accepted_by_email=org_settings.ai_settings_accepted_by.email
            if org_settings.ai_settings_accepted_by
            else None,
            ai_settings_accepted_at=org_settings.ai_settings_accepted_at.isoformat()
            if org_settings.ai_settings_accepted_at
            else None,
        )

        return {"success": True, "res": patch_org_data.dict()}

    except Exception as e:
        logger.error(f"Error toggling AI data sharing: {e}")
        raise HttpError(500, "Failed to update AI data sharing setting")


@router.post("/upload-logo", response=dict)
@has_permission(["can_manage_org_settings"])
@transaction.atomic
def upload_organization_logo(request, logo_file: UploadedFile = File(...)):
    """
    Upload organization logo file.
    Only Account Managers can upload logos.
    """
    try:
        orguser = request.orguser
        if not orguser or not orguser.org:
            raise HttpError(400, "Organization not found")

        logger.info(
            f"Logo upload request from user {orguser.user.email} for org {orguser.org.slug}"
        )
        logger.info(
            f"File details: name={logo_file.name}, size={logo_file.size}, type={logo_file.content_type}"
        )

        # Validate file type
        allowed_types = ["image/jpeg", "image/jpg", "image/png", "image/gif", "image/webp"]
        if logo_file.content_type not in allowed_types:
            raise HttpError(400, f"Invalid file type. Allowed types: {', '.join(allowed_types)}")

        # Validate file size (limit to 10MB)
        max_size = 10 * 1024 * 1024  # 10MB
        if logo_file.size > max_size:
            raise HttpError(400, "File size too large. Maximum size is 10MB.")

        # Get or create org settings (with auto-table creation)
        try:
            org_settings, created = OrgSettings.objects.get_or_create(
                org=orguser.org,
                defaults={
                    "organization_name": orguser.org.name,
                    "website": getattr(orguser.org, "website", None),
                    "ai_data_sharing_enabled": False,
                    "ai_logging_acknowledged": False,
                },
            )
        except ProgrammingError as e:
            if 'relation "ddpui_org_settings" does not exist' in str(e):
                logger.info(
                    "OrgSettings table does not exist. Attempting to create it for upload..."
                )
                if ensure_org_settings_table_exists():
                    # Retry after creating table
                    org_settings, created = OrgSettings.objects.get_or_create(
                        org=orguser.org,
                        defaults={
                            "organization_name": orguser.org.name,
                            "website": getattr(orguser.org, "website", None),
                            "ai_data_sharing_enabled": False,
                            "ai_logging_acknowledged": False,
                        },
                    )
                else:
                    raise HttpError(500, "Unable to create required database table")
            else:
                raise

        # Save the file data
        org_settings.organization_logo = logo_file.read()
        org_settings.organization_logo_filename = logo_file.name
        org_settings.organization_logo_content_type = logo_file.content_type
        org_settings.save()

        logger.info(
            f"Successfully uploaded logo for org {orguser.org.slug} by user {orguser.user.email}"
        )

        return {
            "success": True,
            "message": "Logo uploaded successfully",
            "filename": org_settings.organization_logo_filename,
        }

    except HttpError:
        # Re-raise HTTP errors as-is
        raise
    except Exception as e:
        logger.error(f"Error uploading organization logo: {e}")
        logger.exception("Full traceback for logo upload error:")
        raise HttpError(500, f"Failed to upload organization logo: {str(e)}")


@router.get("/logo")
@has_permission(["can_manage_org_settings"])
def get_organization_logo(request):
    """
    Get organization logo file.
    Only Account Managers can access logos.
    """
    try:
        orguser = request.orguser
        if not orguser or not orguser.org:
            raise HttpError(400, "Organization not found")

        org_settings = OrgSettings.objects.filter(org=orguser.org).first()

        if not org_settings or not org_settings.organization_logo:
            raise HttpError(404, "Organization logo not found")

        # Return the image file
        response = HttpResponse(
            org_settings.organization_logo,
            content_type=org_settings.organization_logo_content_type or "image/png",
        )
        response[
            "Content-Disposition"
        ] = f'inline; filename="{org_settings.organization_logo_filename or "logo"}"'
        return response

    except Exception as e:
        logger.error(f"Error retrieving organization logo: {e}")
        raise HttpError(500, "Failed to retrieve organization logo")


@router.get("/ai-status", response=dict, auth=CustomJwtAuthMiddleware())
def check_ai_status(request):
    """
    Check if AI features are enabled for the organization.
    Available to all authenticated users (no specific permission needed).
    """
    try:
        orguser = request.orguser
        if not orguser or not orguser.org:
            raise HttpError(400, "Organization not found")

        # Get org settings
        org_settings = OrgSettings.objects.filter(org=orguser.org).first()

        # Default to disabled if no settings exist
        # AI is enabled only when data sharing is enabled
        ai_enabled = False
        if org_settings:
            ai_enabled = org_settings.ai_data_sharing_enabled

        return {
            "success": True,
            "ai_enabled": ai_enabled,
            "data_sharing_enabled": org_settings.ai_data_sharing_enabled if org_settings else False,
            "logging_acknowledged": org_settings.ai_logging_acknowledged if org_settings else False,
        }

    except Exception as e:
        logger.error(f"Error checking AI status: {e}")
        raise HttpError(500, "Failed to check AI status")

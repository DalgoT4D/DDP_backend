from typing import Optional
from ninja import Schema

class CreateUserPreferencesSchema(Schema):
    user_id: int
    enable_discord_notifications: bool
    enable_email_notifications: bool
    discord_webhook: Optional[str] = None

class UpdateUserPreferencesSchema(Schema):
    user_id: int
    enable_discord_notifications: Optional[bool] = None
    enable_email_notifications: Optional[bool] = None
    discord_webhook: Optional[str] = None
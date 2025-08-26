import os
import json
from django.core.management.base import BaseCommand
from ddpui.utils.redis_client import RedisClient
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui")


class Command(BaseCommand):
    help = "Clear role_permission key from Redis cache"

    def add_arguments(self, parser):
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Deletes the role_permission key from Redis without making any changes",
        )

    def handle(self, *args, **options):
        dry_run = options["dry_run"]

        # Get the role permissions key from environment
        role_permissions_key = os.getenv("ROLE_PERMISSIONS_REDIS_KEY", "dalgo_permissions_key")

        try:
            redis_client = RedisClient.get_instance()

            # Check if role_permission key exists
            if redis_client.exists(role_permissions_key):
                if dry_run:
                    self.stdout.write(
                        self.style.WARNING(f"DRY RUN - Would delete key: {role_permissions_key}")
                    )
                    # Show the type and some info about the key
                    key_type = redis_client.type(role_permissions_key).decode("utf-8")
                    self.stdout.write(f"Key type: {key_type}")
                    if key_type == "string":
                        try:
                            value = redis_client.get(role_permissions_key)
                            if value:
                                permissions_data = json.loads(value)
                                self.stdout.write(
                                    f"Contains permissions for {len(permissions_data)} roles"
                                )
                        except json.JSONDecodeError:
                            self.stdout.write("Key contains non-JSON data")
                else:
                    # Delete the key
                    result = redis_client.delete(role_permissions_key)
                    if result:
                        self.stdout.write(
                            self.style.SUCCESS(
                                f"Successfully deleted {role_permissions_key} key from Redis"
                            )
                        )
                        logger.info(f"Cleared role permissions cache key: {role_permissions_key}")
                    else:
                        self.stdout.write(
                            self.style.WARNING(
                                f"{role_permissions_key} key was not found or already deleted"
                            )
                        )
            else:
                self.stdout.write(
                    self.style.WARNING(f"{role_permissions_key} key not found in Redis")
                )

        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Error clearing {role_permissions_key} key: {e}"))
            logger.error(f"Error clearing role permissions key: {e}", exc_info=True)

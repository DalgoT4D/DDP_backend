"""script to expire the redis sync-sources key for an org"""

from time import sleep
from dotenv import load_dotenv
from django.core.management.base import BaseCommand

from ddpui.utils.redis_client import RedisClient
from ddpui.utils.custom_logger import CustomLogger
from ddpui.models.org import Org
from ddpui.models.tasks import TaskProgressHashPrefix

logger = CustomLogger("ddpui")

load_dotenv()


class Command(BaseCommand):
    """
    This script unlocks a frozen sync-sources
    """

    help = "Unlock an Org's sync-sources"

    def add_arguments(self, parser):  # skipcq: PYL-R0201
        parser.add_argument("--org", required=True)

    def handle(self, *args, **options):
        """for the given user, set/unset specified attributes"""
        org = Org.objects.filter(slug=options["org"]).first()
        if org is None:
            print("Org not found")
            return
        redis = RedisClient.get_instance()
        hashkey = f"{TaskProgressHashPrefix.SYNCSOURCES}-{org.slug}"
        redis.expire(hashkey, 1)
        sleep(2)
        if len(redis.hkeys(hashkey)) > 0:
            print("Sync sources still locked for org: ", org.slug)
            return
        print("Sync sources unlocked for org: ", org.slug)

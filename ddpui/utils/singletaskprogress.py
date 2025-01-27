"""simple helper for a celery task to update its progress for its invoker to check on"""

import json
from ddpui.utils.redis_client import RedisClient


class SingleTaskProgress:
    """
    maintain a list of steps with the task_key as the key
    only one task can be associated with a task_key
    the redis key is only removed by the expire time
    not by the destruction of this object
    """

    def __init__(self, task_key: str, expire_in_seconds: int) -> None:
        self.task_key_ = task_key
        self.redis = RedisClient.get_instance()
        self.redis.set(self.task_key_, json.dumps([]), expire_in_seconds)

    def get(self) -> list:
        """get the list of progress"""
        return json.loads(self.redis.get(self.task_key_))

    def set(self, progress: list) -> None:
        """set the list of progress"""
        expiry = self.redis.ttl(self.task_key_)
        self.redis.set(self.task_key_, json.dumps(progress), expiry)

    def add(self, progress) -> None:
        """append the latest progress to the list and update in redis"""
        taskprogress = self.get()
        taskprogress.append(progress)
        self.set(taskprogress)

    @staticmethod
    def fetch(task_key):
        """look up progress by task_key"""
        redis = RedisClient.get_instance()
        result = redis.get(task_key)
        if result:
            return json.loads(result)
        return None

    @staticmethod
    def get_ttl(task_key: str) -> int:
        """return the ttl for the key"""
        redis = RedisClient.get_instance()
        return redis.ttl(task_key)

    @staticmethod
    def remove(task_key: str) -> int:
        """remove the key"""
        redis = RedisClient.get_instance()
        return redis.delete(task_key)

"""simple helper for a celery task to update its progress for its invoker to check on"""

import json
from ddpui.utils.redis_client import  RedisClient


class TaskProgress:
    """
    maintain a list of steps, store in redis under the name "taskprogress"
    with the task_id as the key
    """

    def __init__(self, task_id, hashkey="taskprogress", expire_in_seconds=None) -> None:
        self.hashkey = hashkey
        self.task_id = task_id
        self.taskprogress = []
        self.redis = RedisClient.get_instance()
        if expire_in_seconds:
            self.redis.expire(hashkey, expire_in_seconds)

    def add(self, progress) -> None:
        """append the latest progress to the list and update in redis"""
        self.taskprogress.append(progress)
        self.redis.hset(self.hashkey, self.task_id, json.dumps(self.taskprogress))

    def remove(self) -> None:
        """removes the hash from redis"""
        self.redis.delete(self.hashkey)

    @staticmethod
    def fetch(task_id, hashkey="taskprogress"):
        """look up progress by task_id"""
        redis = RedisClient.get_instance()
        result = redis.hget(hashkey, task_id)
        if result:
            return json.loads(result)
        return None

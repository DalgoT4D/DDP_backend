"""simple helper for a celery task to update its progress for its invoker to check on"""

import json
from redis import Redis


class TaskProgress:
    """
    maintain a list of steps, store in redis under the name "taskprogress"
    with the task_id as the key
    """

    def __init__(self, task_id, hashkey, expire_in_seconds: int | None = None) -> None:
        self.hashkey = hashkey
        self.task_id = task_id
        self.taskprogress = []
        self.redis = Redis()
        # the key doesn't exist yet, can't set the expiration
        self.expiration_set = False
        self.expire_in_seconds = expire_in_seconds

    def add(self, progress) -> None:
        """append the latest progress to the list and update in redis"""
        self.taskprogress.append(progress)
        self.redis.hset(self.hashkey, self.task_id, json.dumps(self.taskprogress))
        if not self.expiration_set:
            if self.expire_in_seconds:
                self.redis.expire(self.hashkey, self.expire_in_seconds)
            self.expiration_set = True

    def remove(self) -> None:
        """removes the hash from redis"""
        self.redis.delete(self.hashkey)

    @staticmethod
    def fetch(task_id, hashkey):
        """look up progress by task_id"""
        redis = Redis()
        result = redis.hget(hashkey, task_id)
        if result:
            return json.loads(result)
        return None

    @staticmethod
    def get_running_tasks(hashkey):
        """look up any running tasks for this hashkey"""
        redis = Redis()
        return redis.hkeys(hashkey)

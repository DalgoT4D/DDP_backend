"""simple helper for a celery task to update its progress for its invoker to check on"""
import json
from redis import Redis


class TaskProgress:
    """
    maintain a list of steps, store in redis under the name "taskprogress"
    with the task_id as the key
    """

    def __init__(self, task_id) -> None:
        self.task_id = task_id
        self.taskprogress = []
        self.redis = Redis()

    def add(self, progress) -> None:
        """append the latest progress to the list and update in redis"""
        self.taskprogress.append(progress)
        self.redis.hset('taskprogress', self.task_id, json.dumps(self.taskprogress))

    @staticmethod
    def fetch(task_id):
        """look up progress by task_id"""
        redis = Redis()
        result = redis.hget('taskprogress', task_id)
        if result:
            return json.loads(result)
        return None

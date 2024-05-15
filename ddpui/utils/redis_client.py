import os
from redis import Redis


class RedisClient:
    """
    class to instantiate a Redis client. Use this class anywhere in the code to interact with the Redis server
    """

    _redis_instance = None

    @classmethod
    def get_instance(cls):
        if cls._redis_instance is None:
            host = os.getenv("REDIS_HOST", "localhost")
            port = int(os.getenv("REDIS_PORT", "6379"))
            cls._redis_instance = Redis(host=host, port=port)
        return cls._redis_instance

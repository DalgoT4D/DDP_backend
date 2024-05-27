import os
import threading

from redis import Redis


class RedisClient:
    """
    Singleton Class to instantiate a Redis client.
    Use this class anywhere in the code to interact with the Redis server
    """

    lock = threading.Lock()
    _redis_instance = None

    @classmethod
    def get_instance(cls) -> Redis:
        """
        Returns the Redis instance.
        If the Redis instance is not already created, it creates a new instance using the
        host and port specified in the environment variables REDIS_HOST and REDIS_PORT.
        If the environment variables are not set, it defaults to using "localhost" as the host
        and "6379" as the port.
        To prevent multiple Redis object being created we make use of locks
        Returns:
            Redis: The Redis instance.
        """
        if cls._redis_instance is None:
            if cls.lock.acquire(timeout=10):
                if cls._redis_instance is None:
                    host = os.getenv("REDIS_HOST", "localhost")
                    port = int(os.getenv("REDIS_PORT", "6379"))
                    cls._redis_instance = Redis(host=host, port=port)
            cls.lock.release()
        return cls._redis_instance

    @classmethod
    def reset_instance(cls) -> None:
        """
        Reset the instance to None
        """
        cls._redis_instance = None

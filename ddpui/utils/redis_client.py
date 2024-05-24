import os

from redis import Redis


class RedisClient:
    """Class to instantiate a Redis client. Use this class anywhere in the code to interact with the Redis server"""

    _redis_instance = None

    @classmethod
    def get_instance(cls):
        """
        Returns the Redis instance.

        If the Redis instance is not already created, it creates a new instance using the
        host and port specified in the environment variables REDIS_HOST and REDIS_PORT.
        If the environment variables are not set, it defaults to using "localhost" as the host
        and "6379" as the port.

        Returns:
            Redis: The Redis instance.

        """
        if cls._redis_instance is None:
            host = os.getenv("REDIS_HOST", "localhost")
            port = int(os.getenv("REDIS_PORT", "6379"))
            cls._redis_instance = Redis(host=host, port=port)
        return cls._redis_instance

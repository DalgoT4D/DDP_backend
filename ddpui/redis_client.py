from redis import Redis
import threading


class RedisClient:
    """
    Singleton class to manage Redis connection
    We dont actually create an object of this class since python doesn't allow private constructors
    """

    lock = threading.Lock()
    conn_instance: Redis = None

    host = "localhost"
    port = 6379

    @classmethod
    def get_instance(cls) -> Redis:
        if cls.conn_instance is None:
            if cls.lock.acquire():
                if cls.conn_instance is None:
                    cls.conn_instance = Redis(host=cls.host, port=cls.port)
            cls.lock.release()

        return cls.conn_instance

    @classmethod
    def reset_instance(cls) -> None:
        cls.conn_instance = None

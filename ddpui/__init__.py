__all__ = ("celery_app",)


def __getattr__(name):
    """Load the Celery app only when the Celery entrypoint asks for it."""
    if name != "celery_app":
        raise AttributeError(name)
    from .celery import app as celery_app

    return celery_app

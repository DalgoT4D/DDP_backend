import threading

# Thread-local storage
_thread_locals = threading.local()


def get_current_request():
    """get the current request object from the thread"""
    return getattr(_thread_locals, "request", None)


def set_current_request(request):
    """set the current request object in the thread"""
    _thread_locals.request = request

import os
from django.http import HttpResponse, Http404

from ddpui.utils.redis_client import RedisClient
from ddpui.utils.file_storage.storage_factory import StorageFactory


def get_elementary_report(request, tokenhex: str):
    """serve the generated docs"""
    redis = RedisClient.get_instance()
    redis_key = f"elementary-report-{tokenhex}"
    htmlfilename = redis.get(redis_key)
    if htmlfilename is None:
        raise Http404("link has expired")

    htmlfilename = htmlfilename.decode("utf-8")
    storage = StorageFactory.get_storage_adapter()
    if not storage.exists(htmlfilename):
        raise Http404("link has expired")

    html = storage.read_file(htmlfilename)
    response = HttpResponse(html)
    # the only valid values for x-frame-options are "deny" and "sameorigin", both
    # of which will stop the iframe from rendering the docs
    # removing the header causes it to be set to "deny" by django
    # but if we set it to an invalid value, it makes its way to the browser where
    # it is ignored
    response.headers["X-Frame-Options"] = "ignore"
    response.headers[
        "Content-Security-Policy"
    ] = f"frame-src localhost:8002 {request.headers['Host']};"
    return response

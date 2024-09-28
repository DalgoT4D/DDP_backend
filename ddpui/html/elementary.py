import os
from django.http import HttpResponse, Http404

from ddpui.utils.redis_client import RedisClient


def get_elementary_report(request, tokenhex: str):
    """serve the generated docs"""
    redis = RedisClient.get_instance()
    redis_key = f"elementary-report-{tokenhex}"
    htmlfilename = redis.get(redis_key)
    if htmlfilename is None:
        raise Http404("link has expired")

    htmlfilename = htmlfilename.decode("utf-8")
    if not os.path.exists(htmlfilename):
        raise Http404("link has expired")

    with open(htmlfilename, "r", encoding="utf-8") as htmlfile:
        html = htmlfile.read()
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

from django.http import HttpResponse


def healthcheck(request):  # pylint:disable=unused-argument
    """Healthcheck endpoint for load balancers"""
    return HttpResponse("OK")

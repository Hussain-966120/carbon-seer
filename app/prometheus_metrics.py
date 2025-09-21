from prometheus_client import Histogram, Counter, generate_latest, CONTENT_TYPE_LATEST
from flask import Response

REQUEST_LATENCY = Histogram(
    "demo_request_latency_seconds", "Request latency seconds",
    buckets=(.01, .02, .05, .1, .2, .5, 1, 2)
)
REQUEST_COUNT = Counter("demo_requests_total", "Total requests")

def metrics_response():
    return Response(generate_latest(), mimetype=CONTENT_TYPE_LATEST)

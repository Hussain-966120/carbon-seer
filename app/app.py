from flask import Flask, request
from prometheus_metrics import REQUEST_LATENCY, REQUEST_COUNT, metrics_response
import random, time, os

app = Flask(__name__)

@app.route("/")
def index():
    REQUEST_COUNT.inc()
    # Simulate request-work determined by ?load=0.05 (seconds)
    load = float(request.args.get("load", os.getenv("DEFAULT_LOAD", "0.05")))
    t = random.uniform(load * 0.8, load * 1.4)
    with REQUEST_LATENCY.time():
        time.sleep(t)
    return f"OK {t:.3f}", 200

@app.route("/metrics")
def metrics():
    return metrics_response()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)

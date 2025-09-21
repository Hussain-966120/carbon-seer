#!/usr/bin/env python3
"""
Replayer that reads replayer/carbon_traces.csv and posts updates to autoscaler endpoint.
CSV format: timestamp,region,carbon
"""
import csv, time, os, requests

CARBON_CSV = os.getenv("CARBON_CSV", "carbon_traces.csv")
AUTOSCALER_ENDPOINT = os.getenv("AUTOSCALER_ENDPOINT", "http://localhost:9200/update_carbon")
SLEEP_SEC = float(os.getenv("REPLAYER_SLEEP", "5"))

def replay_once():
    if not os.path.exists(CARBON_CSV):
        print("No carbon_traces.csv found in replayer directory.")
        return
    with open(CARBON_CSV) as f:
        reader = csv.DictReader(f)
        for row in reader:
            region = row.get("region", "local")
            carbon = row.get("carbon")
            ts = row.get("timestamp", "")
            try:
                payload = {"region": region, "carbon": float(carbon), "timestamp": ts}
                print(f"[replayer] push {payload}")
                requests.post(AUTOSCALER_ENDPOINT, json=payload, timeout=5)
            except Exception as e:
                print("replayer post error:", e)
            time.sleep(SLEEP_SEC)

if __name__ == "__main__":
    while True:
        replay_once()

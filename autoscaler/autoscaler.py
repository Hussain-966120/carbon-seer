#!/usr/bin/env python3
"""
CarbOnSeer autoscaler (updated):
- Queries Prometheus for p95 latency
- Queries ElectricityMap or WattTime APIs for latest carbon intensity (with caching + fallback)
- Exposes a small HTTP endpoint (/update_carbon) so replayer can push trace points
- Scales Deployment up/down by 1 based on SLO and simple policy.
"""
import time
import argparse
import yaml
import threading
import math
import os
import json
from http.server import BaseHTTPRequestHandler, HTTPServer

import requests
from prometheus_client import start_http_server, Gauge, Counter
from kubernetes import client, config as k8s_config
from prometheus_api_client import PrometheusConnect

SCALE_EVENTS = Counter("carb_scale_events_total", "Scale events emitted")
CURRENT_SCORE = Gauge("carb_current_score", "Last computed objective score")
P95_LATENCY = Gauge("carb_p95_latency_ms", "P95 latency (ms) seen")
CARBON_G = Gauge("carb_current_gco2_per_kwh", "Current carbon intensity (gCO2/kWh)")

class CarbOnSeer:
    def __init__(self, cfg):
        self.cfg = cfg
        # Prometheus
        self.prom = PrometheusConnect(url=cfg['prometheus']['url'], disable_ssl=True)
        # kube
        k8s_config.load_kube_config()  # local dev; in-cluster uses load_incluster_config
        self.apps = client.AppsV1Api()
        # carbon
        self.carbon_map = cfg.get("carbon_map", {})
        self._carbon_cache = {}   # region -> (value, timestamp)
        self.providers = cfg.get("carbon_providers", {})
        # autoscaling params
        self.min_replicas = cfg.get("min_replicas", 1)
        self.max_replicas = cfg.get("max_replicas", 10)
        self.slo_p95_ms = cfg.get("slo_p95_ms", 200)
        self.weights = cfg.get("weights", {'slo':0.6,'carbon':0.3,'cost':0.1})
        self.cost_per_replica = cfg.get("cost_per_replica", 0.02)
        self.deployment = cfg['deployment']
        # HTTP API server if enabled
        self.http_host = cfg.get("http_api", {}).get("host", "0.0.0.0")
        self.http_port = cfg.get("http_api", {}).get("port", 9200)
        self.http_enabled = cfg.get("http_api", {}).get("enabled", True)
        self.cache_ttl = cfg.get("cache_ttl_seconds", 300)

    # ---------------------------
    # Prometheus queries
    # ---------------------------
    def query_p95(self):
        q = 'histogram_quantile(0.95, sum(rate(demo_request_latency_seconds_bucket[1m])) by (le))'
        try:
            res = self.prom.custom_query(q)
            if res and len(res) > 0 and 'value' in res[0]:
                val = float(res[0]['value'][1])
                return val * 1000.0  # seconds -> ms
        except Exception as e:
            print("Prometheus query error:", e)
        return None

    # ---------------------------
    # Carbon providers
    # ---------------------------
    def fetch_electricitymap(self, zone):
        prov = self.providers.get("electricitymap", {})
        api_key = prov.get("api_key") or os.getenv("ELECTRICITYMAP_API_KEY")
        endpoint = prov.get("endpoint")
        if not api_key or not endpoint:
            return None
        params = {"zone": zone}
        headers = {"auth-token": api_key}
        try:
            r = requests.get(endpoint, params=params, headers=headers, timeout=8)
            r.raise_for_status()
            j = r.json()
            # ElectricityMap v3 returns structure with data and carbon intensity in gCO2eq/kWh
            # e.g. {"data": {"carbonIntensity": <value>, ...}}
            val = None
            if "data" in j and ("carbonIntensity" in j["data"] or "carbon_intensity" in j["data"]):
                val = j["data"].get("carbonIntensity") or j["data"].get("carbon_intensity")
            else:
                # some responses return direct payload: try keys
                for k in ("carbonIntensity", "carbon_intensity", "gCO2eq_per_kWh"):
                    if k in j:
                        val = j[k]
                        break
            if val is not None:
                return float(val)
        except Exception as e:
            print("ElectricityMap fetch error:", e)
        return None

    def fetch_watttime_index(self, params=None):
        prov = self.providers.get("watttime", {})
        token = prov.get("token") or os.getenv("WATTTIME_TOKEN")
        endpoint = prov.get("endpoint_index")
        if not token or not endpoint:
            return None
        headers = {"Authorization": f"Token {token}"}
        try:
            r = requests.get(endpoint, headers=headers, params=params or {}, timeout=8)
            r.raise_for_status()
            j = r.json()
            # WattTime index endpoint returns fields like 'moer' or 'index_value'
            # use 'moer' if present, else 'index'
            if "moer" in j:
                return float(j["moer"])
            if "index_value" in j:
                return float(j["index_value"])
            if "value" in j:
                return float(j["value"])
        except Exception as e:
            print("WattTime fetch error:", e)
        return None

    def get_carbon(self, region="local"):
        now = time.time()
        # cached?
        c = self._carbon_cache.get(region)
        if c and (now - c[1]) < self.cache_ttl:
            return c[0]
        # try providers in order: electricitymap, watttime
        val = None
        em = self.providers.get("electricitymap", {})
        if em.get("enabled"):
            try:
                val = self.fetch_electricitymap(em.get("zone", region))
            except Exception as e:
                print("em fetch error", e)
        if val is None:
            wt = self.providers.get("watttime", {})
            if wt.get("enabled"):
                try:
                    val = self.fetch_watttime_index()
                except Exception as e:
                    print("wt fetch error", e)
        if val is None:
            # fallback: use configured map
            val = float(self.carbon_map.get(region, self.cfg.get("default_carbon", 300)))
        # cache it
        self._carbon_cache[region] = (val, now)
        return val

    # ---------------------------
    # scoring & scaling
    # ---------------------------
    def compute_scores(self, p95_ms, region='local'):
        # SLO score: 1 if p95 <= slo_p95_ms else linearly degrade
        if p95_ms is None:
            slo_score = 0.5
        else:
            slo_score = 1.0 if p95_ms <= self.slo_p95_ms else max(0.0, 1.0 - (p95_ms - self.slo_p95_ms) / (self.slo_p95_ms * 5))
        carbon_val = self.get_carbon(region)
        # normalize carbon in 0..1000 gCO2eq/kWh
        carbon_score = max(0.0, 1.0 - carbon_val / 1000.0)
        cost_score = 1.0 - min(1.0, self.cost_per_replica / max(1e-6, self.cfg.get("cost_norm_max", 1.0)))
        combined = self.weights['slo']*slo_score + self.weights['carbon']*carbon_score + self.weights['cost']*cost_score
        return {'slo': slo_score, 'carbon': carbon_score, 'cost': cost_score, 'combined': combined, 'carbon_g': carbon_val}

    def get_current_replicas(self):
        ns = self.deployment['namespace']; name = self.deployment['name']
        dep = self.apps.read_namespaced_deployment(name, ns)
        return dep.spec.replicas

    def patch_replicas(self, replicas):
        ns = self.deployment['namespace']; name = self.deployment['name']
        body = {'spec': {'replicas': replicas}}
        self.apps.patch_namespaced_deployment(name, ns, body)
        SCALE_EVENTS.inc()
        print(f"[autoscaler] patched {name} -> replicas {replicas}")

    # ---------------------------
    # HTTP endpoint to accept replayer updates
    # ---------------------------
    def start_http_server_for_updates(self):
        if not self.http_enabled:
            return

        parent = self
        class ReqHandler(BaseHTTPRequestHandler):
            def do_POST(self):
                if self.path != "/update_carbon":
                    self.send_response(404); self.end_headers(); return
                length = int(self.headers.get("content-length", 0))
                raw = self.rfile.read(length) if length>0 else b"{}"
                try:
                    j = json.loads(raw)
                    region = j.get("region", "local")
                    carbon = float(j.get("carbon"))
                    # update internal map & cache
                    parent.carbon_map[region] = carbon
                    parent._carbon_cache[region] = (carbon, time.time())
                    print(f"[http] updated carbon_map {region} -> {carbon}")
                    self.send_response(200); self.end_headers()
                    self.wfile.write(b"ok")
                except Exception as e:
                    print("HTTP update error:", e)
                    self.send_response(400); self.end_headers()
            def log_message(self, format, *args):
                # silence default logging
                return

        server = HTTPServer((self.http_host, self.http_port), ReqHandler)
        t = threading.Thread(target=server.serve_forever, daemon=True)
        t.start()
        print(f"[http] Carbon update endpoint running on {self.http_host}:{self.http_port}/update_carbon")

    # ---------------------------
    # main loop
    # ---------------------------
    def run_loop(self):
        # start http receiver for replayer pushes
        self.start_http_server_for_updates()
        while True:
            p95 = self.query_p95()
            if p95 is not None:
                P95_LATENCY.set(p95)
            scores = self.compute_scores(p95)
            CURRENT_SCORE.set(scores['combined'])
            CARBON_G.set(scores['carbon_g'])
            try:
                cur = self.get_current_replicas()
            except Exception as e:
                print("Error reading replicas:", e)
                time.sleep(self.cfg.get("interval_s", 15))
                continue
            newr = cur
            # simple policy: scale up when SLO violated, scale down when much under SLO
            if p95 is not None:
                if p95 > self.slo_p95_ms and cur < self.max_replicas:
                    newr = min(self.max_replicas, cur + 1)
                elif p95 < (self.slo_p95_ms * 0.6) and cur > self.min_replicas:
                    newr = max(self.min_replicas, cur - 1)
            if newr != cur:
                try:
                    self.patch_replicas(newr)
                except Exception as e:
                    print("Error patching replicas:", e)
            print(f"[autoscaler] p95={p95}ms scores={scores} replicas={cur}->{newr}")
            time.sleep(self.cfg.get("interval_s", 15))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="config.yaml")
    args = parser.parse_args()
    with open(args.config) as fh:
        cfg = yaml.safe_load(fh)
    start_http_server(cfg.get("metrics_port", 9102))
    cs = CarbOnSeer(cfg)
    cs.run_loop()

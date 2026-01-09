import os
import time
import requests
from typing import Any, Dict, List, Optional

from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.exporter.prometheus_remote_write import PrometheusRemoteWriteMetricsExporter


ECOWITT_APP_KEY = os.environ["ECOWITT_APP_KEY"]
ECOWITT_API_KEY = os.environ["ECOWITT_API_KEY"]
ECOWITT_MAC = os.environ["ECOWITT_MAC"]

GRAFANA_RW_URL = os.environ["GRAFANA_RW_URL"]
GRAFANA_RW_USERNAME = os.environ["GRAFANA_RW_USERNAME"]
GRAFANA_RW_PASSWORD = os.environ["GRAFANA_RW_PASSWORD"]


def fetch_ecowitt_realtime() -> Dict[str, Any]:
    url = "https://api.ecowitt.net/api/v3/device/real_time"
    params = {
        "application_key": ECOWITT_APP_KEY,
        "api_key": ECOWITT_API_KEY,
        "mac": ECOWITT_MAC,
        "call_back": "all",
    }
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json()


def _to_float(v: Any) -> Optional[float]:
    try:
        return float(v)
    except Exception:
        return None


def normalize_ecowitt_data(raw_data: Any) -> Dict[str, Any]:
    if isinstance(raw_data, dict):
        return raw_data

    if isinstance(raw_data, list):
        out: Dict[str, Any] = {}
        for item in raw_data:
            if not isinstance(item, dict):
                continue
            if "key" in item:
                out[item["key"]] = item
            elif len(item) == 1:
                k, v = next(iter(item.items()))
                out[k] = v
        return out

    return {}


def pick(data: Dict[str, Any], *names: str) -> Any:
    for n in names:
        if n in data:
            v = data[n]
            if isinstance(v, dict) and "value" in v:
                return v["value"]
            return v
    return None


def main():
    print("START ecowitt_to_grafana", flush=True)

    exporter = PrometheusRemoteWriteMetricsExporter(
        endpoint=GRAFANA_RW_URL,
        basic_auth={
            "username": GRAFANA_RW_USERNAME,
            "password": GRAFANA_RW_PASSWORD,
        },
    )

    reader = PeriodicExportingMetricReader(exporter, export_interval_millis=1000)
    provider = MeterProvider(metric_readers=[reader])
    meter = provider.get_meter("ecowitt")

    g_temp = meter.create_gauge("ecowitt_temperature_c", unit="C")

    payload = fetch_ecowitt_realtime()
    raw_data = payload.get("data", [])
    data = normalize_ecowitt_data(raw_data)

    print("Payload code/msg:", payload.get("code"), payload.get("msg"), flush=True)
    print("Normalized keys:", list(data.keys())[:20], flush=True)

    temp = _to_float(pick(data, "temp", "outdoor_temperature", "temperature"))

    print("Temp value:", temp, flush=True)

    if temp is not None:
        g_temp.set(temp, {"station_mac": ECOWITT_MAC})

    provider.force_flush()
    time.sleep(5)
    provider.shutdown()

    print("DONE", flush=True)


if __name__ == "__main__":
    main()


import os
import time
import requests

from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.exporter.prometheus_remote_write import PrometheusRemoteWriteMetricsExporter

ECOWITT_APP_KEY = os.environ["ECOWITT_APP_KEY"]
ECOWITT_API_KEY = os.environ["ECOWITT_API_KEY"]
ECOWITT_MAC = os.environ["ECOWITT_MAC"]

GRAFANA_RW_URL = os.environ["GRAFANA_RW_URL"]
GRAFANA_RW_USERNAME = os.environ["GRAFANA_RW_USERNAME"]
GRAFANA_RW_PASSWORD = os.environ["GRAFANA_RW_PASSWORD"]

print("START ecowitt_to_grafana (no-indent version)", flush=True)
print("Remote write username(first6):", (GRAFANA_RW_USERNAME[:6] if GRAFANA_RW_USERNAME else ""), flush=True)

# --- Fetch Ecowitt ---
url = "https://api.ecowitt.net/api/v3/device/real_time"
params = {
    "application_key": ECOWITT_APP_KEY,
    "api_key": ECOWITT_API_KEY,
    "mac": ECOWITT_MAC,
    "call_back": "all",
}
def fetch_ecowitt_realtime() -> dict:
    url = "https://api.ecowitt.net/api/v3/device/real_time"
    params = {
        "application_key": ECOWITT_APP_KEY,
        "api_key": ECOWITT_API_KEY,
        "mac": ECOWITT_MAC,
        "call_back": "all",
    }

    last_err = None
    for attempt in range(1, 5):  # 4 intentos
        try:
            # timeout=(connect, read)
            r = requests.get(url, params=params, timeout=(10, 90))
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_err = e
            print(f"Ecowitt request failed (attempt {attempt}/4): {repr(e)}", flush=True)
            time.sleep(5 * attempt)  # 5s, 10s, 15s, 20s

    # si falla todo: levantamos el error para ver el motivo en el log
    raise last_err

print("Ecowitt payload keys:", (list(payload.keys()) if isinstance(payload, dict) else type(payload)), flush=True)
print("Ecowitt code/msg:", (payload.get("code") if isinstance(payload, dict) else None), (payload.get("msg") if isinstance(payload, dict) else None), flush=True)

raw_data = payload.get("data") if isinstance(payload, dict) else None
print("raw_data type:", type(raw_data), flush=True)

# --- Normalize data to dict (best-effort) ---
data = raw_data if isinstance(raw_data, dict) else (
    {item.get("key"): item for item in raw_data if isinstance(item, dict) and isinstance(item.get("key"), str)}
    if isinstance(raw_data, list) else {}
)

print("normalized keys sample:", (list(data.keys())[:30] if isinstance(data, dict) else None), flush=True)

# --- Pick helper (no function, no indent) ---
def _unwrap(v):
    return (v.get("value") if isinstance(v, dict) and "value" in v else v)

def _to_float(x):
    try:
        return float(x)
    except Exception:
        return None

temp_raw = _unwrap(next((data.get(k) for k in ("temp", "outdoor_temperature", "temp_out", "temperature", "outtemp") if isinstance(data, dict) and k in data), None))
hum_raw  = _unwrap(next((data.get(k) for k in ("humidity", "outdoor_humidity", "humi_out", "humi") if isinstance(data, dict) and k in data), None))
press_raw= _unwrap(next((data.get(k) for k in ("baromabs", "baromrel", "pressure", "press") if isinstance(data, dict) and k in data), None))
wind_raw = _unwrap(next((data.get(k) for k in ("windspeed", "wind_speed", "wind", "windspd") if isinstance(data, dict) and k in data), None))
gust_raw = _unwrap(next((data.get(k) for k in ("windgust", "gustspeed", "wind_gust", "gust") if isinstance(data, dict) and k in data), None))
rain_raw = _unwrap(next((data.get(k) for k in ("rainrate", "rain_rate", "rainRate") if isinstance(data, dict) and k in data), None))

temp = _to_float(temp_raw)
hum = _to_float(hum_raw)
press = _to_float(press_raw)
wind = _to_float(wind_raw)
gust = _to_float(gust_raw)
rainrate = _to_float(rain_raw)

print("Values:", "temp=", temp, "hum=", hum, "press=", press, "wind=", wind, "gust=", gust, "rainrate=", rainrate, flush=True)

# --- Export to Grafana Cloud remote_write ---
exporter = PrometheusRemoteWriteMetricsExporter(
    endpoint=GRAFANA_RW_URL,
    basic_auth={"username": GRAFANA_RW_USERNAME, "password": GRAFANA_RW_PASSWORD},
    headers={},
)

reader = PeriodicExportingMetricReader(exporter, export_interval_millis=1000)
provider = MeterProvider(metric_readers=[reader])
meter = provider.get_meter("ecowitt")

g_temp_c = meter.create_gauge("ecowitt_temperature_c", unit="C")
g_hum_pct = meter.create_gauge("ecowitt_humidity_pct", unit="%")
g_press_hpa = meter.create_gauge("ecowitt_pressure_hpa", unit="hPa")
g_wind_ms = meter.create_gauge("ecowitt_wind_speed_ms", unit="m/s")
g_gust_ms = meter.create_gauge("ecowitt_wind_gust_ms", unit="m/s")
g_rainrate_mm = meter.create_gauge("ecowitt_rain_rate_mm", unit="mm")

labels = {"station_mac": ECOWITT_MAC}

(temp is not None) and g_temp_c.set(temp, labels)
(hum is not None) and g_hum_pct.set(hum, labels)
(press is not None) and g_press_hpa.set(press, labels)
(wind is not None) and g_wind_ms.set(wind, labels)
(gust is not None) and g_gust_ms.set(gust, labels)
(rainrate is not None) and g_rainrate_mm.set(rainrate, labels)

provider.force_flush()
print("force_flush() OK", flush=True)

time.sleep(10)
provider.shutdown()
print("DONE", flush=True)

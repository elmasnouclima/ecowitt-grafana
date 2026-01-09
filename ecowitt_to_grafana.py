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


def fetch_ecowitt_realtime() -> dict:
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


def _to_float(v):
    try:
        return float(v)
    except Exception:
        return None


def _normalize_ecowitt_data(data):
    """
    Ecowitt a veces devuelve data como dict y a veces como lista.
    Si es lista, suele ser:
      [{"key":"temp","value":"12.3","unit":"C"}, ...]
    Lo convertimos a dict:
      {"temp": {"value":"12.3","unit":"C"}, ...}
    """
    if isinstance(data, dict):
        return data

    if isinstance(data, list):
        out = {}
        for item in data:
            if not isinstance(item, dict):
                continue

            k = item.get("key") or item.get("name") or item.get("field")
            if not k:
                continue

            out[k] = item
        return out

    return {}


def _pick(data: dict, *names):
    for name in names:
        if name not in data:
            continue
        v = data.get(name)
        if isinstance(v, dict):
            if "value" in v:
                v = v.get("value")
            elif "val" in v:
                v = v.get("val")
        if v is not None:
            return v
    return None


def main():
    print("START ecowitt_to_grafana", flush=True)
    print(f"Remote write URL: {GRAFANA_RW_URL}", flush=True)
    print(f"Remote write username(first6): {GRAFANA_RW_USERNAME[:6]}", flush=True)

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

    payload = fetch_ecowitt_realtime()
    raw_data = payload.get("data", {}) if isinstance(payload, dict) else {}
    data = _normalize_ecowitt_data(raw_data)

    print("Ecowitt code/msg:", payload.get("code"), payload.get("msg"), flush=True)
    print(
        "raw_data type:",
        type(raw_data),
        "len:",
        (len(raw_data) if isinstance(raw_data, list) else "n/a"),
        flush=True,
    )
    print(
        "raw_data first item:",
        (raw_data[0] if isinstance(raw_data, list) and len(raw_data) > 0 else None),
        flush=True,
    )
    print(
        "raw_data first 5:",
        (raw_data[:5] if isinstance(raw_data, list) else None),
        flush=True,
    )

if isinstance(raw_data, list):
    print("Ecowitt raw data type: list, len=", len(raw_data), flush=True)
    print("Ecowitt raw data sample (first 5):", raw_data[:5], flush=True)
elif isinstance(raw_data, dict):
    print("Ecowitt raw data type: dict, keys sample:", list(raw_data.keys())[:30], flush=True)
else:
    print("Ecowitt raw data type:", type(raw_data), flush=True)


    print("Ecowitt payload keys:", (list(payload.keys()) if isinstance(payload, dict) else type(payload)), flush=True)
    print("Ecowitt raw data type:", type(raw_data), flush=True)
    print("Ecowitt normalized data sample keys:", list(data.keys())[:30], flush=True)

    labels = {"station_mac": ECOWITT_MAC}

    temp = _to_float(_pick(data, "temp", "outdoor_temperature", "temp_out", "tempin"))
    hum = _to_float(_pick(data, "humidity", "outdoor_humidity", "humi_out", "humidityin"))
    press = _to_float(_pick(data, "baromabs", "baromrel", "pressure", "press"))
    wind = _to_float(_pick(data, "windspeed", "wind_speed", "wind"))
    gust = _to_float(_pick(data, "windgust", "gustspeed", "wind_gust"))
    rainrate = _to_float(_pick(data, "rainrate", "rain_rate", "rain_rate_piezo"))

    print(f"Values: temp={temp} hum={hum} press={press} wind={wind} gust={gust} rainrate={rainrate}", flush=True)

    if temp is not None:
        g_temp_c.set(temp, labels)
    if hum is not None:
        g_hum_pct.set(hum, labels)
    if press is not None:
        g_press_hpa.set(press, labels)
    if wind is not None:
        g_wind_ms.set(wind, labels)
    if gust is not None:
        g_gust_ms.set(gust, labels)
    if rainrate is not None:
        g_rainrate_mm.set(rainrate, labels)

    provider.force_flush()
    print("force_flush() OK", flush=True)

    time.sleep(10)
    provider.shutdown()
    print("DONE", flush=True)


if __name__ == "__main__":
    main()

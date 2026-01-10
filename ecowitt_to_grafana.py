import os
import time
import requests
from typing import Any, Dict, Optional

from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.exporter.prometheus_remote_write import PrometheusRemoteWriteMetricsExporter


ECOWITT_APP_KEY = os.environ["ECOWITT_APP_KEY"]
ECOWITT_API_KEY = os.environ["ECOWITT_API_KEY"]
ECOWITT_MAC = os.environ["ECOWITT_MAC"]  # en tu caso funcionó con ":" y code/msg=0

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

    for attempt in range(1, 5):
        try:
            r = requests.get(url, params=params, timeout=(10, 90))
            r.raise_for_status()
            return r.json()
        except Exception as e:
            print(f"Ecowitt request failed (attempt {attempt}/4): {repr(e)}", flush=True)
            time.sleep(5 * attempt)

    print("Ecowitt unreachable after retries, skipping this run.", flush=True)
    return {}


def _unwrap(v: Any) -> Any:
    if isinstance(v, dict):
        if "value" in v:
            return v.get("value")
        if "val" in v:
            return v.get("val")
    return v


def _to_float(v: Any) -> Optional[float]:
    try:
        if v is None:
            return None
        return float(v)
    except Exception:
        return None


def normalize_ecowitt_data(raw_data: Any) -> Dict[str, Any]:
    # en tu cuenta normalmente llega como dict
    if isinstance(raw_data, dict):
        return raw_data

    # si llega como list, intentamos convertir a dict
    out: Dict[str, Any] = {}
    if isinstance(raw_data, list):
        for item in raw_data:
            if isinstance(item, dict) and isinstance(item.get("key"), str):
                out[item["key"]] = item
        if out:
            return out
        for item in raw_data:
            if isinstance(item, dict) and len(item) == 1:
                k, v = next(iter(item.items()))
                if isinstance(k, str):
                    out[k] = v
        return out

    return {}


def pick_path(data: Any, *path: str) -> Any:
    cur = data
    for p in path:
        if not isinstance(cur, dict) or p not in cur:
            return None
        cur = cur[p]
    return _unwrap(cur)


def first_found(*vals: Any) -> Any:
    for v in vals:
        if v is not None:
            return v
    return None


def debug_block(data: Dict[str, Any], key: str) -> None:
    v = data.get(key)
    print(f"BLOCK {key} type:", type(v), flush=True)

    if isinstance(v, dict):
        keys = list(v.keys())
        print(f"BLOCK {key} keys(sample):", keys[:50], flush=True)
        # muestra 2 ejemplos
        for kk in keys[:2]:
            print(f"BLOCK {key} example {kk} =", v.get(kk), flush=True)

    elif isinstance(v, list):
        print(f"BLOCK {key} len:", len(v), flush=True)
        print(f"BLOCK {key} first item:", (v[0] if len(v) > 0 else None), flush=True)
        print(f"BLOCK {key} first 3:", v[:3], flush=True)

    else:
        print(f"BLOCK {key} value:", v, flush=True)


def main() -> None:
    print("START ecowitt_to_grafana", flush=True)
    print(f"Remote write username(first6): {GRAFANA_RW_USERNAME[:6]}", flush=True)

    payload = fetch_ecowitt_realtime()
    if not payload:
        print("No payload from Ecowitt, exiting without exporting.", flush=True)
        return

    print("Ecowitt code/msg:", payload.get("code"), payload.get("msg"), flush=True)

    raw_data = payload.get("data")
    print("raw_data type:", type(raw_data), flush=True)

    data = normalize_ecowitt_data(raw_data)
    print("TOP keys:", list(data.keys())[:30], flush=True)

    # DEBUG infalible de bloques
    for k in ["outdoor", "wind", "pressure", "rainfall", "indoor", "solar_and_uvi", "battery"]:
        if k in data:
            debug_block(data, k)

    # --- exporter remote_write ---
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

    # Intento inicial (se ajustará con lo que veamos en el DEBUG)
    temp = _to_float(first_found(
        pick_path(data, "outdoor", "temperature"),
        pick_path(data, "outdoor", "temp"),
    ))
    hum = _to_float(first_found(
        pick_path(data, "outdoor", "humidity"),
        pick_path(data, "outdoor", "humi"),
    ))
    press = _to_float(first_found(
        pick_path(data, "pressure", "relative"),
        pick_path(data, "pressure", "abs"),
        pick_path(data, "pressure", "absolute"),
    ))
    wind = _to_float(first_found(
        pick_path(data, "wind", "wind_speed"),
        pick_path(data, "wind", "speed"),
    ))
    gust = _to_float(first_found(
        pick_path(data, "wind", "wind_gust"),
        pick_path(data, "wind", "gust"),
    ))
    rainrate = _to_float(first_found(
        pick_path(data, "rainfall", "rain_rate"),
        pick_path(data, "rainfall", "rainrate"),
        pick_path(data, "rainfall", "rate"),
    ))

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

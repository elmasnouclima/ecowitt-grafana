import os
import time
import requests
from typing import Any, Dict, Optional

from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.exporter.prometheus_remote_write import PrometheusRemoteWriteMetricsExporter


# --- ENV (GitHub Secrets) ---
ECOWITT_APP_KEY = os.environ["ECOWITT_APP_KEY"]
ECOWITT_API_KEY = os.environ["ECOWITT_API_KEY"]
ECOWITT_MAC = os.environ["ECOWITT_MAC"]  # puede ir con ":" si así te funciona

GRAFANA_RW_URL = os.environ["GRAFANA_RW_URL"]
GRAFANA_RW_USERNAME = os.environ["GRAFANA_RW_USERNAME"]
GRAFANA_RW_PASSWORD = os.environ["GRAFANA_RW_PASSWORD"]


def fetch_ecowitt_realtime() -> Dict[str, Any]:
    """
    Devuelve JSON de Ecowitt o {} si falla tras reintentos (para que el workflow no se ponga rojo).
    """
    url = "https://api.ecowitt.net/api/v3/device/real_time"
    params = {
        "application_key": ECOWITT_APP_KEY,
        "api_key": ECOWITT_API_KEY,
        "mac": ECOWITT_MAC,
        "call_back": "all",
    }

    for attempt in range(1, 5):  # 4 intentos
        try:
            r = requests.get(url, params=params, timeout=(10, 90))  # (connect, read)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            print(f"Ecowitt request failed (attempt {attempt}/4): {repr(e)}", flush=True)
            time.sleep(5 * attempt)

    print("Ecowitt unreachable after retries, skipping this run.", flush=True)
    return {}


def _to_float(v: Any) -> Optional[float]:
    try:
        if v is None:
            return None
        return float(v)
    except Exception:
        return None


def _unwrap(v: Any) -> Any:
    """
    Ecowitt suele dar dicts: {'value': '12.3', 'unit': 'C'}
    """
    if isinstance(v, dict):
        if "value" in v:
            return v.get("value")
        if "val" in v:
            return v.get("val")
    return v


def normalize_ecowitt_data(raw_data: Any) -> Dict[str, Any]:
    """
    Para tu caso ya sabemos que las claves son bloques:
    outdoor, indoor, wind, pressure, rainfall, etc.
    Normalmente raw_data llega ya como dict.
    Si llega como lista, intentamos convertirla.
    """
    if isinstance(raw_data, dict):
        return raw_data

    if isinstance(raw_data, list):
        out: Dict[str, Any] = {}
        # lista de dicts con key
        for item in raw_data:
            if isinstance(item, dict) and isinstance(item.get("key"), str):
                out[item["key"]] = item
        if out:
            return out
        # lista de dicts con 1 clave
        for item in raw_data:
            if isinstance(item, dict) and len(item) == 1:
                k, v = next(iter(item.items()))
                if isinstance(k, str):
                    out[k] = v
        return out

    return {}


def pick_path(data: Dict[str, Any], *path: str) -> Any:
    """
    Lee valores anidados, ej:
      pick_path(data, "outdoor", "temperature")
    """
    cur: Any = data
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


def main() -> None:
    print("START ecowitt_to_grafana", flush=True)
    print(f"Remote write username(first6): {GRAFANA_RW_USERNAME[:6]}", flush=True)

    payload = fetch_ecowitt_realtime()
    if not payload:
        print("No payload from Ecowitt, exiting without exporting.", flush=True)
        return

    print("Ecowitt code/msg:", payload.get("code"), payload.get("msg"), flush=True)

    raw_data = payload.get("data")
    data = normalize_ecowitt_data(raw_data)

    print("normalized keys sample:", list(data.keys())[:20], flush=True)

    # --- DEBUG corto: muestra subclaves de los bloques principales (para ajustar nombres) ---
    for k in ["outdoor", "wind", "pressure", "rainfall"]:
        v = data.get(k)
        if isinstance(v, dict):
            print(f"SUBKEYS {k}:", list(v.keys())[:50], flush=True)

    # --- Exporter remote_write ---
    exporter = PrometheusRemoteWriteMetricsExporter(
        endpoint=GRAFANA_RW_URL,
        basic_auth={"username": GRAFANA_RW_USERNAME, "password": GRAFANA_RW_PASSWORD},
        headers={},
    )
    reader = PeriodicExportingMetricReader(exporter, export_interval_millis=1000)
    provider = MeterProvider(metric_readers=[reader])
    meter = provider.get_meter("ecowitt")

    # --- gauges ---
    g_temp_c = meter.create_gauge("ecowitt_temperature_c", unit="C")
    g_hum_pct = meter.create_gauge("ecowitt_humidity_pct", unit="%")
    g_press_hpa = meter.create_gauge("ecowitt_pressure_hpa", unit="hPa")
    g_wind_ms = meter.create_gauge("ecowitt_wind_speed_ms", unit="m/s")
    g_gust_ms = meter.create_gauge("ecowitt_wind_gust_ms", unit="m/s")
    g_rainrate_mm = meter.create_gauge("ecowitt_rain_rate_mm", unit="mm")

    labels = {"station_mac": ECOWITT_MAC}

    # --- Lectura anidada (probamos varios nombres típicos por bloque) ---
    temp = _to_float(
        first_found(
            pick_path(data, "outdoor", "temperature"),
            pick_path(data, "outdoor", "temp"),
            pick_path(data, "outdoor", "temp_c"),
        )
    )
    hum = _to_float(
        first_found(
            pick_path(data, "outdoor", "humidity"),
            pick_path(data, "outdoor", "humi"),
        )
    )

    press = _to_float(
        first_found(
            pick_path(data, "pressure", "relative"),
            pick_path(data, "pressure", "rel"),
            pick_path(data, "pressure", "absolute"),
            pick_path(data, "pressure", "abs"),
        )
    )

    wind = _to_float(
        first_found(
            pick_path(data, "wind", "wind_speed"),
            pick_path(data, "wind", "speed"),
            pick_path(data, "wind", "windspeed"),
        )
    )
    gust = _to_float(
        first_found(
            pick_path(data, "wind", "wind_gust"),
            pick_path(data, "wind", "gust"),
            pick_path(data, "wind", "windgust"),
        )
    )

    rainrate = _to_float(
        first_found(
            pick_path(data, "rainfall", "rain_rate"),
            pick_path(data, "rainfall", "rainrate"),
            pick_path(data, "rainfall", "rate"),
        )
    )

    print(f"Values: temp={temp} hum={hum} press={press} wind={wind} gust={gust} rainrate={rainrate}", flush=True)

    # --- set metrics si hay valores ---
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

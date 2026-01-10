import os
import time
import requests
from typing import Any, Dict, Optional, Iterable

from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.exporter.prometheus_remote_write import PrometheusRemoteWriteMetricsExporter


# --- ENV (GitHub Secrets) ---
ECOWITT_APP_KEY = os.environ["ECOWITT_APP_KEY"]
ECOWITT_API_KEY = os.environ["ECOWITT_API_KEY"]
ECOWITT_MAC = os.environ["ECOWITT_MAC"]  # en tu caso funciona con ":" y code/msg=0

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


def _unwrap(v: Any) -> Any:
    """
    Ecowitt suele dar dicts tipo {'value': '12.3', 'unit': 'C'}.
    """
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
    """
    En tu caso raw_data ya viene como dict con bloques:
      outdoor, wind, pressure, rainfall, ...
    """
    if isinstance(raw_data, dict):
        return raw_data

    # fallback si alguna vez viniera como lista
    if isinstance(raw_data, list):
        out: Dict[str, Any] = {}
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


def debug_block(data: Dict[str, Any], block: str) -> None:
    v = data.get(block)
    print(f"BLOCK {block} type:", type(v), flush=True)
    if isinstance(v, dict):
        keys = list(v.keys())
        print(f"SUBKEYS {block}:", keys[:80], flush=True)
        # muestra 3 ejemplos
        for kk in keys[:3]:
            print(f"EXAMPLE {block}.{kk} =", v.get(kk), flush=True)
    else:
        print(f"BLOCK {block} value:", v, flush=True)


def iter_numeric_fields(d: Dict[str, Any]) -> Iterable[tuple[str, float]]:
    """
    Recorre un dict y devuelve pares (key_lower, float_value) para valores numéricos,
    incluso si vienen como {'value': '...'}.
    """
    for k, v in d.items():
        vv = _unwrap(v)
        f = _to_float(vv)
        if f is not None:
            yield (str(k).lower(), f)


def pick_by_keywords(block: Any, keywords: Iterable[str]) -> Optional[float]:
    """
    Busca el primer campo numérico cuyo nombre contenga alguna keyword.
    """
    if not isinstance(block, dict):
        return None

    kws = [k.lower() for k in keywords]
    for key_lower, f in iter_numeric_fields(block):
        if any(kw in key_lower for kw in kws):
            return f
    return None


def main() -> None:
    print("START ecowitt_to_grafana", flush=True)
    print(f"Remote write username(first6): {GRAFANA_RW_USERNAME[:6]}", flush=True)

    payload = fetch_ecowitt_realtime()
    if not payload:
        print("No payload from Ecowitt, exiting without exporting.", flush=True)
        return

    print("Ecowitt payload keys:", list(payload.keys()), flush=True)
    print("Ecowitt code/msg:", payload.get("code"), payload.get("msg"), flush=True)

    raw_data = payload.get("data")
    print("raw_data type:", type(raw_data), flush=True)

    data = normalize_ecowitt_data(raw_data)
    print("TOP keys:", list(data.keys())[:30], flush=True)

    # --- DEBUG de bloques (para que veas cómo se llaman realmente los campos) ---
    for b in ["outdoor", "wind", "pressure", "rainfall"]:
        if b in data:
            debug_block(data, b)

    # --- Extraer valores de forma robusta por keywords ---
    outdoor = data.get("outdoor")
    wind_block = data.get("wind")
    press_block = data.get("pressure")
    rain_block = data.get("rainfall")

    # temperatura / humedad exterior
    temp = pick_by_keywords(outdoor, ["temp", "temperature", "out_temp", "outdoor_temp"])
    hum = pick_by_keywords(outdoor, ["hum", "humidity", "humi"])

    # presión (relativa o absoluta)
    press = pick_by_keywords(press_block, ["rel", "relative", "baromrel", "abs", "absolute", "baromabs", "press", "pressure"])

    # viento y racha
    wind = pick_by_keywords(wind_block, ["wind_speed", "windspeed", "speed", "avg", "wind"])
    gust = pick_by_keywords(wind_block, ["gust", "wind_gust", "windgust", "max"])

    # lluvia instantánea (rain rate)
    rainrate = pick_by_keywords(rain_block, ["rain_rate", "rainrate", "rate"])

    print(f"Values: temp={temp} hum={hum} press={press} wind={wind} gust={gust} rainrate={rainrate}", flush=True)

    # --- Export a Grafana Cloud remote_write ---
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

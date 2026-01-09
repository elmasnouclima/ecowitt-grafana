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
        if v is None:
            return None
        return float(v)
    except Exception:
        return None


def _unwrap_value(v: Any) -> Any:
    if isinstance(v, dict):
        if "value" in v:
            return v.get("value")
        if "val" in v:
            return v.get("val")
    return v


def _flatten_pairs_list(raw: List[Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for item in raw:
        if isinstance(item, (list, tuple)) and len(item) >= 2 and isinstance(item[0], str):
            out[item[0]] = item[1]
    return out


def normalize_ecowitt_data(raw_data: Any) -> Dict[str, Any]:
    if isinstance(raw_data, dict):
        return raw_data

    if isinstance(raw_data, list):
        # A) lista de pares
        out = _flatten_pairs_list(raw_data)
        if out:
            return out

        # B) lista de dicts con key/name/field
        out2: Dict[str, Any] = {}
        for item in raw_data:
            if not isinstance(item, dict):
                continue
            k = item.get("key") or item.get("name") or item.get("field")
            if isinstance(k, str) and k:
                out2[k] = item
        if out2:
            return out2

        # C) lista de dicts con una sola clave: {"temp": {...}}
        out3: Dict[str, Any] = {}
        for item in raw_data:
            if not isinstance(item, dict):
                continue
            if len(item) == 1:
                k, v = next(iter(item.items()))
                if isinstance(k, str):
                    out3[k] = v
        if out3:
            return out3

        return {}

    return {}


def pick(data: Dict[str, Any], *names: str) -> Any:
    for n in names:
        if n in data:
            return _unwrap_value(data[n])
    return None


def main() -> None:
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
    raw_data = payload.get("data") if isinstance(payload, dict) else None

    print("Ecowitt payload keys:", list(payload.keys()) if isinstance(payload, dict) else type(payload), flush=True)
    print("Ecowitt code/msg:", payload.get("code"), payload.get("msg"), flush=True)
    print("raw_data type:", type(raw_data), flush=True)

    if isinstance(raw_data, list):
        print("raw_data len:", len(raw_data), flush=True)
        print("raw_data first item:", (raw_data[0] if len(raw_data) > 0 else None), flush=True)
        print("raw_data first 5:", raw_data[:5], flush=True)

    data = normalize_ecowitt_data(raw_data)
    print("normalized keys sample:", list(data.keys())[:30], flush=True)

    labels = {"station_mac": ECOWITT_MAC}

    temp = _to_float(pick(data, "temp", "outdoor_temperature", "temp_out", "temperature", "outtemp"))
    hum = _to_float(pick(data, "humidity", "outdoor_humidity", "humi_out", "humi", "outhumidity"))
    press = _to_float(pick(data, "baromabs", "baromrel", "pressure", "press", "barometer"))
    wind = _to_float(pick(data, "windspeed", "wind_speed", "wind", "windspd", "windSpeed"))
    gust = _to_float(pick(data, "windgust", "gustspeed", "wind_gust", "gust", "windGust"))
    rainrate = _to_float(pick(data, "rainrate", "rain_rate", "rain_rate_piezo", "rainRate"))

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

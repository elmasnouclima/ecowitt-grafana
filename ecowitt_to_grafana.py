import os
import time
import requests
from typing import Any, Dict, Optional

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

    for attempt in range(1, 5):  # 4 intentos
        try:
            r = requests.get(url, params=params, timeout=(10, 90))
            r.raise_for_status()
            return r.json()
        except Exception as e:
            print(f"Ecowitt request failed (attempt {attempt}/4): {repr(e)}", flush=True)
            time.sleep(5 * attempt)

    print("Ecowitt unreachable after retries, skipping this run.", flush=True)
    return {}


def unwrap_value_unit(v: Any) -> tuple[Optional[float], Optional[str]]:
    """
    Espera dicts tipo {'value': '50.7', 'unit': 'ºF'} o valores numéricos.
    """
    if isinstance(v, dict):
        val = v.get("value")
        unit = v.get("unit")
        try:
            f = float(val) if val is not None else None
        except Exception:
            f = None
        return f, (str(unit) if unit is not None else None)

    try:
        return float(v), None
    except Exception:
        return None, None


def get_path(data: Dict[str, Any], *path: str) -> Any:
    cur: Any = data
    for p in path:
        if not isinstance(cur, dict) or p not in cur:
            return None
        cur = cur[p]
    return cur


def f_to_c(f: float) -> float:
    return (f - 32.0) * 5.0 / 9.0


def mph_to_ms(mph: float) -> float:
    return mph * 0.44704


def inhg_to_hpa(inhg: float) -> float:
    return inhg * 33.8638866667


def in_per_hr_to_mm_per_hr(inph: float) -> float:
    return inph * 25.4


def main() -> None:
    print("START ecowitt_to_grafana", flush=True)

    payload = fetch_ecowitt_realtime()
    if not payload:
        print("No payload from Ecowitt, exiting without exporting.", flush=True)
        return

    print("Ecowitt code/msg:", payload.get("code"), payload.get("msg"), flush=True)

    raw_data = payload.get("data")
    if not isinstance(raw_data, dict):
        print("Unexpected data type:", type(raw_data), flush=True)
        return

    # --- Leer valores (según tus SUBKEYS reales) ---
    # Outdoor
    temp_v, temp_u = unwrap_value_unit(get_path(raw_data, "outdoor", "temperature"))
    hum_v, hum_u = unwrap_value_unit(get_path(raw_data, "outdoor", "humidity"))

    # Wind
    wind_v, wind_u = unwrap_value_unit(get_path(raw_data, "wind", "wind_speed"))
    gust_v, gust_u = unwrap_value_unit(get_path(raw_data, "wind", "wind_gust"))

    # Pressure (usamos relative)
    press_v, press_u = unwrap_value_unit(get_path(raw_data, "pressure", "relative"))

    # Rain rate
    rainrate_v, rainrate_u = unwrap_value_unit(get_path(raw_data, "rainfall", "rain_rate"))

    # --- Convertir a unidades métricas según unit ---
    temp_c: Optional[float] = None
    if temp_v is not None:
        if temp_u and ("f" in temp_u.lower()):
            temp_c = f_to_c(temp_v)
        else:
            # si ya viniera en C (o sin unidad), asumimos que es C
            temp_c = temp_v

    hum_pct: Optional[float] = hum_v  # normalmente ya es %
    press_hpa: Optional[float] = None
    if press_v is not None:
        if press_u and ("inhg" in press_u.lower()):
            press_hpa = inhg_to_hpa(press_v)
        else:
            # si algún día viniera ya en hPa, lo dejamos
            press_hpa = press_v

    wind_ms: Optional[float] = None
    if wind_v is not None:
        if wind_u and ("mph" in wind_u.lower()):
            wind_ms = mph_to_ms(wind_v)
        else:
            wind_ms = wind_v

    gust_ms: Optional[float] = None
    if gust_v is not None:
        if gust_u and ("mph" in gust_u.lower()):
            gust_ms = mph_to_ms(gust_v)
        else:
            gust_ms = gust_v

    rainrate_mm: Optional[float] = None
    if rainrate_v is not None:
        if rainrate_u and ("in/hr" in rainrate_u.lower() or "in" in rainrate_u.lower()):
            # rain_rate viene en in/hr en tu caso
            rainrate_mm = in_per_hr_to_mm_per_hr(rainrate_v)
        else:
            rainrate_mm = rainrate_v

    print(
        f"Values(metric): temp_c={temp_c} hum_pct={hum_pct} press_hpa={press_hpa} "
        f"wind_ms={wind_ms} gust_ms={gust_ms} rain_rate_mmph={rainrate_mm}",
        flush=True,
    )

    # --- Exporter remote_write ---
    exporter = PrometheusRemoteWriteMetricsExporter(
        endpoint=GRAFANA_RW_URL,
        basic_auth={"username": GRAFANA_RW_USERNAME, "password": GRAFANA_RW_PASSWORD},
        headers={},
    )
    reader = PeriodicExportingMetricReader(exporter, export_interval_millis=1000)
    provider = MeterProvider(metric_readers=[reader])
    meter = provider.get_meter("ecowitt")

    # --- gauges en métrico ---
    g_temp_c = meter.create_gauge("ecowitt_temperature_c", unit="C")
    g_hum_pct = meter.create_gauge("ecowitt_humidity_pct", unit="%")
    g_press_hpa = meter.create_gauge("ecowitt_pressure_hpa", unit="hPa")
    g_wind_ms = meter.create_gauge("ecowitt_wind_speed_ms", unit="m/s")
    g_gust_ms = meter.create_gauge("ecowitt_wind_gust_ms", unit="m/s")
    g_rainrate_mm = meter.create_gauge("ecowitt_rain_rate_mm", unit="mm/h")

    labels = {"station_mac": ECOWITT_MAC}

    if temp_c is not None:
        g_temp_c.set(temp_c, labels)
    if hum_pct is not None:
        g_hum_pct.set(hum_pct, labels)
    if press_hpa is not None:
        g_press_hpa.set(press_hpa, labels)
    if wind_ms is not None:
        g_wind_ms.set(wind_ms, labels)
    if gust_ms is not None:
        g_gust_ms.set(gust_ms, labels)
    if rainrate_mm is not None:
        g_rainrate_mm.set(rainrate_mm, labels)

    provider.force_flush()
    print("force_flush() OK", flush=True)

    time.sleep(10)
    provider.shutdown()
    print("DONE", flush=True)


if __name__ == "__main__":
    main()

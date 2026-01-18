import os
import time
import requests
from typing import Any, Dict, Optional, Tuple

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
    for attempt in range(1, 5):
        try:
            r = requests.get(url, params=params, timeout=(10, 90))
            r.raise_for_status()
            return r.json()
        except Exception as e:
            print(f"Ecowitt request failed (attempt {attempt}/4): {e}", flush=True)
            time.sleep(5 * attempt)
    return {}


def unwrap(v: Any) -> Tuple[Optional[float], Optional[str]]:
    if isinstance(v, dict):
        val = v.get("value")
        unit = v.get("unit")
        try:
            return (float(val), str(unit) if unit is not None else None)
        except Exception:
            return (None, str(unit) if unit is not None else None)
    try:
        return (float(v), None)
    except Exception:
        return (None, None)


def get_path(d: Dict[str, Any], *path: str) -> Any:
    cur: Any = d
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
    print("RW url:", GRAFANA_RW_URL, flush=True)
    print("RW user(first6):", (GRAFANA_RW_USERNAME[:6] if GRAFANA_RW_USERNAME else ""), flush=True)

    exporter = PrometheusRemoteWriteMetricsExporter(
        endpoint=GRAFANA_RW_URL,
        basic_auth={"username": GRAFANA_RW_USERNAME, "password": GRAFANA_RW_PASSWORD},
        headers={},
    )
    reader = PeriodicExportingMetricReader(exporter, export_interval_millis=1000)
    provider = MeterProvider(metric_readers=[reader])
    meter = provider.get_meter("ecowitt")

    labels = {"station_mac": ECOWITT_MAC}

    # ✅ MÉTRICA TEST (siempre debe aparecer)
    g_test = meter.create_gauge("ecowitt_test_one", unit="1")
    g_test.set(1.0, labels)

    payload = fetch_ecowitt_realtime()
    if not payload:
        print("No payload from Ecowitt (but test metric should still be sent).", flush=True)
    else:
        raw = payload.get("data", {})
        print("Ecowitt code/msg:", payload.get("code"), payload.get("msg"), flush=True)

        temp_v, temp_u = unwrap(get_path(raw, "outdoor", "temperature"))
        hum_v, _ = unwrap(get_path(raw, "outdoor", "humidity"))
        wind_v, wind_u = unwrap(get_path(raw, "wind", "wind_speed"))
        gust_v, gust_u = unwrap(get_path(raw, "wind", "wind_gust"))
        press_v, press_u = unwrap(get_path(raw, "pressure", "relative"))
        rain_v, rain_u = unwrap(get_path(raw, "rainfall", "rain_rate"))

        temp_c = f_to_c(temp_v) if (temp_v is not None and temp_u and "f" in temp_u.lower()) else temp_v
        wind_ms = mph_to_ms(wind_v) if (wind_v is not None and wind_u and "mph" in wind_u.lower()) else wind_v
        gust_ms = mph_to_ms(gust_v) if (gust_v is not None and gust_u and "mph" in gust_u.lower()) else gust_v
        press_hpa = inhg_to_hpa(press_v) if (press_v is not None and press_u and "inhg" in press_u.lower()) else press_v
        rain_mmph = in_per_hr_to_mm_per_hr(rain_v) if (rain_v is not None and rain_u and "in/hr" in rain_u.lower()) else rain_v

        print(
            f"Values(metric): temp_c={temp_c} hum={hum_v} press_hpa={press_hpa} wind_ms={wind_ms} gust_ms={gust_ms} rain_mmph={rain_mmph}",
            flush=True,
        )

        g_temp = meter.create_gauge("ecowitt_temperature_c", unit="C")
        g_hum = meter.create_gauge("ecowitt_humidity_pct", unit="%")
        g_press = meter.create_gauge("ecowitt_pressure_hpa", unit="hPa")
        g_wind = meter.create_gauge("ecowitt_wind_speed_ms", unit="m/s")
        g_gust = meter.create_gauge("ecowitt_wind_gust_ms", unit="m/s")
        g_rain = meter.create_gauge("ecowitt_rain_rate_mm", unit="mm/h")

        if temp_c is not None: g_temp.set(temp_c, labels)
        if hum_v is not None: g_hum.set(hum_v, labels)
        if press_hpa is not None: g_press.set(press_hpa, labels)
        if wind_ms is not None: g_wind.set(wind_ms, labels)
        if gust_ms is not None: g_gust.set(gust_ms, labels)
        if rain_mmph is not None: g_rain.set(rain_mmph, labels)

    provider.force_flush()
    print("force_flush() OK", flush=True)

    # deja tiempo para exportar
    time.sleep(20)
    provider.shutdown()
    print("DONE", flush=True)


if __name__ == "__main__":
    main()

import os
import time
import requests

from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.exporter.prometheus_remote_write import PrometheusRemoteWriteMetricsExporter


# --- ENV (GitHub Secrets) ---
ECOWITT_APP_KEY = os.environ["ECOWITT_APP_KEY"]
ECOWITT_API_KEY = os.environ["ECOWITT_API_KEY"]
ECOWITT_MAC = os.environ["ECOWITT_MAC"]

GRAFANA_RW_URL = os.environ["GRAFANA_RW_URL"]            # https://.../api/prom/push
GRAFANA_RW_USERNAME = os.environ["GRAFANA_RW_USERNAME"]  # normalmente tu "slug" (ej: fmp14352)
GRAFANA_RW_PASSWORD = os.environ["GRAFANA_RW_PASSWORD"]  # token (Access Policy)


def fetch_ecowitt_realtime() -> dict:
    # Ecowitt API v3: real_time
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


def _pick(data: dict, *names):
    """
    Ecowitt a veces devuelve:
      key: {"value":"12.3","unit":"C"}
    o directamente:
      key: "12.3"
    Aquí probamos varias claves posibles y devolvemos el primer valor encontrado.
    """
    for name in names:
        if name not in data:
            continue
        v = data.get(name)
        if isinstance(v, dict):
            v = v.get("value")
        if v is not None:
            return v
    return None


def main():
    # Exporter: Prometheus remote_write (Grafana Cloud)
    exporter = PrometheusRemoteWriteMetricsExporter(
        endpoint=GRAFANA_RW_URL,
        username=GRAFANA_RW_USERNAME,
        password=GRAFANA_RW_PASSWORD,
    )

    reader = PeriodicExportingMetricReader(exporter, export_interval_millis=1000)
    provider = MeterProvider(metric_readers=[reader])
    meter = provider.get_meter("ecowitt")

    # Métricas típicas (gauges)
    g_temp_c = meter.create_gauge("ecowitt_temperature_c", unit="C")
    g_hum_pct = meter.create_gauge("ecowitt_humidity_pct", unit="%")
    g_press_hpa = meter.create_gauge("ecowitt_pressure_hpa", unit="hPa")
    g_wind_ms = meter.create_gauge("ecowitt_wind_speed_ms", unit="m/s")
    g_gust_ms = meter.create_gauge("ecowitt_wind_gust_ms", unit="m/s")
    g_rainrate_mm = meter.create_gauge("ecowitt_rain_rate_mm", unit="mm")

    payload = fetch_ecowitt_realtime()
    data = payload.get("data", {}) if isinstance(payload, dict) else {}

    # Etiquetas (labels) para filtrar por estación
    labels = {"station_mac": ECOWITT_MAC}

    # Intentamos varias claves porque Ecowitt puede nombrarlas distinto según sensores
    temp = _to_float(_pick(data, "temp", "outdoor_temperature", "tempin", "temp_out"))
    hum = _to_float(_pick(data, "humidity", "outdoor_humidity", "humidityin", "humi_out"))
    press = _to_float(_pick(data, "baromabs", "baromrel", "pressure", "press"))
    wind = _to_float(_pick(data, "windspeed", "wind_speed", "wind"))
    gust = _to_float(_pick(data, "windgust", "gustspeed", "wind_gust"))
    rainrate = _to_float(_pick(data, "rainrate", "rain_rate", "rain_rate_piezo"))

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

    # Espera breve para que exporte
    time.sleep(2)
    provider.shutdown()


if __name__ == "__main__":
    main()

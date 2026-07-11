"""
Capa de EXTRACCIÓN.

Responsabilidad única: hablar con la API de Frankfurter y devolver un
DataFrame de PySpark con schema tipado y datos crudos (sin transformar).

Decisiones de diseño:
- Se usa el endpoint de rango de fechas (`/v1/{start}..{end}`) en una sola
  llamada en vez de iterar día por día: menos requests, menos probabilidad
  de rate limiting, y la API ya resuelve fines de semana/festivos
  devolviendo solo días hábiles.
- Reintentos con backoff exponencial vía `tenacity`, sólo sobre errores
  transitorios (timeout, 5xx, errores de conexión). Un 4xx (ej. rango de
  fechas inválido) no se reintenta porque no se va a resolver solo.
- La función de parseo es pura (JSON -> DataFrame) y está separada de la
  función de red, para poder testearla sin hacer llamadas HTTP reales.
"""
from datetime import date, datetime, timezone

import requests
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from src.common import get_logger, load_config

logger = get_logger(__name__)

RAW_SCHEMA = StructType(
    [
        StructField("date", StringType(), False),
        StructField("currency", StringType(), False),
        StructField("rate", DoubleType(), True),
        StructField("base_currency", StringType(), False),
        StructField("ingestion_timestamp", TimestampType(), False),
    ]
)


class FrankfurterAPIError(Exception):
    """Error no recuperable al hablar con la API (ej. 4xx, respuesta inválida)."""


def _retryable_http_error(exc: BaseException) -> bool:
    """Sólo reintentamos timeouts, errores de conexión y 5xx/429."""
    if isinstance(exc, (requests.exceptions.Timeout, requests.exceptions.ConnectionError)):
        return True
    if isinstance(exc, requests.exceptions.HTTPError):
        status = exc.response.status_code if exc.response is not None else None
        return status is not None and (status == 429 or status >= 500)
    return False


def _build_retry_decorator(cfg: dict):
    api_cfg = cfg["api"]
    return retry(
        retry=retry_if_exception_type(requests.exceptions.RequestException)
        & retry_if_exception_type((requests.exceptions.Timeout, requests.exceptions.ConnectionError, requests.exceptions.HTTPError)),
        stop=stop_after_attempt(api_cfg["max_retries"]),
        wait=wait_exponential(multiplier=api_cfg["backoff_base_seconds"], min=1, max=30),
        reraise=True,
    )


def fetch_range(start_date: str, end_date: str, currencies: list[str]) -> dict:
    """
    Llama a GET /v1/{start}..{end}?base=USD&symbols=... con reintentos.
    Devuelve el JSON crudo tal como lo entrega la API.
    """
    cfg = load_config()
    api_cfg = cfg["api"]
    url = f"{api_cfg['base_url']}/{start_date}..{end_date}"
    params = {"base": api_cfg["base_currency"], "symbols": ",".join(currencies)}

    @_build_retry_decorator(cfg)
    def _do_request():
        logger.info(f"GET {url} params={params}")
        resp = requests.get(url, params=params, timeout=api_cfg["timeout_seconds"])
        # Lanza HTTPError para 4xx/5xx, lo captura nuestro filtro de retry
        resp.raise_for_status()
        return resp.json()

    try:
        return _do_request()
    except requests.exceptions.HTTPError as e:
        status = e.response.status_code if e.response is not None else "unknown"
        raise FrankfurterAPIError(
            f"Error HTTP {status} al consultar Frankfurter API tras reintentos: {e}"
        ) from e
    except requests.exceptions.RequestException as e:
        raise FrankfurterAPIError(
            f"Fallo de red al consultar Frankfurter API tras reintentos: {e}"
        ) from e


def raw_json_to_rows(raw: dict) -> list[dict]:
    """
    Aplana la respuesta anidada de Frankfurter:
      { "rates": { "2024-01-02": { "MXN": 17.06, "EUR": 0.91 }, ... } }
    a filas planas (date, currency, rate). No incluye fines de semana ni
    festivos porque la API simplemente no los devuelve — eso se maneja aquí
    por omisión, no como error, y se documenta en el reporte de calidad
    (capa transform) donde se distingue "día no hábil" de "dato faltante".
    """
    base_currency = raw.get("base", "USD")
    rates_by_date = raw.get("rates", {})
    now = datetime.now(timezone.utc)

    rows = []
    for day_str, currency_rates in rates_by_date.items():
        for currency, rate in currency_rates.items():
            rows.append(
                {
                    "date": day_str,
                    "currency": currency,
                    "rate": float(rate) if rate is not None else None,
                    "base_currency": base_currency,
                    "ingestion_timestamp": now,
                }
            )
    return rows


def _filter_to_configured_range(rows: list[dict], start_date: str, end_date: str) -> list[dict]:
    """
    Blindaje contra un comportamiento observado de Frankfurter API: cuando
    `start_date` cae en un día sin tasa publicada (fin de semana/festivo),
    la API puede "anclar" al día hábil más cercano DISPONIBLE, que puede
    quedar antes del rango pedido (ej. pedir desde 2024-01-01 —feriado—
    puede devolver también 2023-12-29, el último día hábil de 2023).

    Se filtra explícitamente aquí en vez de confiar en que la API respete
    el rango exacto solicitado — comparación de strings es válida porque
    las fechas vienen en formato ISO 8601 (yyyy-MM-dd), que ordena
    lexicográficamente igual que cronológicamente.
    """
    filtered = [r for r in rows if start_date <= r["date"] <= end_date]
    dropped = len(rows) - len(filtered)
    if dropped > 0:
        dropped_dates = sorted({r["date"] for r in rows if not (start_date <= r["date"] <= end_date)})
        logger.warning(
            f"{dropped} filas fuera del rango configurado ({start_date} a {end_date}) "
            f"descartadas — la API devolvió fechas ancla fuera de rango: {dropped_dates}."
        )
    return filtered


def extract_exchange_rates(spark: SparkSession) -> DataFrame:
    """Punto de entrada de la capa de extracción: red -> DataFrame tipado."""
    cfg = load_config()
    api_cfg = cfg["api"]

    raw = fetch_range(api_cfg["start_date"], api_cfg["end_date"], api_cfg["target_currencies"])
    rows = raw_json_to_rows(raw)
    rows = _filter_to_configured_range(rows, api_cfg["start_date"], api_cfg["end_date"])

    if not rows:
        raise FrankfurterAPIError(
            "La API no devolvió tasas para el rango/monedas solicitados. "
            "Verifica start_date/end_date/target_currencies en config/settings.yaml."
        )

    logger.info(f"Extraídas {len(rows)} filas crudas ({api_cfg['start_date']} a {api_cfg['end_date']}).")

    df = spark.createDataFrame(rows, schema=RAW_SCHEMA)
    return df

"""
Capa de MODELADO.

Modelo dimensional simple (esquema estrella) inspirado en un data mart de
riesgo/tesorería, que es el consumo típico de este dato en una fintech:

- fact_exchange_rates
    Grano: 1 fila = 1 tipo de cambio observado para 1 moneda en 1 fecha
    (base USD). Contiene las métricas numéricas (rate, variación, medias
    móviles, volatilidad) más la llave foránea a dim_currency.

- dim_currency
    Grano: 1 fila = 1 moneda. Dimensión de crecimiento lento (SCD tipo 1
    aquí, por simplicidad — no se justifica versionar historia de "nombre
    de moneda" para este ejercicio).

Por qué este grano y no uno más fino (ej. por hora) o más agregado (ej. por
semana): la fuente (Frankfurter) sólo publica una tasa por día hábil, así
que "día" es el grano natural más fino disponible sin inventar datos.
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

# Catálogo estático de metadata de monedas. En un caso real esto vendría de
# un maestro de datos (ej. tabla de referencia ISO 4217); aquí se declara
# explícitamente porque son solo 5 monedas y así se documenta la fuente.
CURRENCY_METADATA = {
    "USD": {"currency_name": "US Dollar", "region": "North America"},
    "MXN": {"currency_name": "Mexican Peso", "region": "Latin America"},
    "EUR": {"currency_name": "Euro", "region": "Europe"},
    "BRL": {"currency_name": "Brazilian Real", "region": "Latin America"},
    "COP": {"currency_name": "Colombian Peso", "region": "Latin America"},
}


def build_dim_currency(spark: SparkSession) -> DataFrame:
    rows = [
        {"currency_code": code, **meta} for code, meta in CURRENCY_METADATA.items()
    ]
    return spark.createDataFrame(rows)


def build_fact_exchange_rates(enriched_with_cdc: DataFrame) -> DataFrame:
    """
    Proyecta el dataset enriquecido + CDC a las columnas del hecho.
    Se excluyen filas DELETE del hecho "vivo" (quedan solo en el
    log de auditoría de Iceberg vía MERGE INTO / time travel).
    """
    fact = enriched_with_cdc.filter(F.col("operation_type") != "DELETE").select(
        F.col("date"),
        F.col("currency").alias("currency_code"),
        F.col("base_currency"),
        F.col("rate"),
        F.col("daily_change_pct"),
        F.col("rolling_avg_7d"),
        F.col("rolling_avg_30d"),
        F.col("rolling_volatility_30d"),
        F.col("year"),
        F.col("month"),
        F.col("operation_type"),
        F.col("row_hash"),
        F.col("ingestion_timestamp"),
        F.col("updated_at"),
    )
    return fact

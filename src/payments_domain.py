"""
DOMINIO OPCIONAL: pagos/tarjetas (sección 4.4 del enunciado — "opcional").

Simula el core de tarjetas de Konfio con tres CSVs sintéticos generados con
semilla fija (`data/customers.csv`, `cards.csv`, `transactions.csv`):

- dim_customer   — grano: 1 fila = 1 cliente (PyME)
- dim_card       — grano: 1 fila = 1 tarjeta, FK a customer_id
- fact_transactions — grano: 1 fila = 1 transacción, FK a card_id

Enriquecimiento que conecta este dominio con el pipeline principal: cada
transacción se convierte a USD usando la tasa de cambio de `db.tipos_cambio_
enriquecidos` correspondiente a la moneda de la tarjeta y la fecha de la
transacción (con forward-fill para fines de semana, igual que en
`secondary_source.py`). Esto demuestra que el modelo dimensional no es solo
una tabla aislada — se integra con el resto del lakehouse.

Decisión de diseño: estas tablas se cargan con overwrite completo en cada
corrida (igual que `metricas_mensuales`/`anomalias`), NO con MERGE INTO/CDC.
El requisito de CDC del enunciado ya está cubierto en profundidad por
`fact_exchange_rates` (la tabla obligatoria); replicar esa misma lógica
aquí sería duplicar complejidad sin una razón de negocio real — estos CSVs
no cambian entre corridas (son estáticos, generados una sola vez), así que
un MERGE INTO no tendría nada nuevo que detectar.
"""
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, DoubleType, StringType, StructField, StructType
from pyspark.sql.window import Window

from src.common import get_logger, load_config

logger = get_logger(__name__)

CUSTOMER_SCHEMA = StructType(
    [
        StructField("customer_id", StringType(), False),
        StructField("customer_name", StringType(), False),
        StructField("segment", StringType(), False),
        StructField("state", StringType(), False),
        StructField("signup_date", StringType(), False),
    ]
)

CARD_SCHEMA = StructType(
    [
        StructField("card_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("card_type", StringType(), False),
        StructField("currency", StringType(), False),
        StructField("issued_date", StringType(), False),
    ]
)

TRANSACTION_SCHEMA = StructType(
    [
        StructField("transaction_id", StringType(), False),
        StructField("card_id", StringType(), False),
        StructField("date", StringType(), False),
        StructField("amount", DoubleType(), False),
        StructField("merchant_category", StringType(), False),
    ]
)


def _read_csv(spark: SparkSession, path: str, schema: StructType) -> DataFrame:
    if not os.path.exists(path):
        raise FileNotFoundError(f"No se encontró el CSV en {path}. Verifica que data/ se copió en el Dockerfile.")
    return spark.read.option("header", "true").schema(schema).csv(path)


def extract_customers(spark: SparkSession) -> DataFrame:
    cfg = load_config()
    path = cfg["paths"]["customers_csv"]
    df = _read_csv(spark, path, CUSTOMER_SCHEMA).withColumn("signup_date", F.to_date("signup_date"))
    logger.info(f"Extraídos {df.count()} clientes desde {path}.")
    return df


def extract_cards(spark: SparkSession) -> DataFrame:
    cfg = load_config()
    path = cfg["paths"]["cards_csv"]
    df = _read_csv(spark, path, CARD_SCHEMA).withColumn("issued_date", F.to_date("issued_date"))
    logger.info(f"Extraídas {df.count()} tarjetas desde {path}.")
    return df


def extract_transactions(spark: SparkSession) -> DataFrame:
    cfg = load_config()
    path = cfg["paths"]["transactions_csv"]
    df = _read_csv(spark, path, TRANSACTION_SCHEMA).withColumn("date", F.to_date("date"))
    logger.info(f"Extraídas {df.count()} transacciones desde {path}.")
    return df


def build_dim_customer(customers_df: DataFrame) -> DataFrame:
    """Grano: 1 fila = 1 cliente. Pass-through tipado, sin transformación de negocio."""
    return customers_df


def build_dim_card(cards_df: DataFrame) -> DataFrame:
    """Grano: 1 fila = 1 tarjeta. Pass-through tipado, sin transformación de negocio."""
    return cards_df


def build_fact_transactions(transactions_df: DataFrame, cards_df: DataFrame, exchange_rates_df: DataFrame) -> DataFrame:
    """
    Grano de salida: 1 fila = 1 transacción, enriquecida con:
    - currency de la tarjeta (vía join con dim_card)
    - amount_usd: monto convertido a USD usando la tasa del día (o la
      última tasa hábil conocida, forward-fill) de la moneda de la tarjeta.
      Transacciones ya en USD no se convierten (rate = 1.0 implícito).
    """
    txn_with_currency = transactions_df.join(
        cards_df.select("card_id", "currency"), on="card_id", how="left"
    )

    # Tasa por (fecha, moneda) — mismo patrón de forward-fill que en
    # secondary_source.py, para cubrir fines de semana en las transacciones.
    non_usd_currencies = [r["currency"] for r in txn_with_currency.select("currency").distinct().collect() if r["currency"] != "USD"]

    rates = exchange_rates_df.filter(F.col("currency").isin(non_usd_currencies)).select(
        "date", "currency", F.col("rate").alias("fx_rate")
    )

    all_dates = txn_with_currency.select("date").distinct()
    calendar = all_dates.crossJoin(
        exchange_rates_df.select("currency").distinct().filter(F.col("currency").isin(non_usd_currencies))
    )
    calendar_rates = calendar.join(rates, on=["date", "currency"], how="left")

    w = Window.partitionBy("currency").orderBy("date").rowsBetween(Window.unboundedPreceding, 0)
    calendar_rates_filled = calendar_rates.withColumn(
        "fx_rate_filled", F.last("fx_rate", ignorenulls=True).over(w)
    ).select("date", "currency", "fx_rate_filled")

    result = txn_with_currency.join(calendar_rates_filled, on=["date", "currency"], how="left")

    result = result.withColumn(
        "amount_usd",
        F.when(F.col("currency") == "USD", F.col("amount")).otherwise(
            F.round(F.col("amount") / F.col("fx_rate_filled"), 2)
        ),
    )

    result = result.withColumn("year", F.year("date")).withColumn("month", F.month("date"))
    result = result.select(
        "transaction_id", "card_id", "date", "currency", "amount", "amount_usd", "merchant_category", "year", "month"
    )

    logger.info(f"fact_transactions construida: {result.count()} filas.")
    return result

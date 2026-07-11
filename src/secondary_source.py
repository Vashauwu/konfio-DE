"""
SEGUNDA FUENTE DE DATOS (punto extra 7.2).

Simula datos operativos internos de Konfio: volumen diario de solicitudes
de crédito, monto promedio solicitado y tasa de aprobación. Se modela como
CSV porque así llegaría un extracto de un sistema interno (core bancario /
CRM) en un escenario real de integración batch.

Por qué esta fuente y no otra API externa: el valor de negocio real está en
cruzar una métrica interna (demanda de crédito) contra una métrica externa
(volatilidad cambiaria) — eso es exactamente el tipo de pregunta que un
equipo de riesgo haría ("¿la demanda de crédito se mueve con la volatilidad
del tipo de cambio?"), y es más representativo del contexto de Konfio que
agregar una tercera API de tipos de cambio redundante.

Grano de `solicitudes_credito`: 1 fila = 1 día calendario (incluye fines de
semana, a diferencia de la fuente de tipos de cambio — una fintech puede
recibir solicitudes cualquier día, aunque se procesen en días hábiles).

Grano de la tabla de JOIN (`db.riesgo_diario`): 1 fila = 1 día calendario,
con la tasa MXN del día hábil más reciente disponible (forward-fill) para
poder unir con días donde no hay tipo de cambio (fines de semana).
"""
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType
from pyspark.sql.window import Window

from src.common import get_logger, load_config

logger = get_logger(__name__)

CSV_SCHEMA = StructType(
    [
        StructField("date", StringType(), False),
        StructField("num_solicitudes", IntegerType(), False),
        StructField("monto_promedio_mxn", DoubleType(), False),
        StructField("tasa_aprobacion_pct", DoubleType(), False),
    ]
)


def extract_loan_requests(spark: SparkSession) -> DataFrame:
    """Lee el CSV de solicitudes de crédito con schema tipado explícito."""
    cfg = load_config()
    csv_path = cfg["paths"].get("loan_requests_csv", "/app/data/solicitudes_credito.csv")

    if not os.path.exists(csv_path):
        raise FileNotFoundError(
            f"No se encontró el CSV de solicitudes de crédito en {csv_path}. "
            "Verifica que la carpeta data/ se copió en el Dockerfile."
        )

    df = (
        spark.read.option("header", "true")
        .schema(CSV_SCHEMA)
        .csv(csv_path)
        .withColumn("date", F.to_date("date", "yyyy-MM-dd"))
    )
    logger.info(f"Extraídas {df.count()} filas de solicitudes de crédito desde {csv_path}.")
    return df


def build_daily_risk_view(loan_requests_df: DataFrame, exchange_rates_df: DataFrame, target_currency: str = "MXN") -> DataFrame:
    """
    JOIN enriquecido: une el volumen diario de solicitudes de crédito con la
    tasa de cambio USD/MXN del mismo día.

    Los fines de semana no tienen tipo de cambio publicado (la fuente 1 no
    cubre esos días), así que se usa forward-fill (`last(..., ignorenulls)`
    sobre una ventana ordenada por fecha) para propagar la última tasa
    hábil conocida — es el supuesto estándar en finanzas para valorar
    posiciones en días no hábiles (el mercado no se mueve, se usa el
    último cierre).
    """
    mxn_rates = (
        exchange_rates_df.filter(F.col("currency") == target_currency)
        .select("date", F.col("rate").alias("mxn_rate"))
        .distinct()
    )

    all_dates = loan_requests_df.select("date").distinct()
    rates_calendar = all_dates.join(mxn_rates, on="date", how="left")

    w = Window.orderBy("date").rowsBetween(Window.unboundedPreceding, 0)
    rates_filled = rates_calendar.withColumn(
        "mxn_rate_filled", F.last("mxn_rate", ignorenulls=True).over(w)
    ).select("date", "mxn_rate_filled")

    joined = loan_requests_df.join(rates_filled, on="date", how="left")

    joined = joined.withColumn(
        "monto_promedio_usd",
        F.round(F.col("monto_promedio_mxn") / F.col("mxn_rate_filled"), 2),
    )
    joined = joined.withColumn("year", F.year("date")).withColumn("month", F.month("date"))

    logger.info(f"Vista de riesgo diaria construida: {joined.count()} filas (solicitudes x tipo de cambio MXN).")
    return joined

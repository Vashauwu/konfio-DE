"""
Capa de TRANSFORMACIÓN.

Cada función hace UNA cosa y recibe/devuelve DataFrames, para que sean
testeables de forma aislada (ver tests/test_transform.py) y componibles
desde main.py.

Grano de entrada/salida de esta capa: 1 fila = 1 (moneda, fecha).
"""
from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.common import get_logger, load_config

logger = get_logger(__name__)

# Rango de sanidad para tasas de cambio: evita valores absurdos por errores
# de la API o de parseo (ej. 0, negativos, o un typo tipo 17000 en vez de 17).
MIN_REASONABLE_RATE = 0.0001
MAX_REASONABLE_RATE = 10000.0


# ---------------------------------------------------------------------------
# 1. Limpieza
# ---------------------------------------------------------------------------
def clean_exchange_rates(df: DataFrame) -> DataFrame:
    """
    - Castea 'date' de string a DateType.
    - Elimina duplicados exactos (misma moneda+fecha+tasa).
    - Descarta nulos en llaves de negocio (date, currency).
    - Filtra tasas fuera de rango razonable (se registran como descartadas,
      no se imputan: inventar una tasa de cambio sería introducir riesgo en
      un contexto financiero).
    """
    before = df.count()

    df = df.withColumn("date", F.to_date("date", "yyyy-MM-dd"))

    df = df.dropDuplicates(["date", "currency", "rate"])

    df = df.filter(F.col("date").isNotNull() & F.col("currency").isNotNull())

    df = df.filter(
        F.col("rate").isNull()
        | ((F.col("rate") >= MIN_REASONABLE_RATE) & (F.col("rate") <= MAX_REASONABLE_RATE))
    )

    df = df.withColumn("currency", F.upper(F.trim(F.col("currency"))))
    df = df.withColumn("base_currency", F.upper(F.trim(F.col("base_currency"))))

    after = df.count()
    logger.info(f"Limpieza: {before} filas -> {after} filas ({before - after} descartadas/deduplicadas).")
    return df


# ---------------------------------------------------------------------------
# 2. Enriquecimiento
# ---------------------------------------------------------------------------
def enrich_exchange_rates(df: DataFrame) -> DataFrame:
    """
    Calcula, por moneda y ordenado por fecha:
    - daily_change_pct: variación % respecto al día hábil anterior
    - rolling_avg_7d / rolling_avg_30d: promedio móvil (ventana de N *observaciones*
      hábiles previas, no N días calendario, ya que la API no trae fines de
      semana/festivos — una ventana calendario dejaría huecos artificiales)
    - rolling_volatility_30d: desviación estándar móvil de 30 observaciones
    """
    w_order = Window.partitionBy("currency").orderBy("date")

    df = df.withColumn("prev_rate", F.lag("rate").over(w_order))
    df = df.withColumn(
        "daily_change_pct",
        F.when(
            F.col("prev_rate").isNotNull() & (F.col("prev_rate") != 0),
            (F.col("rate") - F.col("prev_rate")) / F.col("prev_rate") * 100.0,
        ),
    ).drop("prev_rate")

    w_7d = w_order.rowsBetween(-6, 0)
    w_30d = w_order.rowsBetween(-29, 0)

    df = df.withColumn("rolling_avg_7d", F.avg("rate").over(w_7d))
    df = df.withColumn("rolling_avg_30d", F.avg("rate").over(w_30d))
    df = df.withColumn("rolling_volatility_30d", F.stddev("rate").over(w_30d))

    df = df.withColumn("year", F.year("date")).withColumn("month", F.month("date"))

    return df


# ---------------------------------------------------------------------------
# 3. Agregaciones
# ---------------------------------------------------------------------------
def build_monthly_metrics(df: DataFrame) -> DataFrame:
    """
    Tabla resumen (db.metricas_mensuales). Grano: 1 fila = 1 (moneda, año, mes).
    """
    monthly = (
        df.groupBy("currency", "year", "month")
        .agg(
            F.avg("rate").alias("avg_rate"),
            F.min("rate").alias("min_rate"),
            F.max("rate").alias("max_rate"),
            F.stddev("rate").alias("monthly_volatility"),
            F.count("rate").alias("num_observations"),
        )
        .orderBy("currency", "year", "month")
    )
    return monthly


# ---------------------------------------------------------------------------
# 4. Detección de anomalías
# ---------------------------------------------------------------------------
def detect_anomalies(df: DataFrame, z_threshold: float = 2.0) -> DataFrame:
    """
    Marca como anomalía un día donde |daily_change_pct| excede
    z_threshold desviaciones estándar respecto al promedio móvil de 30 días
    de la propia variación diaria (no de la tasa) — así medimos "movimiento
    inusual" y no simplemente "tasa alta".

    Grano de salida: 1 fila = 1 (moneda, fecha) marcada como anómala.
    """
    w_order = Window.partitionBy("currency").orderBy("date")
    w_30d = w_order.rowsBetween(-29, 0)

    with_stats = df.withColumn(
        "change_pct_avg_30d", F.avg("daily_change_pct").over(w_30d)
    ).withColumn("change_pct_std_30d", F.stddev("daily_change_pct").over(w_30d))

    with_z = with_stats.withColumn(
        "z_score",
        F.when(
            F.col("change_pct_std_30d").isNotNull() & (F.col("change_pct_std_30d") != 0),
            (F.col("daily_change_pct") - F.col("change_pct_avg_30d")) / F.col("change_pct_std_30d"),
        ),
    )

    anomalies = with_z.filter(F.abs(F.col("z_score")) > z_threshold).select(
        "date",
        "currency",
        "rate",
        "daily_change_pct",
        "change_pct_avg_30d",
        "change_pct_std_30d",
        "z_score",
    )
    return anomalies


# ---------------------------------------------------------------------------
# 5. Calidad de datos
# ---------------------------------------------------------------------------
def build_quality_report(spark: SparkSession, df: DataFrame, start_date: str, end_date: str, currencies: list) -> DataFrame:
    """
    Compara el calendario completo del periodo contra las fechas realmente
    observadas por moneda, y clasifica cada día ausente como:
    - 'weekend'   -> sábado/domingo (esperado, no es un problema de calidad)
    - 'missing'   -> día hábil entre semana sin dato (posible falla de la API/fuente)

    Grano de salida: 1 fila = 1 (moneda, fecha faltante, motivo).
    """
    all_dates = spark.sql(
        f"SELECT explode(sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 day)) as date"
    )
    all_dates = all_dates.withColumn("day_of_week", F.dayofweek("date"))
    # dayofweek: 1=domingo, 7=sábado (convención Spark)
    all_dates = all_dates.withColumn(
        "is_weekend", F.col("day_of_week").isin(1, 7)
    )

    currencies_df = spark.createDataFrame([(c,) for c in currencies], ["currency"])
    expected = all_dates.crossJoin(currencies_df)

    observed = df.select("date", "currency").distinct().withColumn("is_observed", F.lit(True))

    joined = expected.join(observed, on=["date", "currency"], how="left")

    missing = joined.filter(F.col("is_observed").isNull()).withColumn(
        "reason",
        F.when(F.col("is_weekend"), F.lit("weekend_or_holiday")).otherwise(F.lit("missing_data")),
    ).select("date", "currency", "reason")

    total_expected = expected.count()
    total_observed = observed.count()
    logger.info(
        f"Reporte de calidad: {total_observed}/{total_expected} observaciones esperadas presentes "
        f"({missing.filter(F.col('reason') == 'missing_data').count()} huecos sin explicación de calendario)."
    )

    return missing.withColumn("report_generated_at", F.current_timestamp())

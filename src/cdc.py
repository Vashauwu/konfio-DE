"""
Capa de CDC (Change Data Capture).

Estrategia elegida: HASH DE FILA sobre las columnas de negocio (config
`cdc.compare_columns`), comparado contra lo que ya existe en Iceberg para la
misma llave de negocio (config `cdc.business_keys`: date + currency).

Por qué hash y no comparación columna por columna:
- Si mañana se agrega una columna de negocio nueva (ej. bid/ask spread),
  el hash la cubre automáticamente sin tocar esta lógica.
- Trade-off: perdemos el detalle de "qué columna cambió exactamente".
  Para este dominio (una sola columna de negocio, `rate`) esa pérdida de
  información es irrelevante; en un modelo con muchas columnas de negocio
  se podría loggear también un diff campo a campo si se necesitara.

Salida: el dataset de entrada + columna `operation_type` (INSERT/UPDATE/
DELETE/NONE) + columnas de auditoría `ingestion_timestamp` (cuándo se vio
por primera vez, se preserva del extract) y `updated_at` (cuándo se detectó
el cambio más reciente).

Idempotencia: si se corre el mismo snapshot dos veces sin cambios reales,
el hash no varía -> operation_type = NONE -> el MERGE INTO en `load.py` no
modifica nada. No se generan duplicados ni eventos falsos.
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common import get_logger, load_config

logger = get_logger(__name__)


def _row_hash(df: DataFrame, compare_columns: list[str]) -> DataFrame:
    concat_cols = [F.coalesce(F.col(c).cast("string"), F.lit("∅")) for c in compare_columns]
    return df.withColumn("row_hash", F.sha2(F.concat_ws("||", *concat_cols), 256))


def detect_changes(new_df: DataFrame, existing_df: DataFrame | None, spark: SparkSession) -> DataFrame:
    """
    Compara `new_df` (snapshot recién extraído/transformado) contra
    `existing_df` (lo que ya está persistido en Iceberg, o None si la tabla
    aún no existe -> primera carga, todo es INSERT).

    Devuelve un DataFrame con el grano de new_df (+ posibles DELETEs
    lógicos) y las columnas: operation_type, row_hash, ingestion_timestamp,
    updated_at.
    """
    cfg = load_config()
    business_keys = cfg["cdc"]["business_keys"]
    compare_columns = cfg["cdc"]["compare_columns"]

    new_hashed = _row_hash(new_df, compare_columns)
    now = F.current_timestamp()

    if existing_df is None or existing_df.rdd.isEmpty():
        logger.info("No hay datos previos en Iceberg: primera carga, todo se marca como INSERT.")
        result = new_hashed.withColumn("operation_type", F.lit("INSERT"))
        result = result.withColumn("updated_at", now)
        # ingestion_timestamp ya viene del extract; si faltara, se completa aquí
        if "ingestion_timestamp" not in result.columns:
            result = result.withColumn("ingestion_timestamp", now)
        return result

    # Se conserva el ingestion_timestamp original (primera vez que se vio la
    # llave) para filas UPDATE/NONE — solo un INSERT nuevo debe traer un
    # ingestion_timestamp fresco. Sin esto, cada UPDATE resetearía
    # incorrectamente "cuándo se ingirió por primera vez este dato".
    existing_hashed = _row_hash(existing_df, compare_columns).select(
        *business_keys,
        F.col("row_hash").alias("existing_hash"),
        F.col("ingestion_timestamp").alias("existing_ingestion_timestamp"),
    )

    joined = new_hashed.join(existing_hashed, on=business_keys, how="left")

    with_op = joined.withColumn(
        "operation_type",
        F.when(F.col("existing_hash").isNull(), F.lit("INSERT"))
        .when(F.col("row_hash") != F.col("existing_hash"), F.lit("UPDATE"))
        .otherwise(F.lit("NONE")),
    ).withColumn(
        "ingestion_timestamp",
        F.coalesce(F.col("existing_ingestion_timestamp"), F.col("ingestion_timestamp")),
    ).drop("existing_hash", "existing_ingestion_timestamp")

    with_op = with_op.withColumn("updated_at", now)

    # --- DELETEs lógicos ---
    # Llaves que existían en Iceberg pero ya no vienen en el snapshot nuevo.
    # Se marcan con operation_type='DELETE' preservando sus columnas de
    # negocio tal como estaban (soft delete: no se borra físicamente).
    new_keys = new_hashed.select(*business_keys).distinct()
    deleted_keys = existing_df.join(new_keys, on=business_keys, how="left_anti")

    if deleted_keys.rdd.isEmpty():
        deletes = None
    else:
        deletes = (
            _row_hash(deleted_keys, compare_columns)
            .withColumn("operation_type", F.lit("DELETE"))
            .withColumn("updated_at", now)
        )
        if "ingestion_timestamp" not in deletes.columns:
            deletes = deletes.withColumn("ingestion_timestamp", now)
        # Alinear columnas con with_op antes del union
        deletes = deletes.select(*with_op.columns)

    result = with_op if deletes is None else with_op.unionByName(deletes)

    counts = result.groupBy("operation_type").count().collect()
    summary = {r["operation_type"]: r["count"] for r in counts}
    logger.info(f"CDC detectado: {summary}")

    return result

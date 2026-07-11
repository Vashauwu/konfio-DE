"""
Capa de CARGA (Load) — Apache Iceberg.

Aquí se demuestra el uso de Iceberg como tabla TRANSACCIONAL, no solo como
almacenamiento de archivos:

1. MERGE INTO: aplica el resultado del CDC (INSERT/UPDATE/DELETE) de forma
   atómica e idempotente contra `db.tipos_cambio_enriquecidos`.
2. Particionado por (year, month): las consultas típicas de este dominio
   (reportes mensuales, series de tiempo) filtran por fecha, así que esta
   partición evita escanear el histórico completo. No particionamos por
   `currency` porque el volumen por moneda es bajo (harían falta muchas
   particiones pequeñas -> problema de "small files").
3. Time travel: se expone una función de ejemplo para consultar un
   snapshot anterior de la tabla (auditoría / reproducibilidad).
4. Schema evolution: `add_column_if_not_exists` muestra cómo se agregaría
   una columna nueva sin reescribir la tabla completa.
"""
from pyspark.sql import DataFrame, SparkSession

from src.common import get_logger, load_config

logger = get_logger(__name__)


def _full_table_name(table_key: str) -> str:
    cfg = load_config()
    catalog = cfg["iceberg"]["catalog_name"]
    db = cfg["iceberg"]["db_name"]
    table = cfg["iceberg"]["tables"][table_key]
    return f"{catalog}.{db}.{table}"


def ensure_database(spark: SparkSession) -> None:
    cfg = load_config()
    catalog = cfg["iceberg"]["catalog_name"]
    db = cfg["iceberg"]["db_name"]
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.{db}")


def table_exists(spark: SparkSession, table_key: str) -> bool:
    full_name = _full_table_name(table_key)
    return spark.catalog.tableExists(full_name)


def read_table(spark: SparkSession, table_key: str) -> DataFrame | None:
    if not table_exists(spark, table_key):
        return None
    return spark.table(_full_table_name(table_key))


def create_partitioned_table_if_not_exists(spark: SparkSession, df: DataFrame, table_key: str) -> None:
    """
    Crea la tabla Iceberg particionada por (year, month) usando el schema
    del DataFrame, si todavía no existe. Se usa CREATE TABLE ... AS SELECT
    limitado a 0 filas para fijar el schema sin duplicar la carga real
    (que se hace después vía MERGE INTO).
    """
    full_name = _full_table_name(table_key)
    if table_exists(spark, table_key):
        return

    df.limit(0).writeTo(full_name).partitionedBy("year", "month").createOrReplace()
    logger.info(f"Tabla Iceberg creada: {full_name} (particionada por year, month).")


def merge_into_fact_table(spark: SparkSession, changes_df: DataFrame, table_key: str, business_keys: list[str]) -> None:
    """
    Aplica el resultado de CDC vía MERGE INTO (comando SQL de Iceberg),
    que es lo que hace a esta carga TRANSACCIONAL e IDEMPOTENTE:
    - Si la llave de negocio ya existe y hay UPDATE -> se actualiza la fila.
    - Si la llave no existe -> se inserta (INSERT).
    - Filas con operation_type='NONE' no generan ningún cambio (evita
      reescrituras innecesarias y garantiza que correr el pipeline dos
      veces con los mismos datos no genera duplicados).
    - DELETE se maneja como soft-delete: se conserva la fila pero se marca
      operation_type='DELETE' (permite auditoría vía time travel en vez de
      perder el registro).
    """
    full_name = _full_table_name(table_key)
    create_partitioned_table_if_not_exists(spark, changes_df, table_key)

    changes_df = changes_df.filter("operation_type != 'NONE'")
    if changes_df.rdd.isEmpty():
        logger.info(f"MERGE INTO {full_name}: sin cambios detectados por CDC, no se escribe nada (idempotente).")
        return

    changes_df.createOrReplaceTempView("cdc_changes")

    on_clause = " AND ".join([f"target.{k} = source.{k}" for k in business_keys])
    update_cols = [c for c in changes_df.columns]
    update_set = ", ".join([f"target.{c} = source.{c}" for c in update_cols])
    insert_cols = ", ".join(update_cols)
    insert_vals = ", ".join([f"source.{c}" for c in update_cols])

    merge_sql = f"""
        MERGE INTO {full_name} AS target
        USING cdc_changes AS source
        ON {on_clause}
        WHEN MATCHED THEN UPDATE SET {update_set}
        WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
    """
    logger.info(f"Ejecutando MERGE INTO {full_name} ({changes_df.count()} filas con cambios)...")
    spark.sql(merge_sql)
    logger.info(f"MERGE INTO {full_name} completado.")


def overwrite_summary_table(spark: SparkSession, df: DataFrame, table_key: str, partition_cols: list[str] | None = None) -> None:
    """
    Para tablas derivadas/resumen (métricas mensuales, anomalías, reporte de
    calidad) se usa overwrite completo en cada corrida: son recalculables
    100% desde la tabla de hechos, así que no necesitan lógica incremental
    propia — el MERGE INTO en la tabla de hechos ya es la fuente de verdad
    incremental. Mantener un MERGE aquí también sería complejidad sin
    beneficio real (estas tablas no tienen updates parciales con sentido).
    """
    full_name = _full_table_name(table_key)
    writer = df.writeTo(full_name)
    if partition_cols:
        writer = writer.partitionedBy(*partition_cols)

    if table_exists(spark, table_key):
        writer.createOrReplace()
    else:
        writer.create()
    logger.info(f"Tabla {full_name} recalculada (overwrite completo, {df.count()} filas).")


def add_column_if_not_exists(spark: SparkSession, table_key: str, column_name: str, column_type: str) -> None:
    """Demostración de schema evolution: agrega una columna sin reescribir la tabla."""
    full_name = _full_table_name(table_key)
    existing_cols = [f.name for f in spark.table(full_name).schema.fields]
    if column_name in existing_cols:
        return
    spark.sql(f"ALTER TABLE {full_name} ADD COLUMN {column_name} {column_type}")
    logger.info(f"Schema evolution: columna '{column_name}' ({column_type}) agregada a {full_name}.")


def read_table_as_of_snapshot(spark: SparkSession, table_key: str, snapshot_id: int) -> DataFrame:
    """Time travel: lee la tabla como estaba en un snapshot específico."""
    full_name = _full_table_name(table_key)
    return spark.read.option("snapshot-id", snapshot_id).table(full_name)


def list_snapshots(spark: SparkSession, table_key: str) -> DataFrame:
    """Lista los snapshots disponibles de una tabla (metadata Iceberg)."""
    cfg = load_config()
    catalog = cfg["iceberg"]["catalog_name"]
    db = cfg["iceberg"]["db_name"]
    table = cfg["iceberg"]["tables"][table_key]
    return spark.sql(f"SELECT * FROM {catalog}.{db}.{table}.snapshots ORDER BY committed_at")


def rewrite_data_files(spark: SparkSession, table_key: str) -> DataFrame:
    """
    COMPACTION: combina archivos pequeños de la tabla en archivos más
    grandes usando el procedimiento nativo de Iceberg `rewrite_data_files`.

    Por qué importa: cada MERGE INTO / commit puede generar archivos
    Parquet pequeños (uno por partición tocada). Con muchas corridas
    incrementales esto degrada el rendimiento de lectura ("small files
    problem"). Compactar periódicamente reescribe esos archivos pequeños
    en unos más grandes, sin cambiar los datos ni el historial de
    snapshots visible para time travel.

    Devuelve un DataFrame con el resumen de la operación (archivos
    reescritos, archivos añadidos, bytes procesados).
    """
    full_name = _full_table_name(table_key)
    cfg = load_config()
    catalog = cfg["iceberg"]["catalog_name"]
    table_without_catalog = full_name.split(".", 1)[1]  # "db.tabla"

    logger.info(f"Ejecutando compaction (rewrite_data_files) sobre {full_name}...")
    result = spark.sql(f"CALL {catalog}.system.rewrite_data_files(table => '{table_without_catalog}')")
    result.show(truncate=False)
    logger.info(f"Compaction completada sobre {full_name}.")
    return result

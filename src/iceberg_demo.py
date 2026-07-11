
"""
Demostración de capacidades TRANSACCIONALES de Iceberg más allá de MERGE INTO:
 
1. TIME TRAVEL: lista los snapshots de db.tipos_cambio_enriquecidos y
   compara el estado de la tabla en el snapshot más antiguo vs. el actual —
   evidencia de que se puede auditar/reproducir cualquier punto en el
   tiempo sin mantener copias manuales de la tabla.
 
2. SCHEMA EVOLUTION: agrega una columna nueva (`source_system`) a la tabla
   ya existente SIN reescribirla — solo se actualiza metadata de Iceberg.
 
Este script es una herramienta de evidencia/depuración, NO parte del
pipeline batch — se corre por separado, después de que `docker compose up`
ya generó al menos un snapshot.
 
Uso:
    docker compose --profile demo up iceberg-demo
"""
from src.common import get_logger, get_spark_session
from src import load
 
logger = get_logger("iceberg_demo")
 
TABLE_KEY = "enriched"
 
 
def demo_time_travel(spark) -> None:
    logger.info("=" * 70)
    logger.info("DEMOSTRACIÓN 1/2 — TIME TRAVEL")
    logger.info("=" * 70)
 
    if not load.table_exists(spark, TABLE_KEY):
        logger.warning(
            "La tabla aún no existe. Corre `docker compose up` primero para generar "
            "al menos un snapshot."
        )
        return
 
    snapshots = load.list_snapshots(spark, TABLE_KEY).collect()
    logger.info(f"Snapshots encontrados en db.tipos_cambio_enriquecidos: {len(snapshots)}")
 
    for s in snapshots:
        logger.info(
            f"  snapshot_id={s['snapshot_id']} | committed_at={s['committed_at']} | "
            f"operation={s['operation']}"
        )
 
    if len(snapshots) == 0:
        return
 
    first_snapshot_id = snapshots[0]["snapshot_id"]
    first_snapshot_df = load.read_table_as_of_snapshot(spark, TABLE_KEY, first_snapshot_id)
    first_count = first_snapshot_df.count()
 
    current_df = load.read_table(spark, TABLE_KEY)
    current_count = current_df.count()
 
    logger.info(f"Filas en el snapshot MÁS ANTIGUO ({first_snapshot_id}): {first_count}")
    logger.info(f"Filas en el estado ACTUAL de la tabla: {current_count}")
 
    if len(snapshots) > 1:
        logger.info(
            "La tabla tiene múltiples snapshots: cada corrida del pipeline que aplica "
            "cambios vía MERGE INTO genera uno nuevo. Time travel permite consultar "
            "cualquiera de ellos con `spark.read.option('snapshot-id', ...).table(...)`, "
            "sin necesidad de mantener copias manuales de la tabla para auditoría."
        )
    else:
        logger.info(
            "Solo hay 1 snapshot todavía (primera carga). Corre `docker compose up` una "
            "segunda vez con datos distintos y vuelve a correr este demo para ver más "
            "de un snapshot y una comparación de time travel con datos reales."
        )
 
 
def demo_schema_evolution(spark) -> None:
    logger.info("=" * 70)
    logger.info("DEMOSTRACIÓN 2/2 — SCHEMA EVOLUTION")
    logger.info("=" * 70)
 
    if not load.table_exists(spark, TABLE_KEY):
        logger.warning("La tabla aún no existe. Corre `docker compose up` primero.")
        return
 
    before_cols = [f.name for f in load.read_table(spark, TABLE_KEY).schema.fields]
    logger.info(f"Columnas ANTES: {before_cols}")
 
    if "source_system" in before_cols:
        logger.info(
            "La columna 'source_system' ya existe (este demo ya se corrió antes). "
            "add_column_if_not_exists es idempotente: no la vuelve a agregar."
        )
    else:
        load.add_column_if_not_exists(spark, TABLE_KEY, "source_system", "STRING")
 
    after_cols = [f.name for f in load.read_table(spark, TABLE_KEY).schema.fields]
    logger.info(f"Columnas DESPUÉS: {after_cols}")
 
    row_count_after = load.read_table(spark, TABLE_KEY).count()
    logger.info(
        f"La tabla sigue teniendo {row_count_after} filas: agregar la columna NO "
        "reescribió los datos existentes (solo se actualizó la metadata de Iceberg). "
        "Las filas ya persistidas muestran NULL en 'source_system' hasta que se "
        "actualicen explícitamente."
    )
 
 
def demo_compaction(spark) -> None:
    logger.info("=" * 70)
    logger.info("DEMOSTRACIÓN 3/3 — COMPACTION (rewrite_data_files)")
    logger.info("=" * 70)
 
    if not load.table_exists(spark, TABLE_KEY):
        logger.warning("La tabla aún no existe. Corre `docker compose up` primero.")
        return
 
    load.rewrite_data_files(spark, TABLE_KEY)
    logger.info(
        "La compaction combina archivos Parquet pequeños generados por corridas "
        "incrementales sucesivas en archivos más grandes, mejorando el rendimiento "
        "de lectura sin alterar los datos ni el historial de snapshots (time travel "
        "sigue funcionando igual después de compactar)."
    )
 
 
def main() -> None:
    spark = get_spark_session("konfio-iceberg-demo")
    load.ensure_database(spark)
 
    demo_time_travel(spark)
    demo_schema_evolution(spark)
    demo_compaction(spark)
 
    spark.stop()
 
 
if __name__ == "__main__":
    main()
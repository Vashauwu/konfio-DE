"""
Punto de entrada del pipeline. Orquesta las capas como un DAG EXPLÍCITO
(ver src/dag.py) con dependencias declaradas entre pasos, en vez de una
secuencia de llamadas de función implícita.

El grafo resultante tiene una rama principal (extract -> clean -> enrich ->
cdc -> load -> agregaciones) y dos ramas secundarias que dependen de la
principal en distintos puntos:
  - eventos (Kafka + disco), que dependen del resultado del CDC
  - riesgo diario (segunda fuente + join), que depende del estado ya
    mergeado en Iceberg

Ejecutar dos veces seguidas sin cambios en la fuente debe dejar:
  - operation_type = NONE para todas las filas del CDC
  - 0 eventos nuevos generados
  - la tabla Iceberg de hechos sin cambios de datos (aunque sí registra un
    nuevo snapshot vacío, visible con `docker compose --profile demo up iceberg-demo`)
Eso es lo que valida la idempotencia pedida en el enunciado.
"""
import sys
import time

from src.common import get_logger, get_spark_session, load_config
from src.dag import DAG
from src import cdc, events, extract, load, model, secondary_source, transform

logger = get_logger("main")


def build_pipeline_dag(spark, cfg: dict) -> DAG:
    api_cfg = cfg["api"]
    business_keys = cfg["cdc"]["business_keys"]

    dag = DAG()

    # --- Rama principal: extracción -> transformación -> CDC -> carga ---
    dag.add_step("extract", lambda ctx: extract.extract_exchange_rates(spark))

    dag.add_step(
        "clean", lambda ctx: transform.clean_exchange_rates(ctx["extract"]), depends_on=["extract"]
    )

    def _enrich(ctx):
        df = transform.enrich_exchange_rates(ctx["clean"])
        df.cache()
        return df

    dag.add_step("enrich", _enrich, depends_on=["clean"])

    dag.add_step("read_existing", lambda ctx: load.read_table(spark, "enriched"))

    def _cdc(ctx):
        df = cdc.detect_changes(ctx["enrich"], ctx["read_existing"], spark)
        df.cache()
        return df

    dag.add_step("cdc", _cdc, depends_on=["enrich", "read_existing"])

    def _load_merge(ctx):
        load.merge_into_fact_table(spark, ctx["cdc"], "enriched", business_keys)
        return True

    dag.add_step("load_merge", _load_merge, depends_on=["cdc"])

    dag.add_step(
        "read_current_state", lambda ctx: load.read_table(spark, "enriched"), depends_on=["load_merge"]
    )

    # --- Agregaciones derivadas (recalculadas completas cada corrida) ---
    def _monthly_metrics(ctx):
        df = transform.build_monthly_metrics(ctx["read_current_state"])
        load.overwrite_summary_table(spark, df, "monthly_metrics")
        return df

    dag.add_step("monthly_metrics", _monthly_metrics, depends_on=["read_current_state"])

    def _anomalies(ctx):
        df = transform.detect_anomalies(
            ctx["read_current_state"], z_threshold=cfg["anomaly_detection"]["z_score_threshold"]
        )
        load.overwrite_summary_table(spark, df, "anomalies")
        return df

    dag.add_step("anomalies", _anomalies, depends_on=["read_current_state"])

    def _quality_report(ctx):
        df = transform.build_quality_report(
            spark, ctx["read_current_state"], api_cfg["start_date"], api_cfg["end_date"], api_cfg["target_currencies"]
        )
        load.overwrite_summary_table(spark, df, "quality_report")
        return df

    dag.add_step("quality_report", _quality_report, depends_on=["read_current_state"])

    # --- Modelo dimensional ---
    def _dim_currency(ctx):
        df = model.build_dim_currency(spark)
        df.writeTo("local.db.dim_currency").createOrReplace()
        return df

    dag.add_step("dim_currency", _dim_currency)

    # --- Rama de eventos (depende del CDC, no de la carga) ---
    dag.add_step("events_build", lambda ctx: events.build_events(ctx["cdc"]), depends_on=["cdc"])

    def _events_disk(ctx):
        events.write_events_to_disk(ctx["events_build"])
        return True

    dag.add_step("events_disk", _events_disk, depends_on=["events_build"])

    def _events_kafka(ctx):
        events.publish_events_to_kafka(ctx["events_build"])
        return True

    dag.add_step("events_kafka", _events_kafka, depends_on=["events_build"])

    # --- Rama de segunda fuente + join enriquecido ---
    dag.add_step("loan_requests", lambda ctx: secondary_source.extract_loan_requests(spark))

    def _daily_risk(ctx):
        df = secondary_source.build_daily_risk_view(ctx["loan_requests"], ctx["read_current_state"])
        load.overwrite_summary_table(spark, df, "daily_risk", partition_cols=["year", "month"])
        return df

    dag.add_step("daily_risk", _daily_risk, depends_on=["loan_requests", "read_current_state"])

    return dag


def run_pipeline() -> None:
    start_time = time.time()
    cfg = load_config()

    logger.info("=" * 70)
    logger.info("INICIO PIPELINE — Konfio Exchange Rates")
    logger.info("=" * 70)

    spark = get_spark_session()
    load.ensure_database(spark)

    dag = build_pipeline_dag(spark, cfg)
    context = {}
    dag.run(context)

    context["enrich"].unpersist()
    context["cdc"].unpersist()

    elapsed = time.time() - start_time
    logger.info("=" * 70)
    logger.info(f"PIPELINE COMPLETADO en {elapsed:.1f}s")
    logger.info("=" * 70)

    spark.stop()


if __name__ == "__main__":
    try:
        run_pipeline()
    except Exception:
        logger.exception("El pipeline falló.")
        sys.exit(1)

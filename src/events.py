"""
Capa de EVENTOS (simulación de Kafka).

Opción mínima elegida (archivos JSON en /events/) en vez de un broker Kafka
real: para el volumen y alcance de este ejercicio, un broker añadiría
infraestructura sin aportar señal adicional sobre el diseño del pipeline.
El schema del evento sí está pensado para mapear 1:1 a un mensaje Kafka real
(event_type, event_timestamp, entity_id, payload, schema_version), por lo
que migrar a un productor real después sería un cambio de "transporte", no
de modelo de datos.

Garantía de consistencia con CDC: los eventos se generan a partir del MISMO
DataFrame que ya trae operation_type (el resultado de cdc.detect_changes),
nunca se recalculan por separado — así se evita que ambas capas diverjan.
"""
import json
import os
from datetime import datetime, timezone

from pyspark.sql import DataFrame

from src.common import get_logger, load_config

logger = get_logger(__name__)

EVENT_SCHEMA_VERSION = "1.0"


def _entity_id(currency: str, event_date: str) -> str:
    return f"{currency}_{event_date}"


def build_events(changes_df: DataFrame) -> list[dict]:
    """
    Convierte cada fila con operation_type != 'NONE' en un evento.
    changes_df debe ser el output de src.cdc.detect_changes (mismo grano,
    misma columna operation_type) para garantizar consistencia CDC <-> eventos.
    """
    relevant_cols = [
        "date",
        "currency",
        "rate",
        "base_currency",
        "daily_change_pct",
        "operation_type",
        "row_hash",
        "updated_at",
    ]
    available_cols = [c for c in relevant_cols if c in changes_df.columns]

    rows = (
        changes_df.filter("operation_type != 'NONE'")
        .select(*available_cols)
        .collect()
    )

    events = []
    now_iso = datetime.now(timezone.utc).isoformat()

    for row in rows:
        d = row.asDict()
        event_date = str(d.get("date"))
        currency = d.get("currency")

        payload = {
            "currency": currency,
            "base_currency": d.get("base_currency"),
            "rate": d.get("rate"),
            "date": event_date,
            "daily_change_pct": d.get("daily_change_pct"),
            "row_hash": d.get("row_hash"),
        }

        event = {
            "schema_version": EVENT_SCHEMA_VERSION,
            "event_type": d.get("operation_type"),
            "event_timestamp": now_iso,
            "entity_id": _entity_id(currency, event_date),
            "payload": payload,
        }
        events.append(event)

    logger.info(f"{len(events)} eventos construidos a partir del resultado del CDC.")
    return events


def write_events_to_disk(events: list[dict]) -> str:
    """
    Escribe un archivo JSON por evento en la carpeta configurada (/events/).
    Nombre de archivo determinista (entity_id + tipo + timestamp) para poder
    rastrear cada evento hasta el cambio que lo originó.
    """
    cfg = load_config()
    events_dir = cfg["paths"]["events"]
    os.makedirs(events_dir, exist_ok=True)

    for event in events:
        ts_compact = event["event_timestamp"].replace(":", "").replace("-", "").replace(".", "")
        filename = f"{event['entity_id']}__{event['event_type']}__{ts_compact}.json"
        filepath = os.path.join(events_dir, filename)
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(event, f, ensure_ascii=False, indent=2, default=str)

    logger.info(f"{len(events)} eventos escritos en {events_dir}")
    return events_dir


def publish_events_to_kafka(events: list[dict]) -> None:
    """
    Publica cada evento en el topic de Kafka configurado. Se ejecuta
    DESPUÉS de write_events_to_disk (no en su lugar): los JSON en /events/
    quedan como copia de auditoría local, independiente de si el broker
    está disponible o no; Kafka es el canal de distribución en tiempo real
    hacia consumidores externos.

    Si `kafka.enabled = false` en config, o no hay eventos, no hace nada.
    Si el broker no está disponible, falla explícitamente (no se traga el
    error) porque en este ejercicio Kafka es una entrega requerida, no
    "best effort".
    """
    cfg = load_config()
    kafka_cfg = cfg.get("kafka", {})

    if not kafka_cfg.get("enabled", False):
        logger.info("Publicación a Kafka deshabilitada en config (kafka.enabled=false). Se omite.")
        return

    if not events:
        logger.info("No hay eventos nuevos que publicar en Kafka.")
        return

    # Import perezoso: así los módulos que no publican a Kafka (ej. tests
    # unitarios que corren fuera de Docker) no requieren kafka-python instalado
    # ni un broker disponible.
    from kafka import KafkaProducer
    from kafka.errors import KafkaError

    bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", kafka_cfg["bootstrap_servers"])
    topic = kafka_cfg["topic"]

    logger.info(f"Conectando a Kafka en {bootstrap_servers} (topic='{topic}')...")

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False, default=str).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        retries=kafka_cfg.get("max_retries", 5),
        retry_backoff_ms=kafka_cfg.get("retry_backoff_ms", 500),
        acks="all",  # espera confirmación del broker -> no se pierden eventos silenciosamente
    )

    sent, failed = 0, 0
    futures = []
    try:
        for event in events:
            # key = entity_id: garantiza que todos los eventos de la misma
            # moneda+fecha caigan en la misma partición y se lean en orden.
            future = producer.send(topic, key=event["entity_id"], value=event)
            futures.append((event["entity_id"], future))

        producer.flush(timeout=30)

        for entity_id, future in futures:
            try:
                future.get(timeout=10)
                sent += 1
            except KafkaError as e:
                failed += 1
                logger.error(f"Fallo al confirmar evento para {entity_id}: {e}")
    finally:
        producer.close(timeout=10)

    logger.info(f"Publicación a Kafka completada: {sent} confirmados, {failed} fallidos, topic='{topic}'.")

    if failed > 0:
        raise RuntimeError(f"{failed} eventos no pudieron confirmarse en Kafka (topic='{topic}').")

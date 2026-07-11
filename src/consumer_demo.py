"""
Consumidor de DEMOSTRACIÓN.

No forma parte del pipeline batch (que solo produce eventos) — este script
existe para poder mostrar, en vivo, que el topic realmente tiene mensajes y
que se pueden leer con garantías estándar de Kafka (offset, orden por
partición, etc.).

Se detiene solo tras `consumer_timeout_ms` de inactividad (no se queda
escuchando para siempre), así sirve tanto para una demo interactiva como
para correrlo una vez y ver el resultado en logs.

Uso:
    docker compose --profile demo up kafka-consumer-demo
    # o, con el pipeline ya corrido antes:
    docker compose run --rm kafka-consumer-demo
"""
import json
import os

from kafka import KafkaConsumer

from src.common import get_logger, load_config

logger = get_logger("consumer_demo")


def main() -> None:
    cfg = load_config()
    kafka_cfg = cfg["kafka"]
    bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", kafka_cfg["bootstrap_servers"])
    topic = kafka_cfg["topic"]

    logger.info(f"Conectando como consumidor a {bootstrap_servers}, topic='{topic}'...")

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset="earliest",   # lee desde el principio del topic
        enable_auto_commit=False,        # demo de solo lectura, no mueve el offset del grupo
        group_id="konfio-demo-consumer",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        consumer_timeout_ms=15000,  # se detiene tras 15s sin mensajes nuevos
    )

    count = 0
    for message in consumer:
        count += 1
        event = message.value
        logger.info(
            f"[partition={message.partition} offset={message.offset}] "
            f"key={message.key} event_type={event.get('event_type')} "
            f"entity_id={event.get('entity_id')} payload={event.get('payload')}"
        )

    consumer.close()
    logger.info(f"Consumidor detenido tras inactividad. Total de mensajes leídos: {count}")


if __name__ == "__main__":
    main()

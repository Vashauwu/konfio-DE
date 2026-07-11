"""
Utilidades compartidas por todo el pipeline:
- Carga de configuración (config/settings.yaml)
- Construcción de la SparkSession con el catálogo Iceberg
- Logging estandarizado

Se centraliza aquí para que cada módulo (extract, transform, cdc, load,
events) no tenga que reimplementar esto y para que el catálogo Iceberg se
configure exactamente igual en todos lados.
"""
import logging
import os
import sys
from functools import lru_cache

import yaml
from pyspark.sql import SparkSession

CONFIG_PATH = os.environ.get(
    "PIPELINE_CONFIG_PATH",
    os.path.join(os.path.dirname(__file__), "..", "config", "settings.yaml"),
)


def get_logger(name: str) -> logging.Logger:
    """Logger consistente para todo el pipeline (stdout, timestamps, nivel INFO)."""
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger


@lru_cache(maxsize=1)
def load_config() -> dict:
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def get_spark_session(app_name: str = "konfio-exchange-rates") -> SparkSession:
    """
    Crea (o recupera) la SparkSession configurada con el catálogo Iceberg tipo
    'hadoop', que persiste en el filesystem local sin requerir infraestructura
    externa (Hive Metastore, Nessie, etc.) — suficiente para este ejercicio y
    consistente con el requisito de "no se requiere infraestructura externa".

    Usamos spark.jars (jar ya descargado en el Dockerfile) en vez de
    spark.jars.packages para que el contenedor pueda arrancar sin resolver
    dependencias por Ivy en cada ejecución (más rápido y reproducible).
    """
    cfg = load_config()
    warehouse_path = cfg["paths"]["warehouse"]
    catalog_name = cfg["iceberg"]["catalog_name"]

    iceberg_jar = os.environ.get("ICEBERG_JAR_PATH", "")

    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{catalog_name}.type", "hadoop")
        .config(f"spark.sql.catalog.{catalog_name}.warehouse", warehouse_path)
        .config("spark.sql.session.timeZone", "UTC")
        # Menos verborrea de Spark en logs para que el ETL sea legible
        .config("spark.ui.showConsoleProgress", "false")
    )

    if iceberg_jar and os.path.exists(iceberg_jar):
        builder = builder.config("spark.jars", iceberg_jar)
    else:
        # Fallback: si el jar no se descargó en build time, se resuelve por
        # Ivy (requiere red en runtime). Útil en desarrollo local sin Docker.
        builder = builder.config(
            "spark.jars.packages",
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2",
        )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

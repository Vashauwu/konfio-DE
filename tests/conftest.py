"""
Fixture de SparkSession para tests: deliberadamente NO configura el catálogo
Iceberg (no se necesita red ni el jar) porque los tests unitarios validan
lógica de transformación/CDC en memoria, no la persistencia real. La
persistencia en Iceberg se valida manualmente/en CI con `docker compose up`.
"""
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("konfio-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()

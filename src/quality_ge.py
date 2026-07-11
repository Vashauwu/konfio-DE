"""
Validación de calidad de datos con Great Expectations (GE), sobre la tabla
ya persistida `db.tipos_cambio_enriquecidos`.

Por qué GE ADEMÁS del reporte de calidad nativo en `transform.py`: el
reporte de calidad (`db.reporte_calidad`) responde "¿qué fechas faltan y
por qué" (una pregunta de completitud de calendario). GE responde una
pregunta distinta y complementaria: "¿los valores que SÍ llegaron cumplen
las reglas de negocio esperadas?" (nulos, rangos, tipos, membresía de
categorías). Son dos capas de calidad distintas, no una duplicando a la
otra.

Se usa la API clásica de `SparkDFDataset` (great_expectations==0.15.x) en
vez del Fluent Data Context de versiones más nuevas, deliberadamente: para
validar un único DataFrame dentro de un pipeline batch no se necesita el
andamiaje completo de un Data Context (datasources, checkpoints, stores
persistentes en YAML) — sería infraestructura desproporcionada para esta
validación puntual. Es el mismo criterio de simplicidad que ya se aplicó
para no usar Airflow (ver README).

Esta validación es un GATE de calidad, no un paso que bloquea el pipeline:
si alguna expectativa falla, se loggea con detalle y se persiste el
resultado en `/app/reports/ge_validation_result.json`, pero el pipeline
continúa — igual que el reporte de calidad nativo, es información para
actuar, no un aborto automático (una tasa de cambio fuera de rango ya se
descartó en `clean_exchange_rates`; si GE la detecta de todas formas, es
una señal de que algo cambió río arriba y hay que investigar, no que el
batch del día deba fallar).
"""
import json
import os

from pyspark.sql import DataFrame

from src.common import get_logger, load_config

logger = get_logger(__name__)


def validate_enriched_rates(df: DataFrame, target_currencies: list) -> dict:
    """
    Corre un conjunto de expectativas sobre `db.tipos_cambio_enriquecidos`
    y devuelve un resumen serializable (dict) con el resultado.
    """
    from great_expectations.dataset import SparkDFDataset

    ge_df = SparkDFDataset(df)

    expectation_results = []

    def _run(name: str, fn, *args, **kwargs):
        result = fn(*args, **kwargs)
        success = bool(result["success"])
        expectation_results.append({"expectation": name, "success": success})
        if not success:
            logger.warning(f"[Great Expectations] FALLÓ: {name} -> {result.get('result', {})}")
        return success

    _run("date_not_null", ge_df.expect_column_values_to_not_be_null, "date")
    _run("currency_not_null", ge_df.expect_column_values_to_not_be_null, "currency")
    _run("rate_not_null", ge_df.expect_column_values_to_not_be_null, "rate", mostly=0.99)
    _run(
        "rate_in_reasonable_range",
        ge_df.expect_column_values_to_be_between,
        "rate",
        min_value=0.0001,
        max_value=10000,
        mostly=1.0,
    )
    _run(
        "currency_in_configured_set",
        ge_df.expect_column_values_to_be_in_set,
        "currency",
        value_set=target_currencies,
    )
    _run(
        "base_currency_is_usd",
        ge_df.expect_column_values_to_be_in_set,
        "base_currency",
        value_set=["USD"],
    )
    _run(
        "operation_type_is_valid",
        ge_df.expect_column_values_to_be_in_set,
        "operation_type",
        value_set=["INSERT", "UPDATE", "DELETE", "NONE"],
    )
    _run(
        "row_hash_not_null",
        ge_df.expect_column_values_to_not_be_null,
        "row_hash",
    )

    total = len(expectation_results)
    passed = sum(1 for r in expectation_results if r["success"])

    summary = {
        "total_expectations": total,
        "passed": passed,
        "failed": total - passed,
        "all_passed": passed == total,
        "details": expectation_results,
    }

    logger.info(
        f"[Great Expectations] {passed}/{total} expectativas cumplidas sobre "
        f"db.tipos_cambio_enriquecidos."
    )
    return summary


def write_validation_report(summary: dict) -> None:
    cfg = load_config()
    reports_dir = cfg["paths"]["reports"]
    os.makedirs(reports_dir, exist_ok=True)
    path = os.path.join(reports_dir, "ge_validation_result.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2, default=str)
    logger.info(f"Resultado de validación de Great Expectations escrito en {path}.")

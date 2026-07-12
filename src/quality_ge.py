
"""
Validación de calidad de datos con GX Core (great_expectations >= 1.0),
sobre la tabla ya persistida `db.tipos_cambio_enriquecidos`.

Por qué GX ADEMÁS del reporte de calidad nativo en `transform.py`: el
reporte de calidad (`db.reporte_calidad`) responde "¿qué fechas faltan y
por qué" (una pregunta de completitud de calendario). GX responde una
pregunta distinta y complementaria: "¿los valores que SÍ llegaron cumplen
las reglas de negocio esperadas?" (nulos, rangos, tipos, membresía de
categorías). Son dos capas de calidad distintas, no una duplicando a la
otra.

MIGRADO a la API "GX Core 1.0+" (Data Context → Data Source → Data Asset →
Batch Definition → Batch → Expectation), que reemplazó a la API clásica
`SparkDFDataset` desde agosto 2024. Decisiones específicas de esta API:

- `mode="ephemeral"`: el Data Context vive solo en memoria durante esta
  corrida, sin escribir ningún directorio de configuración/stores a disco
  (`gx/`, checkpoints, expectation suites persistidas). Para una
  validación puntual dentro de un pipeline batch, un Data Context de tipo
  "file" (persistente) sería andamiaje que no necesitamos — mismo criterio
  de simplicidad que ya se aplicó para no usar Airflow.
- Se valida expectativa por expectativa con `batch.validate(...)` en vez
  de agrupar en un `ValidationDefinition` + `Checkpoint`: no necesitamos
  Actions (alertas Slack/email) ni Checkpoints reutilizables entre
  corridas — cada corrida del pipeline es autocontenida.
- `data_sources.add_spark(...)` reutiliza la `SparkSession` activa (la
  misma que ya trae configurado el catálogo Iceberg vía `common.py`), no
  crea una sesión nueva.

Esta validación sigue siendo un GATE informativo, no bloqueante: si alguna
expectativa falla, se loggea con detalle y se persiste el resultado en
`/app/reports/ge_validation_result.json`, pero el pipeline continúa.
"""
import json
import os

from pyspark.sql import DataFrame

from src.common import get_logger, load_config

logger = get_logger(__name__)


def _extract_success(result) -> bool:
    """
    El resultado de `batch.validate(...)` en GX Core es un objeto
    ExpectationValidationResult. Se intenta acceso por atributo primero
    (API nueva) y se cae a acceso tipo dict como red de seguridad, por si
    la versión instalada difiere ligeramente en la forma del objeto.
    """
    if hasattr(result, "success"):
        return bool(result.success)
    return bool(result["success"])


def validate_enriched_rates(df: DataFrame, target_currencies: list) -> dict:
    """
    Corre un conjunto de expectativas sobre `db.tipos_cambio_enriquecidos`
    usando GX Core, y devuelve un resumen serializable (dict) con el
    resultado.
    """
    import great_expectations as gx
    from great_expectations import expectations as gxe

    context = gx.get_context(mode="ephemeral")

    data_source = context.data_sources.add_spark(name="konfio_spark_source")
    asset = data_source.add_dataframe_asset(name="tipos_cambio_enriquecidos")
    batch_definition = asset.add_batch_definition_whole_dataframe("full_table")
    batch = batch_definition.get_batch(batch_parameters={"dataframe": df})

    expectation_definitions = [
        ("date_not_null", gxe.ExpectColumnValuesToNotBeNull(column="date")),
        ("currency_not_null", gxe.ExpectColumnValuesToNotBeNull(column="currency")),
        ("rate_not_null", gxe.ExpectColumnValuesToNotBeNull(column="rate", mostly=0.99)),
        (
            "rate_in_reasonable_range",
            gxe.ExpectColumnValuesToBeBetween(column="rate", min_value=0.0001, max_value=10000, mostly=1.0),
        ),
        (
            "currency_in_configured_set",
            gxe.ExpectColumnValuesToBeInSet(column="currency", value_set=target_currencies),
        ),
        (
            "base_currency_is_usd",
            gxe.ExpectColumnValuesToBeInSet(column="base_currency", value_set=["USD"]),
        ),
        (
            "operation_type_is_valid",
            gxe.ExpectColumnValuesToBeInSet(column="operation_type", value_set=["INSERT", "UPDATE", "DELETE", "NONE"]),
        ),
        ("row_hash_not_null", gxe.ExpectColumnValuesToNotBeNull(column="row_hash")),
    ]

    expectation_results = []
    for name, expectation in expectation_definitions:
        result = batch.validate(expectation)
        success = _extract_success(result)
        expectation_results.append({"expectation": name, "success": success})
        if not success:
            logger.warning(f"[Great Expectations] FALLÓ: {name}")

    total = len(expectation_results)
    passed = sum(1 for r in expectation_results if r["success"])

    summary = {
        "gx_api": "gx_core_1.x (Data Context / Batch / Expectation)",
        "total_expectations": total,
        "passed": passed,
        "failed": total - passed,
        "all_passed": passed == total,
        "details": expectation_results,
    }

    logger.info(
        f"[Great Expectations] {passed}/{total} expectativas cumplidas sobre "
        f"db.tipos_cambio_enriquecidos (API: GX Core 1.x)."
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
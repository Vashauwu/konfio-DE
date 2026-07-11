from datetime import date, datetime

from src.cdc import detect_changes


def _df(spark, rows):
    """rows: lista de (date, currency, rate)"""
    data = [
        {
            "date": date.fromisoformat(d),
            "currency": c,
            "rate": r,
            "base_currency": "USD",
            "ingestion_timestamp": datetime(2024, 1, 1),
        }
        for d, c, r in rows
    ]
    return spark.createDataFrame(data)


def test_first_load_marks_everything_as_insert(spark):
    new_df = _df(spark, [("2024-01-02", "MXN", 17.0), ("2024-01-02", "EUR", 0.91)])
    result = detect_changes(new_df, existing_df=None, spark=spark)

    ops = [row["operation_type"] for row in result.collect()]
    assert ops == ["INSERT", "INSERT"]


def test_unchanged_rows_are_marked_none(spark):
    existing = _df(spark, [("2024-01-02", "MXN", 17.0)])
    new_df = _df(spark, [("2024-01-02", "MXN", 17.0)])  # mismo valor exacto

    result = detect_changes(new_df, existing_df=existing, spark=spark)
    ops = [row["operation_type"] for row in result.collect()]
    assert ops == ["NONE"]


def test_changed_rate_is_marked_update(spark):
    existing = _df(spark, [("2024-01-02", "MXN", 17.0)])
    new_df = _df(spark, [("2024-01-02", "MXN", 17.5)])  # tasa cambió

    result = detect_changes(new_df, existing_df=existing, spark=spark)
    ops = [row["operation_type"] for row in result.collect()]
    assert ops == ["UPDATE"]


def test_new_key_is_marked_insert_even_with_existing_data(spark):
    existing = _df(spark, [("2024-01-02", "MXN", 17.0)])
    new_df = _df(spark, [("2024-01-02", "MXN", 17.0), ("2024-01-03", "MXN", 17.2)])

    result = detect_changes(new_df, existing_df=existing, spark=spark)
    ops_by_date = {row["date"].isoformat(): row["operation_type"] for row in result.collect()}
    assert ops_by_date["2024-01-02"] == "NONE"
    assert ops_by_date["2024-01-03"] == "INSERT"


def test_missing_key_is_marked_delete(spark):
    existing = _df(spark, [("2024-01-02", "MXN", 17.0), ("2024-01-03", "MXN", 17.2)])
    new_df = _df(spark, [("2024-01-02", "MXN", 17.0)])  # 01-03 ya no viene

    result = detect_changes(new_df, existing_df=existing, spark=spark)
    ops_by_date = {row["date"].isoformat(): row["operation_type"] for row in result.collect()}
    assert ops_by_date["2024-01-02"] == "NONE"
    assert ops_by_date["2024-01-03"] == "DELETE"


def test_idempotent_double_run_produces_no_changes(spark):
    """Simula correr el pipeline dos veces con el mismo snapshot nuevo."""
    existing = _df(spark, [("2024-01-02", "MXN", 17.0)])
    new_df = _df(spark, [("2024-01-02", "MXN", 17.0), ("2024-01-03", "MXN", 17.3)])

    first_run = detect_changes(new_df, existing_df=existing, spark=spark)
    first_ops = sorted([row["operation_type"] for row in first_run.collect()])
    assert first_ops == ["INSERT", "NONE"]

    # Segunda corrida: "existing" ahora ya incluiría 2024-01-03 (post-merge).
    existing_after_merge = _df(spark, [("2024-01-02", "MXN", 17.0), ("2024-01-03", "MXN", 17.3)])
    second_run = detect_changes(new_df, existing_df=existing_after_merge, spark=spark)
    second_ops = [row["operation_type"] for row in second_run.collect()]
    assert second_ops == ["NONE", "NONE"]

from datetime import datetime

from src.transform import (
    build_monthly_metrics,
    clean_exchange_rates,
    detect_anomalies,
    enrich_exchange_rates,
)


def _make_raw_df(spark, rows):
    """rows: lista de (date_str, currency, rate)"""
    data = [
        {
            "date": d,
            "currency": c,
            "rate": r,
            "base_currency": "USD",
            "ingestion_timestamp": datetime(2024, 1, 1),
        }
        for d, c, r in rows
    ]
    return spark.createDataFrame(data)


def test_clean_removes_duplicates(spark):
    df = _make_raw_df(
        spark,
        [
            ("2024-01-02", "MXN", 17.0),
            ("2024-01-02", "MXN", 17.0),  # duplicado exacto
        ],
    )
    cleaned = clean_exchange_rates(df)
    assert cleaned.count() == 1


def test_clean_filters_out_of_range_rates(spark):
    df = _make_raw_df(
        spark,
        [
            ("2024-01-02", "MXN", 17.0),
            ("2024-01-03", "MXN", -5.0),   # negativo, inválido
            ("2024-01-04", "MXN", 999999.0),  # fuera de rango razonable
        ],
    )
    cleaned = clean_exchange_rates(df)
    rates = [r["rate"] for r in cleaned.collect()]
    assert -5.0 not in rates
    assert 999999.0 not in rates
    assert 17.0 in rates


def test_clean_casts_date_type(spark):
    df = _make_raw_df(spark, [("2024-01-02", "MXN", 17.0)])
    cleaned = clean_exchange_rates(df)
    field = [f for f in cleaned.schema.fields if f.name == "date"][0]
    assert field.dataType.typeName() == "date"


def test_enrich_daily_change_pct(spark):
    df = _make_raw_df(
        spark,
        [
            ("2024-01-02", "MXN", 10.0),
            ("2024-01-03", "MXN", 11.0),  # +10%
        ],
    )
    cleaned = clean_exchange_rates(df)
    enriched = enrich_exchange_rates(cleaned)
    rows = {row["date"].isoformat(): row for row in enriched.collect()}

    assert rows["2024-01-02"]["daily_change_pct"] is None  # sin día anterior
    assert abs(rows["2024-01-03"]["daily_change_pct"] - 10.0) < 1e-6


def test_enrich_rolling_avg_uses_available_observations(spark):
    # Solo 3 observaciones: la ventana de 7 debe promediar lo disponible,
    # no fallar ni requerir 7 filas.
    df = _make_raw_df(
        spark,
        [
            ("2024-01-02", "MXN", 10.0),
            ("2024-01-03", "MXN", 20.0),
            ("2024-01-04", "MXN", 30.0),
        ],
    )
    cleaned = clean_exchange_rates(df)
    enriched = enrich_exchange_rates(cleaned)
    rows = {row["date"].isoformat(): row for row in enriched.collect()}

    assert abs(rows["2024-01-04"]["rolling_avg_7d"] - 20.0) < 1e-6  # avg(10,20,30)


def test_monthly_metrics_grain(spark):
    df = _make_raw_df(
        spark,
        [
            ("2024-01-02", "MXN", 10.0),
            ("2024-01-15", "MXN", 20.0),
            ("2024-02-01", "MXN", 30.0),
        ],
    )
    cleaned = clean_exchange_rates(df)
    enriched = enrich_exchange_rates(cleaned)
    metrics = build_monthly_metrics(enriched)

    result = {(r["currency"], r["year"], r["month"]): r for r in metrics.collect()}
    assert result[("MXN", 2024, 1)]["num_observations"] == 2
    assert abs(result[("MXN", 2024, 1)]["avg_rate"] - 15.0) < 1e-6
    assert result[("MXN", 2024, 2)]["num_observations"] == 1


def test_detect_anomalies_flags_large_jump(spark):
    # Construimos una serie estable y luego un salto grande al final.
    rows = [(f"2024-01-{d:02d}", "MXN", 17.0) for d in range(2, 20)]
    rows.append(("2024-01-20", "MXN", 25.0))  # salto grande vs. estable en 17
    df = _make_raw_df(spark, rows)
    cleaned = clean_exchange_rates(df)
    enriched = enrich_exchange_rates(cleaned)
    anomalies = detect_anomalies(enriched, z_threshold=2.0)

    anomaly_dates = [row["date"].isoformat() for row in anomalies.collect()]
    assert "2024-01-20" in anomaly_dates

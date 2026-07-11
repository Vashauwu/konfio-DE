from datetime import date

from src.secondary_source import build_daily_risk_view


def _loan_requests_df(spark, rows):
    """rows: lista de (date, num_solicitudes, monto_promedio_mxn, tasa_aprobacion_pct)"""
    data = [
        {
            "date": date.fromisoformat(d),
            "num_solicitudes": n,
            "monto_promedio_mxn": m,
            "tasa_aprobacion_pct": t,
        }
        for d, n, m, t in rows
    ]
    return spark.createDataFrame(data)


def _exchange_rates_df(spark, rows):
    """rows: lista de (date, currency, rate)"""
    data = [{"date": date.fromisoformat(d), "currency": c, "rate": r} for d, c, r in rows]
    return spark.createDataFrame(data)


def test_join_matches_same_day_rate(spark):
    loans = _loan_requests_df(spark, [("2024-01-02", 100, 180000.0, 50.0)])
    rates = _exchange_rates_df(spark, [("2024-01-02", "MXN", 17.0), ("2024-01-02", "EUR", 0.91)])

    result = build_daily_risk_view(loans, rates).collect()
    assert len(result) == 1
    assert abs(result[0]["mxn_rate_filled"] - 17.0) < 1e-6
    assert abs(result[0]["monto_promedio_usd"] - (180000.0 / 17.0)) < 1e-2


def test_join_forward_fills_weekend_gap(spark):
    # Viernes con tasa, sábado y domingo sin tasa (la API no los publica).
    loans = _loan_requests_df(
        spark,
        [
            ("2024-01-05", 90, 150000.0, 45.0),   # viernes, con tasa
            ("2024-01-06", 40, 120000.0, 40.0),   # sábado, sin tasa
            ("2024-01-07", 30, 110000.0, 38.0),   # domingo, sin tasa
        ],
    )
    rates = _exchange_rates_df(spark, [("2024-01-05", "MXN", 17.1)])

    result = {row["date"].isoformat(): row for row in build_daily_risk_view(loans, rates).collect()}

    assert abs(result["2024-01-05"]["mxn_rate_filled"] - 17.1) < 1e-6
    # Fin de semana: se propaga (forward-fill) la última tasa hábil conocida.
    assert abs(result["2024-01-06"]["mxn_rate_filled"] - 17.1) < 1e-6
    assert abs(result["2024-01-07"]["mxn_rate_filled"] - 17.1) < 1e-6


def test_join_preserves_loan_request_grain(spark):
    # El grano de salida debe ser 1 fila por día de solicitudes, sin duplicar
    # filas aunque exchange_rates tenga varias monedas para la misma fecha.
    loans = _loan_requests_df(spark, [("2024-01-02", 100, 180000.0, 50.0)])
    rates = _exchange_rates_df(
        spark,
        [
            ("2024-01-02", "MXN", 17.0),
            ("2024-01-02", "EUR", 0.91),
            ("2024-01-02", "BRL", 4.9),
            ("2024-01-02", "COP", 3900.0),
        ],
    )
    result = build_daily_risk_view(loans, rates).collect()
    assert len(result) == 1

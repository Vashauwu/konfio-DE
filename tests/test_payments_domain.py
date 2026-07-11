from datetime import date

from src.payments_domain import build_fact_transactions


def _transactions_df(spark, rows):
    """rows: lista de (transaction_id, card_id, date, amount, merchant_category)"""
    data = [
        {"transaction_id": t, "card_id": c, "date": date.fromisoformat(d), "amount": a, "merchant_category": m}
        for t, c, d, a, m in rows
    ]
    return spark.createDataFrame(data)


def _cards_df(spark, rows):
    """rows: lista de (card_id, currency)"""
    data = [{"card_id": c, "customer_id": "CUST0001", "card_type": "Debito", "currency": cur, "issued_date": date(2024, 1, 1)} for c, cur in rows]
    return spark.createDataFrame(data)


def _rates_df(spark, rows):
    """rows: lista de (date, currency, rate)"""
    data = [{"date": date.fromisoformat(d), "currency": c, "rate": r} for d, c, r in rows]
    return spark.createDataFrame(data)


def test_mxn_transaction_converted_to_usd(spark):
    txns = _transactions_df(spark, [("TXN1", "CARD1", "2024-01-02", 1700.0, "Insumos")])
    cards = _cards_df(spark, [("CARD1", "MXN")])
    rates = _rates_df(spark, [("2024-01-02", "MXN", 17.0)])

    result = build_fact_transactions(txns, cards, rates).collect()
    assert len(result) == 1
    assert abs(result[0]["amount_usd"] - 100.0) < 1e-6  # 1700 MXN / 17.0 = 100 USD


def test_usd_transaction_not_converted(spark):
    txns = _transactions_df(spark, [("TXN1", "CARD1", "2024-01-02", 250.0, "Servicios")])
    cards = _cards_df(spark, [("CARD1", "USD")])
    rates = _rates_df(spark, [("2024-01-02", "MXN", 17.0)])  # sin fila USD, no debe hacer falta

    result = build_fact_transactions(txns, cards, rates).collect()
    assert abs(result[0]["amount_usd"] - 250.0) < 1e-6


def test_weekend_transaction_uses_forward_filled_rate(spark):
    txns = _transactions_df(
        spark,
        [
            ("TXN1", "CARD1", "2024-01-05", 1710.0, "Logística"),  # viernes, con tasa
            ("TXN2", "CARD1", "2024-01-06", 1700.0, "Logística"),  # sábado, sin tasa publicada
        ],
    )
    cards = _cards_df(spark, [("CARD1", "MXN")])
    rates = _rates_df(spark, [("2024-01-05", "MXN", 17.1)])

    result = {row["transaction_id"]: row for row in build_fact_transactions(txns, cards, rates).collect()}
    assert abs(result["TXN2"]["amount_usd"] - (1700.0 / 17.1)) < 1e-2


def test_output_grain_matches_input_transactions(spark):
    txns = _transactions_df(
        spark,
        [
            ("TXN1", "CARD1", "2024-01-02", 500.0, "Renta"),
            ("TXN2", "CARD2", "2024-01-02", 800.0, "Nómina"),
        ],
    )
    cards = _cards_df(spark, [("CARD1", "MXN"), ("CARD2", "EUR")])
    rates = _rates_df(spark, [("2024-01-02", "MXN", 17.0), ("2024-01-02", "EUR", 0.91)])

    result = build_fact_transactions(txns, cards, rates).collect()
    assert len(result) == 2  # 1 fila por transacción, sin duplicar por el join

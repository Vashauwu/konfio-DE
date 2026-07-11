from src.extract import _filter_to_configured_range, raw_json_to_rows


def test_filter_drops_dates_before_range():
    rows = [
        {"date": "2023-12-29", "currency": "MXN", "rate": 16.9},  # ancla fuera de rango
        {"date": "2024-01-02", "currency": "MXN", "rate": 17.0},
    ]
    result = _filter_to_configured_range(rows, "2024-01-01", "2024-06-30")
    dates = [r["date"] for r in result]
    assert "2023-12-29" not in dates
    assert "2024-01-02" in dates


def test_filter_drops_dates_after_range():
    rows = [
        {"date": "2024-06-30", "currency": "MXN", "rate": 18.0},
        {"date": "2024-07-01", "currency": "MXN", "rate": 18.1},  # fuera de rango, después
    ]
    result = _filter_to_configured_range(rows, "2024-01-01", "2024-06-30")
    dates = [r["date"] for r in result]
    assert "2024-07-01" not in dates
    assert "2024-06-30" in dates


def test_filter_keeps_all_rows_within_range():
    rows = [{"date": f"2024-01-{d:02d}", "currency": "MXN", "rate": 17.0} for d in range(2, 6)]
    result = _filter_to_configured_range(rows, "2024-01-01", "2024-06-30")
    assert len(result) == len(rows)


def test_raw_json_to_rows_flattens_correctly():
    raw = {
        "base": "USD",
        "rates": {
            "2024-01-02": {"MXN": 17.065, "EUR": 0.9155},
        },
    }
    rows = raw_json_to_rows(raw)
    currencies = sorted(r["currency"] for r in rows)
    assert currencies == ["EUR", "MXN"]
    assert all(r["base_currency"] == "USD" for r in rows)
    assert all(r["date"] == "2024-01-02" for r in rows)
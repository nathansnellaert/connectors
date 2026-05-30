"""Post-DAG health invariants for the Cboe Global Markets download step.

Catches silent degradation that file-existence alone misses: empty payloads,
truncated downloads, wrong format. Thresholds are floored well below observed
volumes so they only trip on real breakage.
"""

from subsets_utils import load_raw_parquet, load_raw_ndjson

INDEX_SPEC = "cboe-global-markets-index-daily-prices"
# Everything else is written via save_raw_ndjson.
NDJSON_MIN_ROWS = {
    "cboe-global-markets-cfe-vix-futures-archive": 500,
    "cboe-global-markets-equities-market-volume-daily": 5_000,
    "cboe-global-markets-equities-market-volume-monthly": 500,
    "cboe-global-markets-options-put-call-ratios": 10_000,
    "cboe-global-markets-vix-settlement-series": 5_000,
}


def test_all_raw_assets_nonempty(spec_ids):
    """Every spec that ran must have produced rows."""
    for sid in spec_ids:
        if sid == INDEX_SPEC:
            table = load_raw_parquet(sid)
            assert len(table) > 0, f"{sid}: raw parquet has 0 rows"
        else:
            rows = load_raw_ndjson(sid)
            assert len(rows) > 0, f"{sid}: raw ndjson has 0 rows"


def test_index_shape(spec_ids):
    """Index history should be a large OHLC table with the expected columns and
    plausible values (close should be non-null for the vast majority of rows)."""
    if INDEX_SPEC not in spec_ids:
        return
    table = load_raw_parquet(INDEX_SPEC)
    assert {"symbol", "date", "open", "high", "low", "close"}.issubset(
        set(table.column_names)
    ), f"index columns: {table.column_names}"
    # Across ~hundreds of symbols with deep history this is comfortably > 100k rows.
    assert len(table) > 50_000, f"index only {len(table)} rows — likely truncated"
    n_symbols = len(set(table.column("symbol").to_pylist()))
    assert n_symbols > 100, f"index only {n_symbols} distinct symbols"
    close = table.column("close").to_pylist()
    non_null = sum(1 for v in close if v is not None)
    assert non_null > 0.9 * len(close), (
        f"index close mostly null ({non_null}/{len(close)}) — parse drift"
    )


def test_ndjson_min_rows(spec_ids):
    """Each fixed-corpus / settlement asset should clear a conservative row floor."""
    for sid, floor in NDJSON_MIN_ROWS.items():
        if sid not in spec_ids:
            continue
        rows = load_raw_ndjson(sid)
        assert len(rows) >= floor, f"{sid}: {len(rows)} rows < floor {floor}"


def test_put_call_series_complete(spec_ids):
    """All four put/call series should be present."""
    sid = "cboe-global-markets-options-put-call-ratios"
    if sid not in spec_ids:
        return
    rows = load_raw_ndjson(sid)
    series = {r.get("series") for r in rows}
    assert {"total", "equity", "index", "vix"}.issubset(series), (
        f"missing put/call series: {series}"
    )

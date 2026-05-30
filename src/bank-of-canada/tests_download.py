"""Post-DAG health invariants for the Bank of Canada download step.

Catches silent degradation that file-existence alone misses: empty catalog,
truncated firehose, or an observation payload whose shape drifted away from the
generic {series_id, dim_key, dim_value, value} contract.
"""

from subsets_utils import load_raw_parquet, load_raw_ndjson, list_raw_files


def test_series_catalog_populated():
    """The series catalog should list a large, stable corpus (~15k+ series)."""
    table = load_raw_parquet("bank-of-canada-series")
    assert len(table) > 10_000, (
        f"series catalog has only {len(table)} rows; expected >10k"
    )
    cols = set(table.column_names)
    assert {"series_id", "label", "description", "link"} <= cols, cols
    # series_id must be fully populated — it's the join key.
    sid = table.column("series_id").to_pylist()
    assert all(s for s in sid), "null/empty series_id present in catalog"
    assert len(set(sid)) == len(sid), "duplicate series_id in catalog"


def _value_batch_assets() -> list[str]:
    """Derive batch asset ids from the raw NDJSON files written for `values`."""
    files = list_raw_files("bank-of-canada-values-*")
    assets = set()
    for path in files:
        name = path.rsplit("/", 1)[-1]
        # strip the first NDJSON extension marker: "...-0007.ndjson.zst"
        if ".ndjson" in name:
            assets.add(name[: name.index(".ndjson")])
    return sorted(assets)


def test_values_firehose_populated():
    """The observation firehose should produce many batches with real rows."""
    assets = _value_batch_assets()
    assert len(assets) > 50, (
        f"only {len(assets)} value batch files; expected hundreds"
    )

    total = 0
    checked = 0
    required = {"series_id", "dim_key", "dim_value", "value"}
    # Spot-check a spread of batches rather than loading the whole corpus.
    for asset in assets[:: max(1, len(assets) // 10)]:
        rows = load_raw_ndjson(asset)
        if not rows:
            continue
        checked += 1
        total += len(rows)
        sample = rows[0]
        assert required <= set(sample.keys()), (
            f"{asset}: row missing fields, got {set(sample.keys())}"
        )
        assert sample["series_id"], f"{asset}: empty series_id"
        assert sample["dim_key"], f"{asset}: empty dim_key"

    assert checked > 0, "no non-empty value batches found"
    assert total > 0, "value batches contained zero observation rows"

"""Post-DAG health invariants for the FTC download step.

Two entities with different raw layouts:
  - ftc-hsr-early-termination-notices : ONE ndjson asset (stateless full re-pull).
  - ftc-dnc-complaints                : a firehose written as seq-range BATCH
    files `ftc-dnc-complaints-<lo>-<hi>.ndjson.zst`; the bare spec id is a prefix,
    not a file. One run writes a bounded slice (possibly partial), so we assert
    structure + non-emptiness rather than a full-corpus count.
"""
from subsets_utils import load_raw_ndjson, list_raw_files

HSR_ID = "ftc-hsr-early-termination-notices"
DNC_PREFIX = "ftc-dnc-complaints-"


def test_hsr_nonempty_and_structured():
    """HSR is a small full snapshot; an empty or shapeless payload means the
    JSON:API envelope changed or auth/pagination broke silently."""
    rows = load_raw_ndjson(HSR_ID)
    assert len(rows) > 0, f"{HSR_ID}: raw ndjson has 0 rows"
    r = rows[0]
    for col in ("id", "transaction-number", "created"):
        assert col in r, f"{HSR_ID}: record missing '{col}' (got {sorted(r)[:12]})"
    # ids identify notices; they must be present on every row.
    assert all(row.get("id") for row in rows), f"{HSR_ID}: some rows have empty id"


def test_dnc_batches_nonempty_and_structured():
    """DNC firehose must have produced at least one non-empty batch file with the
    seq cursor field intact — empty batches mean the offset/seq cursor broke."""
    files = list_raw_files(f"{DNC_PREFIX}*.ndjson.zst")
    assert files, "no ftc-dnc-complaints-*.ndjson.zst batch files were written"
    seen = 0
    for f in files:
        asset_id = f[: -len(".ndjson.zst")]
        rows = load_raw_ndjson(asset_id)
        assert len(rows) > 0, f"{f}: batch has 0 rows"
        r = rows[0]
        assert "id" in r and r.get("id"), f"{f}: record missing id"
        assert isinstance(r.get("seq"), int), f"{f}: 'seq' is not an int (got {r.get('seq')!r})"
        seen += len(rows)
    assert seen > 0, "DNC batches present but collectively empty"

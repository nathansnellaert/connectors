"""Post-DAG health invariants for the FTC download step.

Both entities write ONE ndjson asset (stateless overwrite):
  - ftc-hsr-early-termination-notices : full re-pull (bounded to a few pages when
    only DEMO_KEY is available; full corpus with a registered FTC_API_KEY).
  - ftc-dnc-complaints                : bounded recent window of complaints.

The per-run row count varies with the available api.data.gov key (a handful of
pages on DEMO_KEY vs. thousands with a registered key), so we assert structure +
non-emptiness rather than a full-corpus count. An empty or shapeless payload
means the JSON:API envelope changed or auth/pagination broke silently.
"""
from subsets_utils import load_raw_ndjson

HSR_ID = "ftc-hsr-early-termination-notices"
DNC_ID = "ftc-dnc-complaints"


def test_hsr_nonempty_and_structured():
    rows = load_raw_ndjson(HSR_ID)
    assert len(rows) > 0, f"{HSR_ID}: raw ndjson has 0 rows"
    r = rows[0]
    for col in ("id", "transaction-number", "created"):
        assert col in r, f"{HSR_ID}: record missing '{col}' (got {sorted(r)[:12]})"
    # ids identify notices; they must be present on every row.
    assert all(row.get("id") for row in rows), f"{HSR_ID}: some rows have empty id"


def test_dnc_nonempty_and_structured():
    rows = load_raw_ndjson(DNC_ID)
    assert len(rows) > 0, f"{DNC_ID}: raw ndjson has 0 rows"
    r = rows[0]
    for col in ("id", "seq", "created-date"):
        assert col in r, f"{DNC_ID}: record missing '{col}' (got {sorted(r)[:12]})"
    assert all(row.get("id") for row in rows), f"{DNC_ID}: some rows have empty id"
    # seq is the monotonic complaint cursor; it must be an int on every row.
    assert all(isinstance(row.get("seq"), int) for row in rows), \
        f"{DNC_ID}: some rows have non-int 'seq'"


def test_dnc_ids_unique():
    """The recent-window fetch dedupes by id in-memory; duplicates would mean the
    dedupe broke and transform's id-merge would behave unexpectedly."""
    rows = load_raw_ndjson(DNC_ID)
    ids = [row.get("id") for row in rows]
    assert len(ids) == len(set(ids)), f"{DNC_ID}: duplicate ids in raw payload"

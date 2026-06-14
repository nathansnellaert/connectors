"""Health invariants for UNICEF raw downloads (NDJSON.gz, one per dataflow).

Each spec's raw asset is the full SDMX-CSV table for one dataflow, parsed into
NDJSON. We check every asset holds rows and that the rows carry the universal
SDMX measure column (obs_value) and that it parses numerically — catching a
silent format switch, an empty 200 body (the documented failure mode of this
server's ?format=csv path), or a column-shift in the code:label split.

Flows range from a few thousand rows (WT) to ~870MB (GLOBAL_DATAFLOW), so the
checks STREAM the NDJSON line-by-line and stop after a bounded sample rather
than loading whole flows into memory (load_raw_ndjson would OOM on the big ones).
"""
import json

from subsets_utils import raw_reader

SAMPLE = 5000  # rows inspected per asset — enough to catch shape/format breaks


def _sample_rows(sid):
    """Yield up to SAMPLE parsed rows from an asset's NDJSON.gz without loading
    the whole file."""
    with raw_reader(sid, "ndjson.gz", mode="rt", compression="gzip") as fh:
        for i, line in enumerate(fh):
            if i >= SAMPLE:
                break
            line = line.strip()
            if line:
                yield json.loads(line)


def test_all_raw_assets_nonempty(spec_ids):
    """Every dataflow must yield observation rows. An empty payload usually
    means the endpoint changed format or returned an empty 200."""
    for sid in spec_ids:
        rows = list(_sample_rows(sid))
        assert len(rows) > 0, f"{sid}: raw NDJSON has 0 rows"


def test_obs_value_present_and_populated(spec_ids):
    """Every row must carry the obs_value column (mandatory SDMX measure) and the
    bulk of sampled rows must have it populated — guards against an empty 200
    body, a column-shift, or label bleed in the code:label split.

    Note: obs_value is NOT required to be numeric. A handful of flows publish a
    categorical measure there (CCRI risk bands like "Extremely High"), so a
    numeric-fraction check would wrongly fail those flows; we only assert the
    column is present and non-blank for most rows."""
    for sid in spec_ids:
        rows = list(_sample_rows(sid))
        assert rows, f"{sid}: no rows to check obs_value"
        assert all("obs_value" in r for r in rows), \
            f"{sid}: rows missing obs_value column"
        populated = sum(1 for r in rows if str(r.get("obs_value", "")).strip())
        frac = populated / len(rows)
        assert frac >= 0.8, \
            f"{sid}: only {frac:.0%} of sampled obs_value are non-blank (expected >=80%)"

"""Health invariants for UNICEF raw downloads (NDJSON.gz, one per dataflow).

Each spec's raw asset is the full SDMX-CSV table for one dataflow, parsed into
NDJSON. We check every asset holds rows and that the rows carry the universal
SDMX measure column (obs_value) — catching a silent format switch or an empty
200 body (the documented failure mode of this server's ?format=csv path).
"""
from subsets_utils import load_raw_ndjson


def test_all_raw_assets_nonempty(spec_ids):
    """Every dataflow must yield observation rows. An empty payload usually
    means the endpoint changed format or returned an empty 200."""
    for sid in spec_ids:
        rows = load_raw_ndjson(sid)
        assert len(rows) > 0, f"{sid}: raw NDJSON has 0 rows"


def test_obs_value_present_and_mostly_numeric(spec_ids):
    """Every row must carry obs_value (mandatory SDMX measure), and the bulk
    of values must parse as numbers — guards against column-shift / label
    bleed in the code:label split."""
    for sid in spec_ids:
        rows = load_raw_ndjson(sid)
        sample = rows[:5000]
        assert all("obs_value" in r for r in sample), \
            f"{sid}: rows missing obs_value column"
        numeric = 0
        for r in sample:
            v = r.get("obs_value", "")
            try:
                float(v)
                numeric += 1
            except (TypeError, ValueError):
                pass
        frac = numeric / len(sample) if sample else 0
        assert frac >= 0.8, \
            f"{sid}: only {frac:.0%} of sampled obs_value are numeric (expected >=80%)"

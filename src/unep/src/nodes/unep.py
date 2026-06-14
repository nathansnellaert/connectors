"""UNEP WESR SDG / statistical indicator values connector.

One published subset — ``values`` — the long-format numeric observations across
every WESR SDG/statistical indicator (indicator_id, geography, year, value).

Mechanism (per research handoff). The chosen mechanism is the WESR CKAN
catalogue (``wesr-search.unep.org/ckan``), which is metadata-only: it enumerates
the statistical indicators (``data_type:statistical``, package ``sdg-indicator_id-<id>``)
and carries indicator_id in ``extras``, but holds NO numeric values. Research
flagged that every statistical resource is an HTML pointer to the WESR SDG-data
viewer (``wesr.unep.org/apps/sdg-data/indicator=<id>``) and that THAT deployment
was returning HTTP 500 (PHP platform error) as of probing — i.e. the numbers
were not downloadable there.

During this step a *working* deployment of the very same SDG-data viewer app was
found at ``sdgs.unep.org/sdg-data``, keyed by the identical ``indicator_id``.
Its JSON API exposes the observations:

  * ``/getindicator_details/<id>``           -> {country:[...], region:[...]}
        availability records {country_id, year_start, country_name} (no value)
  * ``/getchartdata/<cids>/<id>/<years>``    -> {country:[{... observation ...}]}
        the actual numeric ``observation`` for each (country_id, year) pair; the
        comma-separated lists let one request return every observation for an
        indicator at once. The flat ``country`` array is the authoritative
        observation list (it includes regional aggregates such as "World").

So we use CKAN exactly as research directed to enumerate indicator_ids, and read
the values from the SDG-data viewer surface research pointed at — at a live
mirror that resolves the access gap.

Shape: stateless full re-pull. ~1.9k indicators, two-ish HTTP calls each; the
whole corpus is re-fetched every refresh and overwritten. No watermark — late
revisions are picked up for free. Freshness gating is the maintain step's job.
"""
from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import httpx
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from subsets_utils import (
    NodeSpec,
    SqlNodeSpec,
    get,
    load_state,
    save_raw_ndjson,
    save_state,
)

CKAN = "https://wesr-search.unep.org/ckan/api/3/action/package_search"
SDG = "https://sdgs.unep.org/sdg-data"

# How many (country_id, year) pairs to pack into one getchartdata URL. Keeps the
# request path comfortably under typical ~8KB server limits (a pair is ~10 chars).
CHUNK = 120
# Concurrency over indicators. No rate limit is documented and none was hit while
# probing; 8 is gentle and keeps the full corpus to a few minutes.
WORKERS = 8
# Systemic-failure ceiling. Sporadic per-indicator failures are logged and
# skipped; if more than this FRACTION of indicators fail, the source is broken
# in a way that must surface — raise instead of publishing a gutted table.
MAX_FAIL_FRACTION = 0.20

_TRANSIENT_EXC = (
    httpx.ConnectError,
    httpx.ConnectTimeout,
    httpx.ReadTimeout,
    httpx.WriteTimeout,
    httpx.PoolTimeout,
    httpx.RemoteProtocolError,
    httpx.ProxyError,
)


def _is_transient(exc: BaseException) -> bool:
    if isinstance(exc, _TRANSIENT_EXC):
        return True
    if isinstance(exc, httpx.HTTPStatusError):
        code = exc.response.status_code
        return code == 429 or 500 <= code < 600
    return False


@retry(
    retry=retry_if_exception(_is_transient),
    stop=stop_after_attempt(6),
    wait=wait_exponential(min=2, max=60),
    reraise=True,
)
def _get_json(url: str, params: dict | None = None):
    resp = get(url, params=params, timeout=(10.0, 120.0))
    resp.raise_for_status()
    return resp.json()


def _enumerate_indicator_ids() -> list[str]:
    """Page CKAN package_search (data_type:statistical) and collect the
    indicator_id carried in each package's extras. ~1.9k of the 3.3k statistical
    packages carry an indicator_id (the rest are document/webpage strays)."""
    ids: list[str] = []
    seen: set[str] = set()
    start, rows = 0, 1000
    while True:
        res = _get_json(
            CKAN,
            params={"fq": "data_type:statistical", "rows": rows, "start": start},
        )["result"]
        total = res["count"]
        results = res["results"]
        for pkg in results:
            extras = {e["key"]: e["value"] for e in pkg.get("extras", [])}
            iid = extras.get("indicator_id")
            if iid and str(iid).isdigit() and iid not in seen:
                seen.add(iid)
                ids.append(iid)
        start += rows
        if start >= total or not results:
            break
    if not ids:
        raise RuntimeError("CKAN returned no statistical indicator_ids")
    return ids


def _fetch_indicator(indicator_id: str) -> list[dict]:
    """All numeric observations for one indicator, as flat row dicts.

    Two calls: availability (country_id, year) pairs, then chunked getchartdata
    to resolve the values. Returns [] when the indicator carries no data.
    """
    detail = _get_json(f"{SDG}/getindicator_details/{indicator_id}")
    pairs = [
        (rec["country_id"], rec["year_start"])
        for rec in (detail.get("country") or []) + (detail.get("region") or [])
        if rec.get("country_id") is not None and rec.get("year_start") is not None
    ]
    if not pairs:
        return []

    # Dedup keeps the chart URLs short and the row set clean; SQL also dedups.
    out: dict[tuple, dict] = {}
    for i in range(0, len(pairs), CHUNK):
        chunk = pairs[i : i + CHUNK]
        cids = ",".join(str(c) for c, _ in chunk)
        yrs = ",".join(str(y) for _, y in chunk)
        data = _get_json(f"{SDG}/getchartdata/{cids}/{indicator_id}/{yrs}")
        for row in data.get("country") or []:
            cid = row.get("country_id")
            yr = row.get("year")
            obs = row.get("observation")
            if cid is None or yr is None or obs is None:
                continue
            out[(cid, yr)] = {
                "indicator_id": int(indicator_id),
                "indicator_name": row.get("indicator_name"),
                "country_id": cid,
                "country_name": row.get("country_name"),
                "year": yr,
                "observation": obs,
            }
    return list(out.values())


def fetch_values(node_id: str) -> None:
    asset = node_id  # the runtime hands us the spec id; it IS the asset name

    indicator_ids = _enumerate_indicator_ids()
    n = len(indicator_ids)
    print(f"unep: enumerated {n} statistical indicators from CKAN", flush=True)

    rows: list[dict] = []
    failures: list[str] = []
    done = 0
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = {
            pool.submit(_fetch_indicator, iid): iid for iid in indicator_ids
        }
        for fut in as_completed(futures):
            iid = futures[fut]
            done += 1
            try:
                rows.extend(fut.result())
            except httpx.HTTPStatusError as exc:
                # Permanent (4xx) or retry-exhausted: skip this indicator, but
                # keep crawling the rest.
                failures.append(iid)
                print(
                    f"unep: indicator {iid} failed "
                    f"({type(exc).__name__} {exc.response.status_code}); skipping",
                    flush=True,
                )
            except _TRANSIENT_EXC as exc:
                failures.append(iid)
                print(
                    f"unep: indicator {iid} failed ({type(exc).__name__}); skipping",
                    flush=True,
                )
            if done % 200 == 0:
                print(
                    f"unep: {done}/{n} indicators, {len(rows)} observations so far",
                    flush=True,
                )

    if failures and len(failures) > MAX_FAIL_FRACTION * n:
        raise RuntimeError(
            f"unep: {len(failures)}/{n} indicators failed "
            f"(> {MAX_FAIL_FRACTION:.0%}); upstream looks broken"
        )

    save_raw_ndjson(rows, asset)

    state = load_state(asset)
    state["last_run_stats"] = {
        "indicators": n,
        "indicators_failed": len(failures),
        "records": len(rows),
    }
    save_state(asset, state)
    print(
        f"unep: wrote {len(rows)} observations "
        f"({len(failures)} indicators skipped)",
        flush=True,
    )


DOWNLOAD_SPECS = [
    NodeSpec(id="unep-values", fn=fetch_values, kind="download"),
]


TRANSFORM_SPECS = [
    SqlNodeSpec(
        id=f"{s.id}-transform",
        deps=[s.id],
        sql=f'''
            SELECT DISTINCT
                CAST(indicator_id AS BIGINT)   AS indicator_id,
                indicator_name,
                CAST(country_id AS BIGINT)     AS area_id,
                country_name                   AS area_name,
                CAST(year AS INTEGER)          AS year,
                CAST(observation AS DOUBLE)    AS value
            FROM "{s.id}"
            WHERE observation IS NOT NULL
              AND year IS NOT NULL
        ''',
    )
    for s in DOWNLOAD_SPECS
]

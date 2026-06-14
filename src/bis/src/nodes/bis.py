"""BIS bulk-CSV connector.

One download node per BIS dataflow (entity union). For each dataflow we fetch
the persistent bulk export ``https://data.bis.org/static/bulk/{ID}_csv_flat.zip``
(the *flat* / tidy-long variant — the ``_csv_col`` variant pivots every time
period into its own column, producing 30k+ columns, so it is unusable). Each zip
holds a single CSV with SDMX-style columns ``CODE:Label`` (e.g.
``FREQ:Frequency``, ``TIME_PERIOD:Time period or range``,
``OBS_VALUE:Observation Value``). The dimension set differs per dataflow (each
has its own DSD), so raw is written as NDJSON (drifty schema) with the column
codes stripped to the bare SDMX code as keys.

Strategy: stateless full re-pull. Each zip is the entire history of its topic and
URLs are persistent across releases, so we re-fetch and overwrite every run; the
maintain step gates whether a given fetch runs. The largest dataflow
(WS_LBS_D_PUB) is a ~356MB zip / multi-GB CSV, so parsing is streamed straight
from the zip member into a gzipped NDJSON writer — never materialised in memory.
"""

import csv
import io
import json
import time
import zipfile

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
    raw_writer,
    save_state,
)

STATE_VERSION = 1
BASE_URL = "https://data.bis.org/static/bulk"
SKIP_TTL_SECONDS = 14 * 86400

# The entity union — BIS dataflow ids that rank scored at/above the publish
# threshold. One download + one transform per id.
ENTITY_IDS = [
    "WS_CBPOL",
    "WS_CBS_PUB",
    "WS_CBTA",
    "WS_CPMI_CASHLESS",
    "WS_CPMI_CT1",
    "WS_CPMI_CT2",
    "WS_CPMI_DEVICES",
    "WS_CPMI_INSTITUT",
    "WS_CPMI_MACRO",
    "WS_CPMI_PARTICIP",
    "WS_CPMI_SYSTEMS",
    "WS_CPP",
    "WS_CREDIT_GAP",
    "WS_DEBT_SEC2_PUB",
    "WS_DER_OTC_TOV",
    "WS_DPP",
    "WS_DSR",
    "WS_EER",
    "WS_GLI",
    "WS_LBS_D_PUB",
    "WS_LONG_CPI",
    "WS_NA_SEC_DSS",
    "WS_OTC_DERIV2",
    "WS_SPP",
    "WS_TC",
    "WS_XRU",
    "WS_XTD_DERIV",
]


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
    wait=wait_exponential(min=4, max=120),
    reraise=True,
)
def _fetch_zip(url: str) -> bytes:
    # read timeout is per-chunk, generous enough for the ~356MB LBS export.
    resp = get(url, timeout=(10.0, 300.0))
    resp.raise_for_status()
    return resp.content


def _entity_id(node_id: str) -> str:
    return node_id[len("bis-"):].upper().replace("-", "_")


def fetch_one(node_id: str) -> None:
    asset = node_id  # the runtime passes the spec id; it IS the asset name
    eid = _entity_id(node_id)
    url = f"{BASE_URL}/{eid}_csv_flat.zip"

    try:
        content = _fetch_zip(url)
    except httpx.HTTPStatusError as exc:
        code = exc.response.status_code
        if code != 429 and 400 <= code < 500:
            # Permanent: this dataflow's bulk export is gone. Mark skipped with a
            # TTL and return so one bad entity doesn't sink the whole step.
            print(f"{asset}: permanent HTTP {code} for {url}; skipping", flush=True)
            state = load_state(asset)
            skipped = state.get("skipped", {})
            skipped[eid] = {
                "reason": f"HTTP {code}",
                "expires_at": int(time.time()) + SKIP_TTL_SECONDS,
            }
            state["skipped"] = skipped
            state["schema_version"] = STATE_VERSION
            save_state(asset, state)
            return
        raise

    zf = zipfile.ZipFile(io.BytesIO(content))
    members = zf.namelist()
    if not members:
        raise AssertionError(f"{asset}: empty zip from {url}")
    member = members[0]

    n = 0
    # Stream straight from the zip member into gzipped NDJSON — never build the
    # full (multi-GB) CSV or row list in memory.
    with raw_writer(asset, "ndjson.gz", mode="wt", compression="gzip") as out:
        with zf.open(member) as fh:
            text = io.TextIOWrapper(fh, encoding="utf-8-sig", newline="")
            reader = csv.reader(text)
            header = next(reader)
            # SDMX bulk headers are "CODE:Label" — keep just the code as the key.
            keys = [h.split(":", 1)[0].strip() for h in header]
            try:
                obs_idx = keys.index("OBS_VALUE")
            except ValueError:
                raise AssertionError(
                    f"{asset}: OBS_VALUE column missing, header={keys}"
                )
            ncols = len(keys)
            for row in reader:
                if len(row) != ncols:
                    continue  # malformed line
                if not row[obs_idx]:
                    continue  # series-definition row with no observation
                rec = {k: (v if v != "" else None) for k, v in zip(keys, row)}
                out.write(json.dumps(rec, ensure_ascii=False))
                out.write("\n")
                n += 1
                if n % 500000 == 0:
                    print(f"{asset}: {n} observations", flush=True)

    print(f"{asset}: wrote {n} observations from {url}", flush=True)

    # Raw written first; record run stats afterwards.
    state = load_state(asset)
    state["schema_version"] = STATE_VERSION
    state["last_run_stats"] = {"records": n, "url": url}
    save_state(asset, state)


DOWNLOAD_SPECS: list[NodeSpec] = [
    NodeSpec(
        id=f"bis-{eid.lower().replace('_', '-')}",
        fn=fetch_one,
        kind="download",
    )
    for eid in ENTITY_IDS
]


# One published Delta table per dataflow. The dimension columns differ per
# dataflow, so the SQL is generic: drop the constant SDMX envelope columns
# (STRUCTURE, ACTION), cast OBS_VALUE to DOUBLE, and keep every observation that
# carries a numeric value. TIME_PERIOD is left as text because its granularity
# ranges from annual ("1948") to daily ("2002-06-03") across dataflows.
TRANSFORM_SPECS: list[SqlNodeSpec] = [
    SqlNodeSpec(
        id=f"{s.id}-transform",
        deps=[s.id],
        sql=f'''
            SELECT * EXCLUDE (STRUCTURE, ACTION)
                   REPLACE (TRY_CAST(OBS_VALUE AS DOUBLE) AS OBS_VALUE)
            FROM "{s.id}"
            WHERE TRY_CAST(OBS_VALUE AS DOUBLE) IS NOT NULL
        ''',
    )
    for s in DOWNLOAD_SPECS
]

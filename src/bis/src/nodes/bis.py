"""BIS (Bank for International Settlements) — download step.

Mechanism: bulk_csv. One zipped CSV per BIS statistical dataflow, served at a
persistent URL: https://data.bis.org/static/bulk/{DATAFLOW_ID}_csv_flat.zip

We fetch the *flat* (long / tidy) SDMX variant, whose columns are the standard
TIME_PERIOD / OBS_VALUE long form described in the research handoff. The exact
dimension columns differ per dataflow (each dataflow has its own DSD — e.g.
WS_CBPOL exposes FREQ/REF_AREA while WS_CPMI_MACRO exposes BIS_TOPIC/REP_CTY/
BIS_SUFFIX), so there is no single stable tabular schema across entities.

Raw shape: we store the downloaded **zip bytes** verbatim via save_raw_file.
The flat CSVs uncompress to very large files (WS_LBS_D_PUB is ~356MB zipped,
multi-GB unzipped), so unzipping + parsing into a typed table here would be a
memory hazard and would force a per-dataflow schema that is genuinely the
transform step's concern. Storing the compressed zip keeps each fetch's peak
RSS bounded by the compressed size and preserves full fidelity.

Fetch shape: stateless full re-pull (shape 1). Each zip is the entire history
for its topic, refreshed on a per-topic schedule; URLs are persistent across
releases. There is no incremental/delta query, so every invocation overwrites
the whole asset. Freshness gating is the MaintainSpec's job, not ours.
"""

import time

import httpx
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from subsets_utils import NodeSpec, get, save_raw_file, load_state, save_state

# The entity union — authoritative coverage target, copied verbatim from
# data/sources/bis/steps/6e5ac3aeadba4795a858fd312fc7fac8/entity_union.json
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

STATE_VERSION = 1
_SKIP_TTL_SECONDS = 14 * 86400  # permanent-failure markers expire after 14 days

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


def _dataflow_id(node_id: str) -> str:
    """Recover the BIS dataflow id from a spec id.

    Spec ids are f"bis-{entity.lower().replace('_','-')}"; BIS dataflow ids are
    all-uppercase with underscores and contain no hyphens, so the reverse is
    unambiguous.
    """
    return node_id[len("bis-"):].upper().replace("-", "_")


@retry(
    retry=retry_if_exception(_is_transient),
    stop=stop_after_attempt(6),
    wait=wait_exponential(min=4, max=120),
    reraise=True,
)
def _download_zip(url: str) -> bytes:
    # (connect, read) — read is generous: WS_LBS_D_PUB is a ~356MB body.
    resp = get(url, timeout=(10.0, 600.0))
    resp.raise_for_status()
    return resp.content


def fetch_one(node_id: str) -> None:
    asset = node_id  # the runtime passes the spec id; it IS the asset name
    dataflow = _dataflow_id(node_id)
    url = f"https://data.bis.org/static/bulk/{dataflow}_csv_flat.zip"

    # Expire any stale skipped marker for this asset before deciding.
    state = load_state(asset)
    if state.get("schema_version") != STATE_VERSION:
        state = {}
    skip = state.get("skipped")
    now = int(time.time())
    if skip and skip.get("expires_at", 0) > now:
        print(
            f"{asset}: skip marker active until {skip['expires_at']} "
            f"(reason: {skip.get('reason')}); not retrying",
            flush=True,
        )
        return

    try:
        content = _download_zip(url)
    except httpx.HTTPStatusError as exc:
        code = exc.response.status_code
        # Permanent client errors (404, 403, ...) — mark skipped with a TTL so
        # source recovery is automatic, and return without raising out.
        if 400 <= code < 500 and code != 429:
            print(
                f"{asset}: permanent HTTP {code} on {url}; writing skip marker",
                flush=True,
            )
            save_state(
                asset,
                {
                    "schema_version": STATE_VERSION,
                    "skipped": {
                        "reason": f"HTTP {code} on {url}",
                        "expires_at": now + _SKIP_TTL_SECONDS,
                    },
                },
            )
            return
        raise

    # Write raw FIRST, then state.
    save_raw_file(content, asset, extension="zip")
    save_state(
        asset,
        {
            "schema_version": STATE_VERSION,
            "last_run_stats": {"bytes": len(content), "url": url},
        },
    )
    print(f"{asset}: saved {len(content)} zip bytes from {url}", flush=True)


DOWNLOAD_SPECS = [
    NodeSpec(
        id=f"bis-{eid.lower().replace('_', '-')}",
        fn=fetch_one,
        kind="download",
    )
    for eid in ENTITY_IDS
]

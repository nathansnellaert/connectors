"""Bundesbank download — per-dataflow whole-corpus snapshots via SDMX 2.1 REST.

Mechanism (chosen by research): sdmx_21 at api.statistiken.bundesbank.de.
For each of the 86 publishable dataflows we issue ONE request:

    GET /rest/data/{flowRef}
    Accept: application/vnd.bbk.data+csv-zip;version=1.0.0

and persist the returned ZIP verbatim.

Why the ZIP content type rather than `?format=csv`: the plain Bundesbank-CSV
surface enforces a 200-series ceiling and returns HTTP 406 for any real
dataflow ("Csv time series limit of 200 exceeded. Request same time series
with content type application/vnd.bbk.data+csv-zip..."). The SDMX-CSV
whole-dataflow surface works but is enormous uncompressed (BBEX3 alone is
~2.3 GB of text). The ZIP variant is the only path that returns the *entire*
dataflow in one bounded, compressed payload (BBEX3 = 8.8 MB zip / 111 MB
across 19 member CSVs; smaller flows are tens of KB).

Each ZIP holds one-or-more member CSVs in Bundesbank's wide layout (columns =
series keys plus per-series *_FLAGS, rows = time periods preceded by metadata
rows; ';' separator). Schemas differ per dataflow, so raw is stored as opaque
ZIP bytes (save_raw_file) and the transform step owns parsing.

Fetch shape: stateless full re-pull. Each dataflow is small-to-moderate as a
ZIP and is re-fetched whole every refresh — overwriting the previous snapshot.
This picks up revisions and late corrections for free; there is no usable
incremental delta surface (only startPeriod/endPeriod date windows, which we
don't need for a full-corpus snapshot). No auth, no documented rate limit.

Memory note: subsets_utils.get buffers the full response body, so the working
ceiling per spec is one dataflow's ZIP held in RAM. ZIP compression keeps this
bounded (single-digit-MB to low-tens-of-MB even for the large flows); read
timeout is set generously (300 s) because big flows stream slowly.
"""
import io
import zipfile

import httpx
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from subsets_utils import NodeSpec, get, save_raw_file

BASE = "https://api.statistiken.bundesbank.de/rest"
# Bundesbank-CSV-as-ZIP: the only content type that returns a whole dataflow.
ZIP_ACCEPT = "application/vnd.bbk.data+csv-zip;version=1.0.0"

# The entity union (authoritative coverage target) — 86 SDMX dataflow ids,
# copied from entity_union.json. Each maps to one download spec.
ENTITY_IDS = [
    "BBAF3", "BBAI3", "BBAPV", "BBASV", "BBBEK1", "BBBEK2", "BBBEK3", "BBBEK4",
    "BBBEK5", "BBBK1", "BBBK10", "BBBK11", "BBBK12", "BBBK13", "BBBK2", "BBBK20",
    "BBBK3", "BBBK4", "BBBK5", "BBBK6", "BBBK7", "BBBK8", "BBBK9", "BBBP1",
    "BBBPS", "BBBS2", "BBBU2", "BBBZ1", "BBDA1", "BBDB2", "BBDE1", "BBDG1",
    "BBDL1", "BBDP1", "BBDR1", "BBDY1", "BBDZ1", "BBEE1", "BBEE5", "BBEX3",
    "BBFBOPV", "BBFEPOEV", "BBFFDIPV", "BBFFDITV", "BBFI1", "BBGFS1", "BBIB1",
    "BBIG1", "BBIM1", "BBIN1", "BBK10", "BBMF1", "BBMFK1", "BBMMB", "BBMME",
    "BBMMS", "BBMMU", "BBNZ1", "BBSAP", "BBSDI", "BBSDP", "BBSEI", "BBSF2",
    "BBSF3", "BBSHI", "BBSIS", "BBSSY", "BBUMF", "BBWCAF1", "BBXE1", "BBXF1",
    "BBXL3", "BBXN1", "BBXP1", "BBXP2", "BBXS1", "BBZVS01", "BBZVS02", "BBZVS03",
    "BBZVS04", "BBZVS05", "BBZVS06", "BBZVS08", "BBZVS11", "BBZVS12", "BBZVSSSI",
]

_TRANSIENT_EXC = (
    httpx.ConnectError, httpx.ConnectTimeout, httpx.ReadTimeout,
    httpx.WriteTimeout, httpx.PoolTimeout, httpx.RemoteProtocolError,
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
def _fetch_zip(flow: str) -> bytes:
    """GET one whole dataflow as a Bundesbank-CSV ZIP. Retries transient faults;
    4xx (e.g. 404 unknown flow, 406 wrong format) raise straight through."""
    url = f"{BASE}/data/{flow}"
    resp = get(url, headers={"Accept": ZIP_ACCEPT}, timeout=(10.0, 300.0))
    resp.raise_for_status()
    return resp.content


def fetch_one(node_id: str) -> None:
    asset = node_id  # the runtime passes the spec id; it IS the asset name
    # Recover the SDMX dataflow id from the spec id. Union ids are uppercase
    # and underscore-free; the convention maps '_' -> '-', so reverse that.
    flow = node_id[len("bundesbank-"):].replace("-", "_").upper()

    content = _fetch_zip(flow)

    # Honest format check: a healthy response is a non-empty ZIP. An empty body
    # or a non-ZIP payload means the surface changed silently — fail loudly
    # rather than persist garbage.
    assert content, f"{flow}: empty response body"
    assert content[:2] == b"PK", f"{flow}: response is not a ZIP (head={content[:16]!r})"
    with zipfile.ZipFile(io.BytesIO(content)) as zf:
        members = zf.namelist()
        assert members, f"{flow}: ZIP contains no members"

    save_raw_file(content, asset, extension="zip")
    print(f"{flow}: saved {len(content)} zip bytes, {len(members)} member CSV(s)", flush=True)


DOWNLOAD_SPECS = [
    NodeSpec(
        id=f"bundesbank-{eid.lower().replace('_', '-')}",
        fn=fetch_one,
        kind="download",
    )
    for eid in ENTITY_IDS
]

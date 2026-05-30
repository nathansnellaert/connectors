"""BLS LABSTAT bulk flat-file download.

Mechanism: bulk_flat_files. The entire LABSTAT corpus is published as one
sub-directory per survey under https://download.bls.gov/pub/time.series/<xx>/
with stable URLs and full history. For each survey entity we enumerate the
HTML directory autoindex and download every file it lists verbatim - the
observation partitions (xx.data.0.Current, xx.data.1..N.*), the series-level
metadata (xx.series), the code-lookup tables (xx.area / xx.item / xx.map /
xx.period / xx.footnote / ...) and the in-tree spec (xx.doc / xx.txt).
Transform parses these whitespace-delimited fixed-column ASCII files per each
survey's documentation.

The autoindex is a Windows/IIS-style listing whose rows look like:
    5/22/2026 10:00 AM   1234  <A HREF="/pub/time.series/cu/cu.series">cu.series</A>
so links are absolute paths (uppercase <A HREF=). We pull every href that
points at a file directly inside the survey directory. This is intentionally
NAMING-AGNOSTIC: most surveys ship xx.series + xx.data.N.* (some only
xx.data.1.AllData, no .0.Current; la/ has 80+ split partitions), and a few are
structurally odd (esbr/ holds only esbrNNN.sf seasonal-factor files and .gif
charts - no series/data files at all). Whatever the directory lists is what we
store.

Shape: stateless full re-pull (decision shape 1). Each survey's files are a
full snapshot regenerated on the survey's release schedule; there is no
incremental query, so every refresh re-fetches the whole survey directory and
overwrites. The largest single file is order ~250 MB (is.data.1.AllData);
files are fetched and written one at a time so subprocess RSS stays bounded.
Freshness gating (skip unchanged surveys via Last-Modified) is the
MaintainSpec's job, not this step's.

Auth quirk: the BLS Akamai WAF returns 403 for default/empty/generic-bot
User-Agent strings, so every request sends a contact-bearing ASCII User-Agent.

Raw format: opaque-ish text files with heterogeneous per-file schemas (data
files vs series vs lookups vs .sf vs .gif all differ), so each file is stored
verbatim via save_raw_file as bytes. Asset id =
f"{node_id}-{filename_with_dots_underscored}" so a survey's many files all
share the f"{node_id}-" prefix and are discoverable with list_raw_files() in
transform and tests.
"""

import re
import time
from urllib.parse import urljoin

import httpx
from ratelimit import limits, sleep_and_retry
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from subsets_utils import (
    NodeSpec,
    configure_http,
    get,
    load_state,
    save_raw_file,
    save_state,
)

STATE_VERSION = 1

BASE_URL = "https://download.bls.gov/pub/time.series/"
# Contact-identifying, ASCII-only User-Agent. The Akamai WAF 403s generic
# bot/library agents; a contact-bearing agent is the documented accepted form.
USER_AGENT = "subsets.io connector (nathan@subsets.io)"

# Permanent-skip TTL: 14 days. If a survey directory genuinely 404s we mark it
# and move on; the marker expires so source recovery needs no human.
SKIP_TTL_SECONDS = 14 * 86400

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


# ~1 request/second per process. Each NodeSpec runs in its own subprocess; with
# the DAG's concurrency held at a few specs this stays well under the Akamai WAF
# throttle threshold the research handoff flagged.
@sleep_and_retry
@limits(calls=1, period=1)
def _paced_get(url: str) -> httpx.Response:
    return get(url, timeout=(10.0, 180.0))


@retry(
    retry=retry_if_exception(_is_transient),
    stop=stop_after_attempt(6),
    wait=wait_exponential(min=4, max=120),
    reraise=True,
)
def _http_get(url: str) -> httpx.Response:
    resp = _paced_get(url)
    resp.raise_for_status()
    return resp


def _list_survey_files(entity: str) -> list[str]:
    """Enumerate the survey directory autoindex and return file basenames.

    Matches every href pointing at a file directly inside this survey dir.
    `[^"'/]+` stops at the next slash, so sub-directory links and the parent
    link are excluded. Case-insensitive (the autoindex emits <A HREF=). Returns
    whatever files the directory lists - no assumption about LABSTAT naming.
    """
    url = urljoin(BASE_URL, f"{entity}/")
    resp = _http_get(url)
    pattern = (
        r"""href\s*=\s*["']/pub/time\.series/%s/([^"'/]+)["']"""
        % re.escape(entity)
    )
    names = set(re.findall(pattern, resp.text, re.IGNORECASE))
    return sorted(names)


def _asset_id(node_id: str, filename: str) -> str:
    return f"{node_id}-{filename.replace('.', '_')}"


def fetch_one(node_id: str) -> None:
    """Download every file in one LABSTAT survey directory.

    node_id is f"bls-{entity}"; the entity is the lowercase LABSTAT survey
    abbreviation and also the directory name under BASE_URL.
    """
    configure_http(headers={"User-Agent": USER_AGENT})
    entity = node_id[len("bls-"):]

    state = load_state(node_id)
    if state.get("schema_version") != STATE_VERSION:
        state = {"schema_version": STATE_VERSION}
    skipped = {
        k: v
        for k, v in state.get("skipped", {}).items()
        if v.get("expires_at", 0) > int(time.time())
    }

    # Enumerate the survey directory. A permanent 4xx here means the survey is
    # gone - record a TTL-bound skip and return cleanly (per-entity failure
    # stays per-entity).
    try:
        filenames = _list_survey_files(entity)
    except httpx.HTTPStatusError as exc:
        code = exc.response.status_code
        if code != 429 and 400 <= code < 500:
            print(
                f"[bls] {node_id}: directory listing {code} at "
                f"{exc.request.url} - writing skipped marker",
                flush=True,
            )
            skipped[entity] = {
                "reason": f"directory listing HTTP {code}",
                "expires_at": int(time.time()) + SKIP_TTL_SECONDS,
            }
            state["skipped"] = skipped
            save_state(node_id, state)
            return
        raise

    if not filenames:
        raise AssertionError(
            f"{node_id}: survey directory {entity}/ listed zero files - "
            f"the autoindex format may have changed"
        )

    total_bytes = 0
    written = 0
    for filename in filenames:
        file_url = urljoin(BASE_URL, f"{entity}/{filename}")
        try:
            resp = _http_get(file_url)
        except httpx.HTTPStatusError as exc:
            code = exc.response.status_code
            # Permanent per-file failure: log and skip this file, keep going.
            if code != 429 and 400 <= code < 500:
                print(
                    f"[bls] {node_id}: file {filename} -> {code} at "
                    f"{file_url}, skipping file",
                    flush=True,
                )
                continue
            raise
        content = resp.content
        save_raw_file(content, _asset_id(node_id, filename), extension="txt")
        total_bytes += len(content)
        written += 1

    if written == 0:
        raise AssertionError(
            f"{node_id}: directory listed {len(filenames)} files but none "
            f"downloaded successfully"
        )

    state["skipped"] = skipped
    state["last_run_stats"] = {
        "files_listed": len(filenames),
        "files_written": written,
        "bytes": total_bytes,
    }
    save_state(node_id, state)
    print(
        f"[bls] {node_id}: wrote {written}/{len(filenames)} files, "
        f"{total_bytes} bytes",
        flush=True,
    )


# Entity union - the 33 LABSTAT surveys at/above the publish threshold. Copied
# verbatim from
# data/sources/bls/steps/166a829b808c4df580b05e48336083d8/entity_union.json
ENTITY_IDS = [
    "ap", "bd", "ca", "ce", "ci", "cm", "cu", "cw", "cx", "ei",
    "esbr", "fa", "fm", "ip", "is", "jt", "kv", "la", "le", "ln",
    "lu", "mp", "nd", "oe", "or", "pc", "pr", "sm", "su", "tu",
    "wd", "wp", "ws",
]

DOWNLOAD_SPECS = [
    NodeSpec(
        id=f"bls-{eid.lower().replace('_', '-')}",
        fn=fetch_one,
        kind="download",
    )
    for eid in ENTITY_IDS
]

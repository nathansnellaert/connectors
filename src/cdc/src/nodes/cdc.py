"""CDC (data.cdc.gov) download node — one spec per Socrata dataset.

Mechanism: Socrata. Each entity is a stable 4x4 dataset id on data.cdc.gov.
We pull the full dataset via the SoQL row endpoint
(`/resource/{four_by_four}.json`) paginating with `$limit`/`$offset` and a
stable `$order=:id` so paging is deterministic, then stream the rows to a
gzipped NDJSON raw asset.

Why NDJSON (not parquet): the 734 datasets are wildly heterogeneous — column
sets and types differ per dataset, Socrata returns every scalar as a JSON
string, and nested `Point`/`Location` fields arrive as dicts. A single shared
parquet schema is impossible and per-dataset inferred schemas are brittle, so
record-shaped NDJSON is the honest format. Transform re-types per dataset.

Why SoQL JSON paging (not the rows.csv bulk export): the research handoff
flags rows.csv as primary but prefers SoQL paging for large datasets; JSON
paging gives clean typed-key records directly (no CSV re-typing) and the same
loop handles small and large datasets uniformly while streaming to disk, so we
use it for every dataset.

Fetch shape: stateless full re-pull per dataset (overwrite). CDC datasets are
bounded snapshots/series; whole-dataset re-pull picks up revisions for free.
State is used only for TTL-bound skip markers when a dataset returns a
permanent error (404/401/403 — restricted Public Use Files), so a known-bad
dataset isn't hammered every refresh and recovers automatically after the TTL.
Freshness gating (whether this fn runs at all) is the maintain step's job.
"""

import json
import time

import httpx
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from subsets_utils import (
    NodeSpec,
    get,
    raw_writer,
    load_state,
    save_state,
)

STATE_VERSION = 1

BASE = "https://data.cdc.gov/resource"
PAGE_SIZE = 50000          # Socrata documented max page size
MAX_PAGES = 4000           # safety ceiling: 4000 * 50k = 200M rows. Raises if hit.
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


@retry(
    retry=retry_if_exception(_is_transient),
    stop=stop_after_attempt(6),
    wait=wait_exponential(min=4, max=120),
    reraise=True,
)
def _fetch_page(four_by_four: str, offset: int) -> list:
    """Fetch one page of rows. 4xx (except 429) raises HTTPStatusError, which
    the caller classifies as permanent; transient errors are retried here."""
    url = f"{BASE}/{four_by_four}.json"
    resp = get(
        url,
        params={"$limit": PAGE_SIZE, "$offset": offset, "$order": ":id"},
        timeout=(10.0, 300.0),
    )
    resp.raise_for_status()
    return resp.json()


def _is_permanent_status(exc: BaseException) -> bool:
    return (
        isinstance(exc, httpx.HTTPStatusError)
        and 400 <= exc.response.status_code < 500
        and exc.response.status_code != 429
    )


def fetch_one(node_id: str) -> None:
    asset = node_id                         # the runtime passes the spec id; it IS the asset name
    four_by_four = node_id[len("cdc-"):]    # spec id is f"cdc-{four_by_four}"

    # Honour a non-expired skip marker: a dataset that recently returned a
    # permanent error stays skipped until its TTL lapses, then is retried.
    state = load_state(asset)
    if state.get("schema_version") != STATE_VERSION:
        state = {}
    skip = state.get("skipped")
    if skip and skip.get("expires_at", 0) > time.time():
        print(f"  {asset}: skip marker active ({skip.get('reason')}), not re-fetching", flush=True)
        return

    # Probe accessibility before opening the writer so a permanent error leaves
    # no partial/empty raw file behind.
    try:
        first = _fetch_page(four_by_four, 0)
    except httpx.HTTPStatusError as exc:
        if _is_permanent_status(exc):
            code = exc.response.status_code
            print(f"  {asset}: permanent HTTP {code} from {BASE}/{four_by_four}.json — writing skip marker", flush=True)
            save_state(asset, {
                "schema_version": STATE_VERSION,
                "skipped": {
                    "reason": f"http_{code}",
                    "expires_at": int(time.time()) + SKIP_TTL_SECONDS,
                },
            })
            return
        raise

    total = 0
    pages = 0
    with raw_writer(asset, "ndjson.gz", mode="wt", compression="gzip") as f:
        rows = first
        offset = 0
        while True:
            for row in rows:
                f.write(json.dumps(row, separators=(",", ":")))
                f.write("\n")
                total += 1
            pages += 1
            if len(rows) < PAGE_SIZE:
                break
            if pages >= MAX_PAGES:
                raise RuntimeError(
                    f"{asset}: hit MAX_PAGES={MAX_PAGES} ({total:,} rows) — dataset larger "
                    f"than expected, raise the cap deliberately after review"
                )
            if pages % 10 == 0:
                print(f"  {asset}: {total:,} rows so far ({pages} pages)", flush=True)
            offset += PAGE_SIZE
            rows = _fetch_page(four_by_four, offset)

    print(f"  {asset}: wrote {total:,} rows across {pages} pages", flush=True)
    save_state(asset, {
        "schema_version": STATE_VERSION,
        "last_run_stats": {"records": total, "pages": pages},
    })


ENTITY_IDS = [
    '23gt-ssfe', '247v-f7n9', '24w5-nppr', '24xb-jxbc', '25m4-6qqq', '29hc-w46k', '29xv-7ajw',
    '2den-c3u2', '2dwv-vfam', '2ew6-ywp6', '2m7c-st88', '2m93-xvra', '2qxe-cmv4', '2snk-eav4',
    '2t2r-sf6s', '2v3t-r3np', '2vpi-n544', '2vtj-68zm', '2yum-eg9f', '32fd-hyzc', '34p9-h4us',
    '35bp-whkw', '367e-pucc', '36ue-xht5', '373s-ayzu', '37nu-tuw8', '39z2-9zu6', '3apk-4u4f',
    '3bmy-cyyd', '3crz-97tw', '3cxc-4k8q', '3h58-x6cd', '3j26-kg6d', '3myw-4j4q', '3nij-2pw6',
    '3nnj-6kcn', '3nnm-4jni', '3nzu-udr9', '3pbe-qh9z', '3q3z-9ucr', '3rge-nu2a', '3sh4-uqpm',
    '3svv-v5nh', '3ts8-hsrw', '3vxk-q2jk', '3x54-3thk', '3yf8-kanr', '45cq-cw4i', '489q-934x',
    '48mw-5apu', '4bc2-bbpq', '4bdk-kyzv', '4bft-6yws', '4day-mt2f', '4g6p-3ed6', '4jje-6vv6',
    '4khb-4xch', '4q35-rqzk', '4r2x-hcfq', '4r3g-hv9c', '4tme-u33f', '4ueh-89p9', '4va6-ph5s',
    '4xf6-nrwk', '4ynm-6jgm', '4yy2-qa9v', '4yyu-3s69', '4zxn-f9dq', '52ds-xw49', '52kb-ccu2',
    '52kh-2h7i', '533q-q3rp', '53g5-jf7x', '53mz-4zqd', '54ys-qyzm', '55uq-699y', '55yu-xksw',
    '56mi-d4wu', '57qw-ifet', '58s6-s24x', '5c6r-xi2t', '5dqz-y4ea', '5eh7-pjx8', '5hns-mwci',
    '5hvx-krph', '5i5k-6cmh', '5jp2-pgaw', '5pqj-rvh4', '5svk-8bnq', '5una-zw6e', '5wdd-3g8t',
    '5xkq-dg7x', '65mz-jvh5', '68sm-zh95', '6ie8-bpiy', '6jg4-xsqq', '6jwg-4k37', '6kf3-4udg',
    '6mjs-pnrx', '6nue-dx9c', '6p3a-6xr9', '6pdm-py4x', '6qm2-fbrx', '6rkc-nb2q', '6rvp-rahv',
    '6ryw-hetw', '6svj-q4zv', '6tn6-vc33', '6uy5-4d9d', '6vp6-wxuq', '6vqh-esgs', '6vwk-ensg',
    '6x7h-usvx', '735e-byxc', '759d-qk63', '76vv-a7x8', '783t-9j9i', '7aq9-prdf', '7b9s-s8ck',
    '7cmc-7y5g', '7ctq-myvs', '7dk4-g6vg', '7gnu-j6js', '7jik-jwvu', '7mra-9cq9', '7nbz-eajm',
    '7nwe-3aj9', '7pvw-pdbr', '7rci-qmm9', '7rih-tqi5', '7siw-u4fz', '7vg3-e5u2', '7xhe-mv2e',
    '7xva-uux8', '82ci-krud', '82nv-dn3y', '8396-v7yb', '83mw-v57c', '83ng-twza', '84rx-ksgd',
    '88eg-qzed', '89qs-mr7i', '89x6-rgq5', '89yk-m38d', '8ame-63pc', '8bda-nhxv', '8cyw-fici',
    '8dyx-9z99', '8ekv-ep3s', '8fbp-accd', '8gpz-j2fr', '8hus-y5nc', '8hxn-cvik', '8hzs-zshh',
    '8jp2-ecz7', '8miz-siyd', '8mrp-rmkw', '8na9-qgz7', '8nyy-xsq7', '8pt5-q6wp', '8v6a-z6zq',
    '8w4j-reb4', '8wmh-yzz9', '8xkx-amqh', '8xy9-ubqz', '8yup-c35n', '8zbb-qqwc', '8zea-kwnt',
    '92ri-yjps', '94wp-9pid', '95ax-ymtc', '95m5-agj4', '96sd-hxdt', '97bc-2r74', '986w-8kut',
    '9976-4iqj', '9axm-gjt8', '9b5z-wnve', '9bhg-hcku', '9cpv-whbv', '9d9z-vf8f', '9dzk-mvmi',
    '9gay-j69q', '9hdi-ekmb', '9ikp-t8tw', '9j2v-jamp', '9k8a-cbgx', '9kbf-icdi', '9mtj-y2ba',
    '9mw4-6adp', '9t9r-e5a3', '9tjt-seye', '9umn-c3jf', '9vgf-r2z6', '9x7v-wy9u', '9xb7-9z99',
    '9xc7-3a4q', '9xt5-u42s', '9y49-tura', 'a35h-9yn4', 'a3gi-4phs', 'a5a8-jsrq', 'a92y-5zud',
    'a93x-tfzm', 'a9xa-yrhn', 'abgz-qs4g', 'abzs-b3gw', 'aemk-wcbf', 'aemt-mg7g', 'aetd-68ew',
    'aewi-gwni', 'agqb-jgkw', 'agz7-4mvg', 'ahrf-yqdt', 'ai6z-tcin', 'akkj-j5ru', 'akn2-qxic',
    'akvg-8vrb', 'amjr-ph5r', 'ar8q-3jhn', 'aspp-bzzu', 'at7e-uhkc', 'atcp-73re', 'axsa-zcg5',
    'b4ji-baqh', 'b5wa-ze9s', 'b6ny-6cd5', 'b6sy-qq3u', 'b6uq-hdgz', 'b72x-p96c', 'b7pe-5nws',
    'b8tp-jsmh', 'bdyv-z46f', 'be57-s94j', 'bi63-dtpu', 'bigw-pgk2', 'biid-68vb', 'binw-6h77',
    'bk9t-cq4b', 'bkcm-ybyk', 'bqmb-vyka', 'brsb-akdp', 'bst4-hnte', 'btv3-srcc', 'bugr-bbfr',
    'bumh-rgsq', 'bw3b-karf', 'bwx3-gx66', 'bx8m-di6q', 'bxq8-mugm', 'bytj-42x7', 'bz96-hgr8',
    'c76y-7pzg', 'c7b2-4ecy', 'cah8-bpvk', 'cchw-gdwa', 'cds4-6y7t', 'cf5u-bm9w', 'ch5i-63ve',
    'ch83-ush6', 'chmz-4uae', 'ci7c-73kg', 'cj8b-94cj', 'cpdh-8cna', 'cr56-k9wj', 'ctu5-k6yz',
    'cvcu-witw', 'cw4r-vcr3', 'cwsq-ngmh', 'd2rk-yvas', 'd2tw-32xv', 'd2zt-4m8y', 'd3i6-k6z5',
    'd4v7-r7ct', 'd6p8-wqjm', 'd89q-62iu', 'd9u6-mdu6', 'de4p-4g3k', 'djj9-kh3p', 'dkyk-v5f5',
    'dmnu-8erf', 'dmzy-x2ad', 'dnnu-xtkq', 'dp9i-idru', 'dt66-w6m6', 'dttw-5yxu', 'duw2-7jbt',
    'dwmy-m9r6', 'e28h-tx85', 'e2a5-s9pr', 'e2d5-ggg7', 'e4ec-z5aa', 'e539-uadk', 'e5zk-7tx5',
    'e6et-eg6c', 'e6fc-ccez', 'e8kx-wbww', 'eanj-9nie', 'eav7-hnsx', 'eb4y-d4ic', 'edkk-ze78',
    'ee48-w5t6', 'ee83-ukst', 'efb8-zbb7', 'efqg-e273', 'egm8-9wq7', 'ekcb-r85s', 'em5e-5hvn',
    'en3s-hzsr', 'epbn-9bv3', 'espg-acwi', 'eudc-n39h', 'ewpg-rz7g', 'ex65-qa8z', 'exs3-hbne',
    'ey8b-ejrf', 'eze9-ahe5', 'ezfr-g6hf', 'f3a8-hmpp', 'f3zz-zga5', 'fdpm-fddm', 'ffbi-is3j',
    'fhky-rtsk', 'fj6i-3v3k', 'fpsi-y8tj', 'fu4u-a9bh', 'fvm6-ic5r', 'fxwg-3udm', 'fztq-uwup',
    'g2ck-geg5', 'g3c9-wbme', 'g3g2-srtq', 'g4jn-64pd', 'g57i-yx3r', 'g5fg-bgtw', 'g653-rqe2',
    'g6fu-zp23', 'g7hk-rc8d', 'ga7k-kycn', 'gb4e-yj24', 'gb67-x49c', 'gd4x-jyhw', 'gebw-t5b7',
    'gepg-djaz', 'ggsw-596z', 'gj3i-hsbz', 'gjsp-ircr', 'gpsd-ru5i', 'gr26-95h2', 'gsea-w83j',
    'gu48-2cs8', 'gvsb-yw6g', 'gxj9-t96f', 'gypc-kpgn', 'h3ej-a9ec', 'h3hw-hzvg', 'h3kf-bqpq',
    'h3my-dzpj', 'h4pd-hu6x', 'h7pm-wmjc', 'h7xa-837u', 'hbbg-vj7f', 'hbpe-6r8n', 'hc4f-j6nb',
    'hdja-ybdg', 'hea5-6w9c', 'hf2a-3ebq', 'hfr9-rurv', 'hgv5-3wrn', 'hgyx-uuxz', 'hhvg-83jq',
    'hj2x-85ya', 'hk9y-quqm', 'hkhc-f7hg', 'hkr7-mcee', 'hksd-2xuw', 'hky2-3tpn', 'hmye-mqgq',
    'hmz2-vwda', 'hn4x-zwk7', 'hrdz-jaxc', 'htq2-rqve', 'hwk8-wu83', 'hwyy-s2tt', 'hyak-nxqs',
    'i43m-djm6', 'i46a-9kgh', 'i667-sjhg', 'i6ej-9eac', 'i6u4-y3g4', 'i8t6-whzd', 'ijqb-a7ye',
    'iqm3-hbev', 'ircd-wk4g', 'isx2-c2ii', 'it4f-frdc', 'ite7-j2w7', 'ithv-4e9m', 'itia-u6fu',
    'iu3b-5ngj', 'iuq5-y9ct', 'ivdz-qhnr', 'iwxc-qftf', 'ix4g-rt8v', 'j32a-sa6u', 'j6gu-p9yd',
    'j7ym-uwqy', 'j9g8-acpt', 'jb9g-gnvr', 'jbhn-e8xn', 'jbmi-9jqv', 'jbxj-8pnr', 'jf8m-mtc3',
    'jfbs-8cpp', 'jgk8-6dpn', 'jiwm-ppbh', 'jjpx-mxt8', 'jk8p-fqhn', 'jkcx-ndu8', 'jnru-aqxk',
    'jqg8-ycmh', 'jqwm-z2g9', 'jr4g-zdpg', 'jr58-6ysp', 'ju63-2fep', 'judz-8etw', 'jwta-jxbg',
    'jxu8-x79m', 'jz6n-v26y', 'k4cb-dxd7', 'k5dc-apj8', 'k62p-6esq', 'k87d-gv3u', 'k8w5-7ju6',
    'k8wy-p9cg', 'k9zj-b28y', 'kebt-3t25', 'kee5-23sr', 'ker6-gs6z', 'kgsi-35re', 'kh8y-3es6',
    'khic-yj26', 'kipu-qxy8', 'kk8c-wtm4', 'kkix-nh4v', 'km4m-vcsb', 'km5s-4339', 'kmap-fsfn',
    'kmvs-jkvx', 'kmxt-xb3i', 'kn79-hsxy', 'knu9-e7pg', 'kp49-9dp8', 'kpbd-vsd5', 'krhz-spsc',
    'krqc-563j', 'ks3g-spdg', 'ksfb-ug5d', 'ku7p-zn4c', 'kusj-ex57', 'kvib-3txy', 'kwbr-syv2',
    'kxvg-q6s7', 'kyph-4i8d', 'm35w-spkz', 'm74n-4hbs', 'm8jv-u92i', 'mb5y-ytti', 'mc4y-cbbv',
    'mcdp-77g7', 'mcvh-596h', 'mdwz-ar4b', 'mfvi-hkb9', 'mk5r-qxdg', 'mnaa-qctp', 'mpgq-jmmr',
    'mpx5-t7tu', 'mqmc-4b9n', 'mr4u-abm2', 'mrip-2k2a', 'msnx-y6hi', 'mtgp-t7vw', 'mtpu-urpp',
    'muep-c3qd', 'muzy-jte6', 'mvaf-qxac', 'mwkk-wzmy', 'mzhq-xsdd', 'n2x4-haas', 'n2zz-25mk',
    'n322-ce6f', 'n5b3-jati', 'n5qs-vw3x', 'n83i-w4cq', 'n8mc-b4w4', 'n97r-u9uh', 'ncvk-7amm',
    'ndai-i7s4', 'ne52-uraz', 'nfuu-hu6j', 'ngaa-n8ir', 'njmz-dpbc', 'nkr7-scx6', 'nqu5-vn7d',
    'nr42-fsyk', 'nr4s-juj3', 'nra9-vzzn', 'nsxk-tvbw', 'ntaa-dtex', 'nu3s-3dwd', 'nw2y-v4gm',
    'p4r5-qsgs', 'p89x-xx88', 'paqx-33a8', 'pbq2-7wr2', 'pd5g-36s6', 'ph8r-wzxn', 'piju-vf3p',
    'pj7m-y5uh', 'pjb2-jvdr', 'pjtk-n43k', 'pp7x-dyj2', 'ppmd-3u54', 'pqn7-e45s', 'pqpp-u99h',
    'psx4-wq38', 'pttf-ck53', 'pvk6-8bzd', 'pwn4-m3yp', 'q2dj-esu7', 'q3t8-zr7t', 'q84f-e68r',
    'q8ig-wwk9', 'q8j9-sue7', 'q9mh-h2tw', 'qbrk-85z2', 'qcai-zfj9', 'qeq7-f3ir', 'qeru-k2y2',
    'qfhf-uhaa', 'qfiq-jir6', 'qgfq-p8ib', 'qktg-6dmb', 'qnzd-25i4', 'qr63-vqq5', 'qtbi-xd4i',
    'qve4-fp9c', 'qvzb-qs6p', 'qz99-wyhv', 'r229-z6ma', 'r5pw-bk5t', 'r85e-hjic', 'r8kw-7aab',
    'r9mz-pvtk', 'rbrz-y4zd', 'rcdh-n3ej', 'rdjz-vn2n', 'rdmq-nq56', 'rdng-ki53', 'rezz-ypcg',
    'rgnm-fkqb', 'rh2h-3yt2', 'rhwp-grxi', 'rksx-33p3', 'rnah-xd9n', 'rnvb-cpxx', 'rppv-wbiv',
    'rpvx-m2md', 'rq85-buyi', 'rsk5-566a', 'rsk8-spa7', 'rtjs-ain8', 'rw4v-h7j9', 's57w-7gbe',
    's5a6-fn5p', 's6p7-fvbw', 's85h-9xpy', 'saz5-9hgg', 'scrf-8d7w', 'sd8v-uq83', 'seuz-s2cv',
    'shc3-fzig', 'si7g-c2bs', 'siwp-yg6m', 'sixg-saap', 'sjpm-fk4b', 'skkh-jsrk', 'sks5-7yq7',
    'smic-9bf9', 'snev-n7vb', 'snkv-n8f6', 'ss2j-8ajj', 'ssz5-s49e', 'sumd-iwm8', 'sw5n-wg2p',
    'swc5-untb', 'sz5x-j2c3', 't6u2-f84c', 'tczv-qfsi', 'tdbk-8ubw', 'tdge-ieq8', 'tfcp-ufzp',
    'tfu6-pjxh', 'th8y-thx5', 'thir-stei', 'tpcp-uiv5', 'tqwu-4a7k', 'trpk-sp8z', 'tscn-ryh9',
    'tug7-57z5', 'twtn-mxqy', 'ty79-wym3', 'u22r-ndns', 'u2nj-bus9', 'u4vw-xsmf', 'u6jv-9ijr',
    'u7e4-s8zi', 'ua33-yiiu', 'ua7e-t2fy', 'uc9k-vc2j', 'udwr-3en6', 'uggs-hy5q', 'ugzv-zzdr',
    'ui6g-vumy', 'ukww-au2k', 'unsk-b7fc', 'uny6-e3dx', 'uqxy-gepz', 'ut5n-bmc3', 'uu9i-eu7y',
    'uuui-fh3m', 'uw7a-a5t8', 'uxgd-cqqc', 'uxwq-vny5', 'uzn2-cq9f', 'v22g-tzpk', 'v246-z5tb',
    'v2g4-wqg2', 'v2mh-3yzr', 'v2pi-w3up', 'v2zw-2d2v', 'v4tm-h8pe', 'v58w-vynu', 'v6ab-adf5',
    'v728-xui5', 'v7tk-n6v3', 'va5e-efw9', 'vba9-s8jp', 'vbim-akqf', 'vdz4-qrri', 'vdzy-6i9v',
    'vfj2-bfuw', 'vfmq-diru', 'vgc8-iyc4', 'vh55-3he6', 'vhcj-3k53', 'vjzj-u7u8', 'vkwg-yswv',
    'vmgc-uspy', 'vncy-2ds7', 'vpk8-vfhm', 'vq7a-fvin', 'vqyf-z2g3', 'vsak-wrfu', 'vugp-mqip',
    'vuhn-dxkt', 'vutn-jzwm', 'vutr-sfkh', 'vwmz-4ja3', 'vyry-2yfg', 'vzfn-ifh5', 'w26f-tf3h',
    'w46e-8kr3', 'w4cs-jspc', 'w76m-r924', 'w79a-dgrh', 'w9h3-6bpu', 'w9j2-ggv5', 'w9zu-fywh',
    'wan8-w4er', 'wd75-kcmv', 'wff4-m3q3', 'wgvr-7mvz', 'whhs-7a5d', 'wibz-pb5q', 'wpti-gvdi',
    'wrev-kwxu', 'wrrd-u9wx', 'wtw5-4wi3', 'wxz7-ekz9', 'wzwe-859x', 'x4dz-rafm', 'x5j9-wybp',
    'x66v-w5ka', 'x6ag-8y7r', 'x749-vh2i', 'x8jf-txib', 'x8ni-jytx', 'x9gk-5huc', 'xb3p-q62w',
    'xerk-pcm8', 'xf9s-d895', 'xgy8-wnft', 'xkb8-kh2a', 'xkkf-xrst', 'xnjn-rdmd', 'xpxn-rzgz',
    'xssa-9qw5', 'xsta-sbh5', 'xt86-xqxz', 'xvdv-hq7x', 'xwa7-cukt', 'xx8k-iu94', 'xyst-f73f',
    'y268-sna3', 'y4ft-s73h', 'y4ut-ybj7', 'y52v-k5rz', 'y5bj-9g5w', 'yctb-fv7w', 'ycxr-emue',
    'yhkp-cczf', 'yib5-h3pw', 'yjkw-uj5s', 'ymmh-divb', 'yn8z-e2cm', 'yni7-er2q', 'ynw2-4viq',
    'ype6-idgy', 'ypxr-mz8e', 'yrur-wghw', 'ysd3-txwj', 'yt7u-eiyg', 'yviw-z6j5',
]

DOWNLOAD_SPECS = [
    NodeSpec(
        id=f"cdc-{eid.lower().replace('_', '-')}",
        fn=fetch_one,
        kind="download",
    )
    for eid in ENTITY_IDS
]

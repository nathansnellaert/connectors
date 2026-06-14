"""UNCTAD connector — bulk 7z-compressed CSV per statistical report.

Mechanism (from research): `bulk_7z_csv`. For each report (entity) we resolve
its bulk file list from
  GET /api/reportMetadata/{reportName}/bulkfiles/en
then download each file via
  GET /api/reportMetadata/{reportName}/bulkfile/{fileId}/en
Each download is application/octet-stream; the real name is in
Content-Disposition (US_<Report>.csv.7z). The 7z archive holds a single
US_<Report>.csv with a stable wide layout:
  <dimension cols...>, then one (value, "<m> Footnote", "<m> Missing value")
  triplet per measure m.
The dimension columns differ per report (Year/Quarter, Economy, Partner,
Flow, Product, Sex, AgeClass, ...), so we normalise every report to ONE
uniform long schema by unpivoting the measure triplets in Python during the
fetch. That keeps the published transform uniform across all 92 reports.

fileId is release-specific, so it is always resolved fresh from /bulkfiles
(never hardcoded). Very large datasets are partitioned into several bulk
files (e.g. US.TradeMatrix); we stream every fileId into one parquet asset.

Strategy: stateless full re-pull. There is no per-row `since` filter, so each
refresh re-fetches the whole file; the maintain step (authored later) gates
whether a given report runs at all using the catalog's lastUpdatedDate.
Downloads are streamed to a temp file and the CSV is parsed row-by-row into
batched parquet writes, so memory stays bounded even for multi-GB reports.
"""
import csv
import json
import tempfile
from pathlib import Path

import httpx
import py7zr
import pyarrow as pa
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
    get_client,
    raw_parquet_writer,
    save_state,
)

STATE_VERSION = 1

_API = "https://unctadstat-api.unctad.org/api"

# Uniform long-format schema: one row per (data row x measure) observation.
SCHEMA = pa.schema([
    ("period", pa.string()),         # value of the leading time column (Year/Quarter)
    ("dimensions", pa.string()),     # JSON map of ALL dimension cols -> value (lossless)
    ("measure", pa.string()),        # measure name (the triplet's base column)
    ("value_raw", pa.string()),      # raw value cell; typed in the transform via TRY_CAST
    ("footnote", pa.string()),       # measure footnote, if any
    ("missing_value", pa.string()),  # missing-value reason, if any
])

_BATCH_ROWS = 200_000  # flush parquet row group ~ every 200k long records


# --- entity union (authoritative) ---------------------------------------
# Copied verbatim from
# data/sources/unctad/steps/92b3294822784c8d9f84fa7dd93dd7d0/entity_union.json
ENTITY_IDS = [
    "US.AssociatedPlasticsTradebyPartner", "US.BioTradeMerchGDPShare",
    "US.BioTradeMerchMarketIndices", "US.BioTradeMerchProdConcent",
    "US.BiotradeMerch", "US.BiotradeMerchRCA", "US.BiotradeMerchShare",
    "US.ConcentDiversIndices", "US.ConcentStructIndices", "US.ContPortThroughput",
    "US.Cpi_A", "US.CreativeGoodsGR", "US.CreativeGoodsValue",
    "US.CreativeServ_Group_E", "US.CreativeServ_Indiv_Tot", "US.CurrAccBalance",
    "US.DigitallyDeliverableServices", "US.ECommerceInternational",
    "US.ECommerceTotal", "US.EnvironmentalGoodsRCA", "US.EnvironmentalGoodsTrade",
    "US.ExchangeRateCrosstab", "US.FTRI", "US.FdiFlowsStock",
    "US.FleetBeneficialOwners", "US.GDPComponent", "US.GDPGR", "US.GDPTotal",
    "US.GNI", "US.Gender_DomesticValueAdded", "US.Gender_TradableIndustries",
    "US.GoodsAndServBalanceBpm6", "US.GoodsAndServTradeOpennessBpm6",
    "US.GoodsAndServicesBpm6", "US.GovExpenditures", "US.IFF_CrimesRelated_In",
    "US.IFF_CrimesRelated_Out", "US.IFF_TradeMisinvoicing_In",
    "US.IFF_TradeMisinvoicing_Out", "US.IctGoodsShare", "US.IctGoodsValue",
    "US.IctProductionSector", "US.IctUseEconActivity", "US.IctUseEconActivity_Isic4",
    "US.IctUseEnterprSize", "US.IctUseLocation", "US.InclusiveGrowth",
    "US.IntraTrade", "US.LSBCI", "US.LSCI", "US.LSCI_M", "US.MerchTheilIndices",
    "US.MerchVolumeQuarterly", "US.MerchantFleet", "US.NonPlasticSubstsTradeByPartner",
    "US.OceanServices", "US.OceanTrade", "US.PCI", "US.PLSCI",
    "US.PlasticsTradebyPartner", "US.PopAgeStruct", "US.PopDependency",
    "US.PopTotal", "US.PortCalls", "US.PortCallsArrivals", "US.PortCallsArrivals_S",
    "US.PortCalls_S", "US.RCA", "US.Remittances", "US.SDG_LULFRG", "US.SDG_PORFVOL",
    "US.SeaborneTrade", "US.ShipBuilding", "US.ShipScrapping", "US.Tariff",
    "US.TermsOfTrade", "US.TotAndComServicesQuarterly", "US.TradeFoodCatByProc",
    "US.TradeFoodProcByCat", "US.TradeMatrix", "US.TradeMerchBalance",
    "US.TradeMerchGR", "US.TradeMerchTotal", "US.TradeServCatByPartner",
    "US.TradeServCatQuarterlyAnnualized", "US.TradeServCatTotal", "US.TradeServICT",
    "US.TransportCosts", "US.UCPI_A", "US.UCPI_M", "US.VesselValueByOwnership",
    "US.VesselValueByRegistration",
]


def _node_id(entity_id: str) -> str:
    return f"unctad-{entity_id.lower().replace('_', '-')}"


# node_id -> original reportName (node ids are lowercased/dashed, so we can't
# reverse them; map them explicitly).
_REPORT_BY_NODE = {_node_id(e): e for e in ENTITY_IDS}


# --- HTTP retry plumbing -------------------------------------------------
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
def _fetch_json(url: str):
    resp = get(url, timeout=(10.0, 120.0))
    resp.raise_for_status()
    return resp.json()


@retry(
    retry=retry_if_exception(_is_transient),
    stop=stop_after_attempt(6),
    wait=wait_exponential(min=4, max=120),
    reraise=True,
)
def _download_to_file(url: str, dest: Path) -> int:
    """Stream a (potentially multi-GB) download to a local temp file."""
    nbytes = 0
    client = get_client()
    with client.stream("GET", url, timeout=httpx.Timeout(600.0, connect=10.0)) as resp:
        resp.raise_for_status()
        with open(dest, "wb") as fh:
            for chunk in resp.iter_bytes(4 * 1024 * 1024):
                fh.write(chunk)
                nbytes += len(chunk)
    return nbytes


# --- CSV layout parsing --------------------------------------------------
def _split_columns(header: list[str]) -> tuple[list[int], list[str]]:
    """Return (dimension column indices, measure base names).

    The wide layout is <dimension cols...> followed by (value, "<m> Footnote",
    "<m> Missing value") triplets. The first measure starts at the first index
    i where header[i+1] == header[i] + " Footnote".
    """
    n = len(header)
    first_measure = None
    for i in range(n - 2):
        if (header[i + 1] == f"{header[i]} Footnote"
                and header[i + 2] == f"{header[i]} Missing value"):
            first_measure = i
            break
    if first_measure is None:
        raise ValueError(f"no measure triplet found in header: {header[:12]}")

    dim_indices = list(range(first_measure))
    measure_cols = header[first_measure:]
    if len(measure_cols) % 3 != 0:
        raise ValueError(
            f"measure columns not in triplets ({len(measure_cols)}): {measure_cols}"
        )
    measures = [measure_cols[j] for j in range(0, len(measure_cols), 3)]
    return dim_indices, measures


def _parse_csv_into(writer, csv_path: Path, node_id: str) -> int:
    """Stream-parse a wide UNCTAD CSV into uniform long parquet batches.

    Only emits a record when the value cell is non-empty (the vast majority of
    cells in sparse cubes are blank); footnote/missing-value-only cells carry
    no observation and are dropped.
    """
    written = 0
    cols = {k: [] for k in ("period", "dimensions", "measure",
                            "value_raw", "footnote", "missing_value")}
    rows_seen = 0

    def flush():
        nonlocal written
        if not cols["period"]:
            return
        table = pa.table(cols, schema=SCHEMA)
        writer.write_table(table)
        written += table.num_rows
        for v in cols.values():
            v.clear()

    with open(csv_path, "r", encoding="utf-8-sig", newline="") as fh:
        reader = csv.reader(fh)
        header = next(reader)
        dim_indices, measures = _split_columns(header)
        first_measure = len(dim_indices)
        # value cell index for measure k = first_measure + 3*k
        for row in reader:
            rows_seen += 1
            if len(row) < len(header):
                row = row + [""] * (len(header) - len(row))
            dims = {header[i]: row[i] for i in dim_indices}
            dims_json = json.dumps(dims, ensure_ascii=False, separators=(",", ":"))
            period = row[dim_indices[0]] if dim_indices else ""
            for k, measure in enumerate(measures):
                base = first_measure + 3 * k
                value = row[base]
                if not value.strip():
                    continue
                footnote = row[base + 1]
                missing = row[base + 2]
                cols["period"].append(period)
                cols["dimensions"].append(dims_json)
                cols["measure"].append(measure)
                cols["value_raw"].append(value)
                cols["footnote"].append(footnote or None)
                cols["missing_value"].append(missing or None)
            if len(cols["period"]) >= _BATCH_ROWS:
                flush()
            if rows_seen % 1_000_000 == 0:
                print(f"  {node_id}: parsed {rows_seen:,} source rows, "
                      f"{written:,} observations", flush=True)
    flush()
    return written


def fetch_one(node_id: str) -> None:
    """Fetch one report's bulk file(s) and write a uniform long parquet asset.

    The runtime passes the spec id; it IS the asset name. Freshness gating is
    the maintain step's job — if we're invoked, we fetch.
    """
    asset = node_id
    report = _REPORT_BY_NODE[node_id]

    bulkfiles = _fetch_json(f"{_API}/reportMetadata/{report}/bulkfiles/en")
    if not isinstance(bulkfiles, list) or not bulkfiles:
        # No bulk file for this report — surface loudly rather than publishing
        # an empty asset (every union entity is expected to have one).
        raise RuntimeError(f"{report}: /bulkfiles returned no files: {bulkfiles!r}")

    total_bytes = 0
    total_obs = 0
    with raw_parquet_writer(asset, SCHEMA) as writer:
        with tempfile.TemporaryDirectory() as td:
            tdp = Path(td)
            for rec in bulkfiles:
                file_id = rec["fileId"]
                url = f"{_API}/reportMetadata/{report}/bulkfile/{file_id}/en"
                archive = tdp / f"{file_id}.7z"
                total_bytes += _download_to_file(url, archive)
                extract_dir = tdp / f"x{file_id}"
                extract_dir.mkdir()
                with py7zr.SevenZipFile(archive, mode="r") as z:
                    members = z.getnames()
                    z.extractall(path=extract_dir)
                archive.unlink()  # free disk before parsing
                for member in members:
                    csv_path = extract_dir / member
                    total_obs += _parse_csv_into(writer, csv_path, node_id)
                    csv_path.unlink()

    if total_obs == 0:
        raise RuntimeError(f"{report}: parsed 0 observations from {len(bulkfiles)} file(s)")

    save_state(asset, {
        "schema_version": STATE_VERSION,
        "last_run_stats": {
            "observations": total_obs,
            "download_bytes": total_bytes,
            "bulk_files": len(bulkfiles),
            "report": report,
        },
    })
    print(f"  -> {asset}: {total_obs:,} observations from "
          f"{len(bulkfiles)} bulk file(s)", flush=True)


DOWNLOAD_SPECS = [
    NodeSpec(id=_node_id(eid), fn=fetch_one, kind="download")
    for eid in ENTITY_IDS
]


TRANSFORM_SPECS = [
    SqlNodeSpec(
        id=f"{s.id}-transform",
        deps=[s.id],
        sql=f'''
            SELECT
                period,
                dimensions,
                measure,
                TRY_CAST(value_raw AS DOUBLE) AS value,
                value_raw,
                footnote,
                missing_value
            FROM "{s.id}"
            WHERE value_raw IS NOT NULL AND TRIM(value_raw) <> ''
        ''',
    )
    for s in DOWNLOAD_SPECS
]

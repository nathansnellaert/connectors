"""Australian Bureau of Statistics — download node.

Catalog connector over the ABS SDMX 2.1 REST surface
(https://data.api.abs.gov.au/rest, public since the Nov-2024 redesign).

One spec per dataflow in the entity union. Each dataflow is fetched in full
via the bulk path `/rest/data/<dataflow>/all?format=csv` — the API has no
per-record incremental query in our usage pattern, so every refresh pulls a
whole-dataset snapshot and overwrites the prior raw asset (shape (e),
snapshot-only; freshness gating is the maintain step's job).

Raw format: SDMX-CSV. Each dataflow declares its own DSD, so the column set
differs per entity (CPI has MEASURE/INDEX/TSEST/REGION..., census tables have
PCHAR/SEXP/REGION_TYPE...). Within a single dataflow the columns are stable,
so each entity gets its own parquet asset. Every column is stored as a STRING:
the dimension columns are SDMX codes (REGION="50", STATE="4", MEASURE="3") that
must never be numerically coerced, and OBS_VALUE carries blanks/flags — the
transform step does the typing. Some dataflows are large (ABS_REGIONAL_ASGS2021
is ~485MB / 6.5M rows), so the CSV is streamed straight from the HTTP response
into a row-group-streamed parquet writer; peak memory stays a few hundred MB
regardless of dataflow size.
"""

import httpx
import pyarrow as pa
import pyarrow.csv as pacsv
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from subsets_utils import NodeSpec, get_client, raw_parquet_writer


BASE_URL = "https://data.api.abs.gov.au/rest"

# Network read chunk + pyarrow CSV parse block. 4MB blocks keep each parsed
# RecordBatch (and thus the per-batch parquet row group) small, which bounds
# peak RSS even on the multi-hundred-MB dataflows.
HTTP_CHUNK_SIZE = 256 * 1024
CSV_BLOCK_SIZE = 4 * 1024 * 1024

# Streaming downloads can take a while for the big dataflows; the read timeout
# is the gap between bytes, not a total cap.
TIMEOUT = httpx.Timeout(connect=30.0, read=600.0, write=30.0, pool=30.0)


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
    """Retry network blips and server-side 429/5xx; surface everything else."""
    if isinstance(exc, _TRANSIENT_EXC):
        return True
    if isinstance(exc, httpx.HTTPStatusError):
        code = exc.response.status_code
        return code == 429 or 500 <= code < 600
    return False


class _StreamReader:
    """Minimal read(size) file-like over an httpx streaming byte iterator.

    pyarrow's CSV reader pulls bytes via read(); we additionally expose
    readline() so the caller can peek the header row (to derive the column
    set) before handing the remaining stream to pyarrow. A pushback buffer
    holds bytes pulled past a line boundary.
    """

    def __init__(self, byte_iter):
        self._it = byte_iter
        self._buf = b""
        self._eof = False
        self.closed = False

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return False

    def writable(self) -> bool:
        return False

    def close(self) -> None:
        self.closed = True

    def _pull(self) -> None:
        try:
            self._buf += next(self._it)
        except StopIteration:
            self._eof = True

    def readline(self) -> bytes:
        while b"\n" not in self._buf and not self._eof:
            self._pull()
        idx = self._buf.find(b"\n")
        if idx == -1:
            out, self._buf = self._buf, b""
            return out
        out, self._buf = self._buf[: idx + 1], self._buf[idx + 1 :]
        return out

    def read(self, size: int = -1) -> bytes:
        if size is None or size < 0:
            chunks = [self._buf]
            self._buf = b""
            for c in self._it:
                chunks.append(c)
            self._eof = True
            return b"".join(chunks)
        while not self._eof and len(self._buf) < size:
            self._pull()
        out, self._buf = self._buf[:size], self._buf[size:]
        return out


@retry(
    retry=retry_if_exception(_is_transient),
    stop=stop_after_attempt(5),
    wait=wait_exponential(min=4, max=120),
    reraise=True,
)
def _stream_dataflow_to_parquet(dataflow_id: str, asset_id: str) -> int:
    """Stream one dataflow's full SDMX-CSV into a parquet raw asset.

    Forces every column to string (DSD dimension members are codes, OBS_VALUE
    carries blanks/flags — typing is the transform step's concern) and flushes
    each parsed block as its own parquet row group so memory stays bounded.
    Returns the row count. A 4xx (e.g. a dataflow that vanished from the
    catalog) is non-transient and propagates — every entity-union member must
    be fetchable, so a genuine gap should fail loudly rather than be skipped.
    """
    url = f"{BASE_URL}/data/{dataflow_id}/all"
    client = get_client()
    rows = 0
    with client.stream(
        "GET",
        url,
        params={"format": "csv"},
        headers={"Accept": "application/vnd.sdmx.data+csv"},
        timeout=TIMEOUT,
    ) as resp:
        resp.raise_for_status()
        reader = _StreamReader(resp.iter_bytes(chunk_size=HTTP_CHUNK_SIZE))

        header = reader.readline().decode("utf-8").rstrip("\r\n")
        if not header:
            raise AssertionError(f"{dataflow_id}: empty SDMX-CSV response (no header)")
        columns = header.split(",")
        schema = pa.schema([(name, pa.string()) for name in columns])

        read_opts = pacsv.ReadOptions(
            column_names=columns, block_size=CSV_BLOCK_SIZE
        )
        convert_opts = pacsv.ConvertOptions(
            column_types={name: pa.string() for name in columns},
            strings_can_be_null=False,
        )
        batch_reader = pacsv.open_csv(
            reader, read_options=read_opts, convert_options=convert_opts
        )

        with raw_parquet_writer(asset_id, schema) as writer:
            for batch in batch_reader:
                # Flush each block as its own row group — bounds peak memory on
                # the large dataflows (e.g. ABS_REGIONAL_ASGS2021, 6.5M rows).
                writer.write_batch(batch, row_group_size=batch.num_rows)
                rows += batch.num_rows
    return rows


def fetch_one(entity_id: str) -> None:
    """Download a single ABS dataflow snapshot to its raw parquet asset.

    The dataflow id IS the entity id (collect's source_metadata.id == the
    union entry). Snapshot-overwrite: if invoked, we fetch — freshness is the
    maintain step's decision, not ours.
    """
    asset_id = f"australian-bureau-of-statistics-{entity_id.lower().replace('_', '-')}"
    print(f"[abs] fetching dataflow {entity_id} -> {asset_id}", flush=True)
    rows = _stream_dataflow_to_parquet(entity_id, asset_id)
    print(f"[abs] {entity_id}: {rows:,} observations", flush=True)


ENTITY_IDS = [
    'ABORIGINAL_ID_POP_PROJ', 'ABORIGINAL_POP_PROJ', 'ABS_ANNUAL_ERP_ASGS2021',
    'ABS_ANNUAL_ERP_LGA2024', 'ABS_DEM_QIM', 'ABS_ERP_COB_STATE', 'ABS_FAMILY_PROJ', 'ABS_HH_PROV',
    'ABS_LABOUR_ACCT', 'ABS_LABOUR_ACCT_UNBAL', 'ABS_NOM_VISA_CY', 'ABS_NOM_VISA_FY',
    'ABS_PERSONS_PROJ', 'ABS_REGIONAL_ASGS2021', 'ABS_REGIONAL_INDIGENOUS_2021',
    'ABS_REGIONAL_LGA2021', 'ABS_REGIONAL_MIGRATION', 'ABS_REGIONAL_REMOTENESS_ASGS2021',
    'ABS_SEIFA2021_LGA', 'ABS_SEIFA2021_POA', 'ABS_SEIFA2021_SA1', 'ABS_SEIFA2021_SA2',
    'ABS_SEIFA2021_SAL', 'ABS_SU_TABLE_2024', 'AG_BROADACRE', 'AG_HORTICULTURE', 'ALC', 'ANA_AGG',
    'ANA_EXP', 'ANA_INC', 'ANA_IND_GVA', 'ANA_SFD', 'ATSI_BIRTHS_SUMM', 'ATSI_FERTILITY',
    'AUSTRALIAN_INDUSTRY', 'AWE', 'BA_GCCSA', 'BA_LGA2025', 'BA_SA2', 'BIRTHS_AGE_FATHER',
    'BIRTHS_AGE_MOTHER', 'BIRTHS_MONTH_OCCURRENCE', 'BIRTHS_MON_OCC_AGE_MOTHER', 'BIRTHS_SUMMARY',
    'BOP', 'BOP_GOODS', 'BOP_STATE', 'BUILDING_ACTIVITY', 'BUSINESS_TURNOVER', 'BWD', 'C21_G01_CED',
    'C21_G01_LGA', 'C21_G01_POA', 'C21_G01_RA', 'C21_G01_SA2', 'C21_G01_SAL', 'C21_G01_SED',
    'C21_G01_SUA', 'C21_G01_UCL', 'C21_G02_CED', 'C21_G02_LGA', 'C21_G02_POA', 'C21_G02_RA',
    'C21_G02_SA2', 'C21_G02_SAL', 'C21_G02_SED', 'C21_G02_SUA', 'C21_G02_UCL', 'C21_G03_CED',
    'C21_G03_LGA', 'C21_G03_POA', 'C21_G03_RA', 'C21_G03_SA2', 'C21_G03_SAL', 'C21_G03_SED',
    'C21_G03_SUA', 'C21_G03_UCL', 'C21_G04_CED', 'C21_G04_LGA', 'C21_G04_POA', 'C21_G04_RA',
    'C21_G04_SA2', 'C21_G04_SAL', 'C21_G04_SED', 'C21_G04_SUA', 'C21_G04_UCL', 'C21_G05_CED',
    'C21_G05_LGA', 'C21_G05_POA', 'C21_G05_RA', 'C21_G05_SA2', 'C21_G05_SAL', 'C21_G05_SED',
    'C21_G05_SUA', 'C21_G05_UCL', 'C21_G06_CED', 'C21_G06_LGA', 'C21_G06_POA', 'C21_G06_RA',
    'C21_G06_SA2', 'C21_G06_SAL', 'C21_G06_SED', 'C21_G06_SUA', 'C21_G06_UCL', 'C21_G07_CED',
    'C21_G07_LGA', 'C21_G07_POA', 'C21_G07_RA', 'C21_G07_SA2', 'C21_G07_SAL', 'C21_G07_SED',
    'C21_G07_SUA', 'C21_G07_UCL', 'C21_G08_CED', 'C21_G08_LGA', 'C21_G08_POA', 'C21_G08_RA',
    'C21_G08_SA2', 'C21_G08_SAL', 'C21_G08_SED', 'C21_G08_SUA', 'C21_G08_UCL', 'C21_G09_CED',
    'C21_G09_LGA', 'C21_G09_POA', 'C21_G09_RA', 'C21_G09_SA2', 'C21_G09_SAL', 'C21_G09_SED',
    'C21_G09_SUA', 'C21_G09_UCL', 'C21_G10_CED', 'C21_G10_LGA', 'C21_G10_POA', 'C21_G10_RA',
    'C21_G10_SA2', 'C21_G10_SAL', 'C21_G10_SED', 'C21_G10_SUA', 'C21_G10_UCL', 'C21_G11_CED',
    'C21_G11_LGA', 'C21_G11_POA', 'C21_G11_RA', 'C21_G11_SA2', 'C21_G11_SAL', 'C21_G11_SED',
    'C21_G11_SUA', 'C21_G11_UCL', 'C21_G12_CED', 'C21_G12_LGA', 'C21_G12_POA', 'C21_G12_RA',
    'C21_G12_SA2', 'C21_G12_SAL', 'C21_G12_SED', 'C21_G12_SUA', 'C21_G12_UCL', 'C21_G13_CED',
    'C21_G13_LGA', 'C21_G13_POA', 'C21_G13_RA', 'C21_G13_SA2', 'C21_G13_SAL', 'C21_G13_SED',
    'C21_G13_SUA', 'C21_G13_UCL', 'C21_G14_CED', 'C21_G14_LGA', 'C21_G14_POA', 'C21_G14_RA',
    'C21_G14_SA2', 'C21_G14_SAL', 'C21_G14_SED', 'C21_G14_SUA', 'C21_G14_UCL', 'C21_G15_CED',
    'C21_G15_LGA', 'C21_G15_POA', 'C21_G15_RA', 'C21_G15_SA2', 'C21_G15_SAL', 'C21_G15_SED',
    'C21_G15_SUA', 'C21_G15_UCL', 'C21_G16_CED', 'C21_G16_LGA', 'C21_G16_POA', 'C21_G16_RA',
    'C21_G16_SA2', 'C21_G16_SAL', 'C21_G16_SED', 'C21_G16_SUA', 'C21_G16_UCL', 'C21_G17_CED',
    'C21_G17_LGA', 'C21_G17_POA', 'C21_G17_RA', 'C21_G17_SA2', 'C21_G17_SAL', 'C21_G17_SED',
    'C21_G17_SUA', 'C21_G17_UCL', 'C21_G18_CED', 'C21_G18_LGA', 'C21_G18_POA', 'C21_G18_RA',
    'C21_G18_SA2', 'C21_G18_SAL', 'C21_G18_SED', 'C21_G18_SUA', 'C21_G18_UCL', 'C21_G19_CED',
    'C21_G19_LGA', 'C21_G19_POA', 'C21_G19_RA', 'C21_G19_SA2', 'C21_G19_SAL', 'C21_G19_SED',
    'C21_G19_SUA', 'C21_G19_UCL', 'C21_G20_CED', 'C21_G20_LGA', 'C21_G20_POA', 'C21_G20_RA',
    'C21_G20_SA2', 'C21_G20_SAL', 'C21_G20_SED', 'C21_G20_SUA', 'C21_G20_UCL', 'C21_G21_CED',
    'C21_G21_LGA', 'C21_G21_POA', 'C21_G21_RA', 'C21_G21_SA2', 'C21_G21_SAL', 'C21_G21_SED',
    'C21_G21_SUA', 'C21_G21_UCL', 'C21_G22_CED', 'C21_G22_LGA', 'C21_G22_POA', 'C21_G22_RA',
    'C21_G22_SA2', 'C21_G22_SAL', 'C21_G22_SED', 'C21_G22_SUA', 'C21_G22_UCL', 'C21_G23_CED',
    'C21_G23_LGA', 'C21_G23_POA', 'C21_G23_RA', 'C21_G23_SA2', 'C21_G23_SAL', 'C21_G23_SED',
    'C21_G23_SUA', 'C21_G23_UCL', 'C21_G24_CED', 'C21_G24_LGA', 'C21_G24_POA', 'C21_G24_RA',
    'C21_G24_SA2', 'C21_G24_SAL', 'C21_G24_SED', 'C21_G24_SUA', 'C21_G24_UCL', 'C21_G25_CED',
    'C21_G25_LGA', 'C21_G25_POA', 'C21_G25_RA', 'C21_G25_SA2', 'C21_G25_SAL', 'C21_G25_SED',
    'C21_G25_SUA', 'C21_G25_UCL', 'C21_G26_CED', 'C21_G26_LGA', 'C21_G26_POA', 'C21_G26_RA',
    'C21_G26_SA2', 'C21_G26_SAL', 'C21_G26_SED', 'C21_G26_SUA', 'C21_G26_UCL', 'C21_G27_CED',
    'C21_G27_LGA', 'C21_G27_POA', 'C21_G27_RA', 'C21_G27_SA2', 'C21_G27_SAL', 'C21_G27_SED',
    'C21_G27_SUA', 'C21_G27_UCL', 'C21_G28_CED', 'C21_G28_LGA', 'C21_G28_POA', 'C21_G28_RA',
    'C21_G28_SA2', 'C21_G28_SAL', 'C21_G28_SED', 'C21_G28_SUA', 'C21_G28_UCL', 'C21_G29_CED',
    'C21_G29_LGA', 'C21_G29_POA', 'C21_G29_RA', 'C21_G29_SA2', 'C21_G29_SAL', 'C21_G29_SED',
    'C21_G29_SUA', 'C21_G29_UCL', 'C21_G30_CED', 'C21_G30_LGA', 'C21_G30_POA', 'C21_G30_RA',
    'C21_G30_SA2', 'C21_G30_SAL', 'C21_G30_SED', 'C21_G30_SUA', 'C21_G30_UCL', 'C21_G31_CED',
    'C21_G31_LGA', 'C21_G31_POA', 'C21_G31_RA', 'C21_G31_SA2', 'C21_G31_SAL', 'C21_G31_SED',
    'C21_G31_SUA', 'C21_G31_UCL', 'C21_G32_CED', 'C21_G32_LGA', 'C21_G32_POA', 'C21_G32_RA',
    'C21_G32_SA2', 'C21_G32_SAL', 'C21_G32_SED', 'C21_G32_SUA', 'C21_G32_UCL', 'C21_G33_CED',
    'C21_G33_LGA', 'C21_G33_POA', 'C21_G33_RA', 'C21_G33_SA2', 'C21_G33_SAL', 'C21_G33_SED',
    'C21_G33_SUA', 'C21_G33_UCL', 'C21_G34_CED', 'C21_G34_LGA', 'C21_G34_POA', 'C21_G34_RA',
    'C21_G34_SA2', 'C21_G34_SAL', 'C21_G34_SED', 'C21_G34_SUA', 'C21_G34_UCL', 'C21_G35_CED',
    'C21_G35_LGA', 'C21_G35_POA', 'C21_G35_RA', 'C21_G35_SA2', 'C21_G35_SAL', 'C21_G35_SED',
    'C21_G35_SUA', 'C21_G35_UCL', 'C21_G36_CED', 'C21_G36_LGA', 'C21_G36_POA', 'C21_G36_RA',
    'C21_G36_SA2', 'C21_G36_SAL', 'C21_G36_SED', 'C21_G36_SUA', 'C21_G36_UCL', 'C21_G37_CED',
    'C21_G37_LGA', 'C21_G37_POA', 'C21_G37_RA', 'C21_G37_SA2', 'C21_G37_SAL', 'C21_G37_SED',
    'C21_G37_SUA', 'C21_G37_UCL', 'C21_G38_CED', 'C21_G38_LGA', 'C21_G38_POA', 'C21_G38_RA',
    'C21_G38_SA2', 'C21_G38_SAL', 'C21_G38_SED', 'C21_G38_SUA', 'C21_G38_UCL', 'C21_G39_CED',
    'C21_G39_LGA', 'C21_G39_POA', 'C21_G39_RA', 'C21_G39_SA2', 'C21_G39_SAL', 'C21_G39_SED',
    'C21_G39_SUA', 'C21_G39_UCL', 'C21_G40_CED', 'C21_G40_LGA', 'C21_G40_POA', 'C21_G40_RA',
    'C21_G40_SA2', 'C21_G40_SAL', 'C21_G40_SED', 'C21_G40_SUA', 'C21_G40_UCL', 'C21_G41_CED',
    'C21_G41_LGA', 'C21_G41_POA', 'C21_G41_RA', 'C21_G41_SA2', 'C21_G41_SAL', 'C21_G41_SED',
    'C21_G41_SUA', 'C21_G41_UCL', 'C21_G42_CED', 'C21_G42_LGA', 'C21_G42_POA', 'C21_G42_RA',
    'C21_G42_SA2', 'C21_G42_SAL', 'C21_G42_SED', 'C21_G42_SUA', 'C21_G42_UCL', 'C21_G43_CED',
    'C21_G43_LGA', 'C21_G43_POA', 'C21_G43_RA', 'C21_G43_SA2', 'C21_G43_SAL', 'C21_G43_SED',
    'C21_G43_SUA', 'C21_G43_UCL', 'C21_G44_CED', 'C21_G44_LGA', 'C21_G44_POA', 'C21_G44_RA',
    'C21_G44_SA2', 'C21_G44_SAL', 'C21_G44_SED', 'C21_G44_SUA', 'C21_G44_UCL', 'C21_G45_CED',
    'C21_G45_LGA', 'C21_G45_POA', 'C21_G45_RA', 'C21_G45_SA2', 'C21_G45_SAL', 'C21_G45_SED',
    'C21_G45_SUA', 'C21_G45_UCL', 'C21_G46_CED', 'C21_G46_LGA', 'C21_G46_POA', 'C21_G46_RA',
    'C21_G46_SA2', 'C21_G46_SAL', 'C21_G46_SED', 'C21_G46_SUA', 'C21_G46_UCL', 'C21_G47_CED',
    'C21_G47_LGA', 'C21_G47_POA', 'C21_G47_RA', 'C21_G47_SA2', 'C21_G47_SAL', 'C21_G47_SED',
    'C21_G47_SUA', 'C21_G47_UCL', 'C21_G48_CED', 'C21_G48_LGA', 'C21_G48_POA', 'C21_G48_RA',
    'C21_G48_SA2', 'C21_G48_SAL', 'C21_G48_SED', 'C21_G48_SUA', 'C21_G48_UCL', 'C21_G49_CED',
    'C21_G49_LGA', 'C21_G49_POA', 'C21_G49_RA', 'C21_G49_SA2', 'C21_G49_SAL', 'C21_G49_SED',
    'C21_G49_SUA', 'C21_G49_UCL', 'C21_G50_CED', 'C21_G50_LGA', 'C21_G50_POA', 'C21_G50_RA',
    'C21_G50_SA2', 'C21_G50_SAL', 'C21_G50_SED', 'C21_G50_SUA', 'C21_G50_UCL', 'C21_G51_CED',
    'C21_G51_LGA', 'C21_G51_POA', 'C21_G51_RA', 'C21_G51_SA2', 'C21_G51_SAL', 'C21_G51_SED',
    'C21_G51_SUA', 'C21_G51_UCL', 'C21_G52_CED', 'C21_G52_LGA', 'C21_G52_POA', 'C21_G52_RA',
    'C21_G52_SA2', 'C21_G52_SAL', 'C21_G52_SED', 'C21_G52_SUA', 'C21_G52_UCL', 'C21_G53_CED',
    'C21_G53_LGA', 'C21_G53_POA', 'C21_G53_RA', 'C21_G53_SA2', 'C21_G53_SAL', 'C21_G53_SED',
    'C21_G53_SUA', 'C21_G53_UCL', 'C21_G54_CED', 'C21_G54_LGA', 'C21_G54_POA', 'C21_G54_RA',
    'C21_G54_SA2', 'C21_G54_SAL', 'C21_G54_SED', 'C21_G54_SUA', 'C21_G54_UCL', 'C21_G55_CED',
    'C21_G55_LGA', 'C21_G55_POA', 'C21_G55_RA', 'C21_G55_SA2', 'C21_G55_SAL', 'C21_G55_SED',
    'C21_G55_SUA', 'C21_G55_UCL', 'C21_G56_CED', 'C21_G56_LGA', 'C21_G56_POA', 'C21_G56_RA',
    'C21_G56_SA2', 'C21_G56_SAL', 'C21_G56_SED', 'C21_G56_SUA', 'C21_G56_UCL', 'C21_G57_CED',
    'C21_G57_LGA', 'C21_G57_POA', 'C21_G57_RA', 'C21_G57_SA2', 'C21_G57_SAL', 'C21_G57_SED',
    'C21_G57_SUA', 'C21_G57_UCL', 'C21_G58_CED', 'C21_G58_LGA', 'C21_G58_POA', 'C21_G58_RA',
    'C21_G58_SA2', 'C21_G58_SAL', 'C21_G58_SED', 'C21_G58_SUA', 'C21_G58_UCL', 'C21_G59_CED',
    'C21_G59_LGA', 'C21_G59_POA', 'C21_G59_RA', 'C21_G59_SA2', 'C21_G59_SAL', 'C21_G59_SED',
    'C21_G59_SUA', 'C21_G59_UCL', 'C21_G60_CED', 'C21_G60_LGA', 'C21_G60_POA', 'C21_G60_RA',
    'C21_G60_SA2', 'C21_G60_SAL', 'C21_G60_SED', 'C21_G60_SUA', 'C21_G60_UCL', 'C21_G61_CED',
    'C21_G61_LGA', 'C21_G61_POA', 'C21_G61_RA', 'C21_G61_SA2', 'C21_G61_SAL', 'C21_G61_SED',
    'C21_G61_SUA', 'C21_G61_UCL', 'C21_G62_CED', 'C21_G62_LGA', 'C21_G62_POA', 'C21_G62_RA',
    'C21_G62_SA2', 'C21_G62_SAL', 'C21_G62_SED', 'C21_G62_SUA', 'C21_G62_UCL', 'C21_T01_LGA',
    'C21_T01_SA2', 'C21_T02_LGA', 'C21_T02_SA2', 'C21_T03_LGA', 'C21_T03_SA2', 'C21_T04_LGA',
    'C21_T04_SA2', 'C21_T05_LGA', 'C21_T05_SA2', 'C21_T06_LGA', 'C21_T06_SA2', 'C21_T07_LGA',
    'C21_T07_SA2', 'C21_T08_LGA', 'C21_T08_SA2', 'C21_T09_LGA', 'C21_T09_SA2', 'C21_T10_LGA',
    'C21_T10_SA2', 'C21_T11_LGA', 'C21_T11_SA2', 'C21_T12_LGA', 'C21_T12_SA2', 'C21_T13_LGA',
    'C21_T13_SA2', 'C21_T14_LGA', 'C21_T14_SA2', 'C21_T15_LGA', 'C21_T15_SA2', 'C21_T16_LGA',
    'C21_T16_SA2', 'C21_T17_LGA', 'C21_T17_SA2', 'C21_T18_LGA', 'C21_T18_SA2', 'C21_T19_LGA',
    'C21_T19_SA2', 'C21_T20_LGA', 'C21_T20_SA2', 'C21_T21_LGA', 'C21_T21_SA2', 'C21_T22_LGA',
    'C21_T22_SA2', 'C21_T23_LGA', 'C21_T23_SA2', 'C21_T24_LGA', 'C21_T24_SA2', 'C21_T25_LGA',
    'C21_T25_SA2', 'C21_T26_LGA', 'C21_T26_SA2', 'C21_T27_LGA', 'C21_T27_SA2', 'C21_T28_LGA',
    'C21_T28_SA2', 'C21_T29_LGA', 'C21_T29_SA2', 'C21_T30_LGA', 'C21_T30_SA2', 'C21_T31_LGA',
    'C21_T31_SA2', 'C21_T32_LGA', 'C21_T32_SA2', 'C21_T33_LGA', 'C21_T33_SA2', 'C21_T34_LGA',
    'C21_T34_SA2', 'C21_T35_LGA', 'C21_T35_SA2', 'CAPEX', 'CAPEX_EST', 'CONFINEMENTS_NUPTIALITY',
    'CPI', 'CPI_M', 'CPI_Q', 'CPI_WEIGHTS', 'CWD', 'DEATHS_AGESPECIFIC_OCCURENCEYEAR',
    'DEATHS_AGESPECIFIC_REGISTRATIONYEAR', 'DEATHS_AGESPECIFIC_REGISTRATIONYEAR_1',
    'DEATHS_INDIGENOUS', 'DEATHS_INDIGENOUS_SUMMARY', 'DEATHS_MARITAL_STATUS',
    'DEATHS_MONTHOCCURENCE', 'DEATHS_SUMMARY', 'ERP_ASGS2021', 'ERP_ATSI', 'ERP_ATSI_REMOTE',
    'ERP_COB', 'ERP_COMP_LGA2025', 'ERP_COMP_Q', 'ERP_COMP_SA_ASGS2021', 'ERP_LGA2025', 'ERP_Q',
    'EWD', 'FERTILITY_AGE_STATE', 'HSI_M', 'HSI_Q', 'IIP', 'INFANTDEATHS_REGISTRATIONYEAR',
    'INFANTDEATHS_YEAROCCURENCE', 'ITGS', 'ITPI_EXP', 'ITPI_IMP', 'JV', 'LABOUR_ACCT_Q', 'LCI',
    'LCI_WEIGHTS', 'LEND_BUSINESS', 'LEND_HOUSING', 'LEND_PERSONAL', 'LF', 'LF_AGES', 'LF_EDU',
    'LF_HOURS', 'LF_UNDER', 'LSTOCK_MEAT', 'LSTOCK_SLAUGHT', 'MERCH_EXP', 'MERCH_IMP', 'MIN_EXP',
    'NIM_CY', 'NIM_FY', 'NOM_CY', 'NOM_FY', 'OAD_COUNTRY', 'OAD_REASON', 'OMAD_VISA',
    'PATERNITY_AGE_STATE', 'PET_EXP', 'POPULATION_CLOCK', 'POP_PROJ', 'POP_PROJ_REGION', 'PPI',
    'PPI_FD', 'PROV_MORTALITY', 'PROV_MORTALITY_CAUSE', 'PROV_MORTALITY_CAUSE_WK',
    'PROV_MORTALITY_WK', 'QBIS', 'RES_DWELL', 'RES_DWELL_ST', 'RIME_ROME',
    'RIME_SA4_GCCSA_STE_ASGS2021', 'RPPI', 'RT', 'SECURITISERS', 'TRADE_SERV_CNTRY_CY',
    'TRADE_SERV_CNTRY_FY', 'TRADE_SERV_STATE_CY', 'TRADE_SERV_STATE_FY', 'WPI'
]


DOWNLOAD_SPECS = [
    NodeSpec(
        id=f"australian-bureau-of-statistics-{eid.lower().replace('_', '-')}",
        fn=fetch_one,
        args=(eid,),
        deps=(),
        kind="download",
    )
    for eid in ENTITY_IDS
]

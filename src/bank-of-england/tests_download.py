"""Post-DAG health invariants for the Bank of England connector.

Run in-process after the DAG by the connector itself; data is read through the
same subsets_utils loaders the download nodes wrote with.

`spec_ids` is built from every DAG node — both download nodes AND the
`-transform` leaf nodes. Only the download nodes write raw parquet (transforms
publish Delta tables), so each test filters to the download ids first.
"""
from subsets_utils import load_raw_parquet

_IADB_ID = "bank-of-england-iadb-observations"
_TRANSFORM_SUFFIX = "-transform"


def _download_ids(spec_ids):
    """Raw-asset ids only: drop the transform leaf nodes from the DAG node set."""
    return [s for s in spec_ids if not s.endswith(_TRANSFORM_SUFFIX)]


def test_all_raw_assets_nonempty(spec_ids):
    """Every download asset must hold rows — an empty payload usually means the
    endpoint switched format, the workbook layout changed, or Akamai blocked us."""
    for sid in _download_ids(spec_ids):
        table = load_raw_parquet(sid)
        assert table.num_rows > 0, f"{sid}: raw parquet has 0 rows"


def test_iadb_long_format(spec_ids):
    """IADB observations should be a real long-format corpus: many series,
    plenty of dated observations, and numeric values present."""
    if _IADB_ID not in _download_ids(spec_ids):
        return
    table = load_raw_parquet(_IADB_ID)
    assert table.num_rows >= 1000, f"iadb only {table.num_rows} observations"
    codes = set(table.column("series_code").to_pylist())
    assert len(codes) >= 5, f"iadb only {len(codes)} distinct series codes"
    values = table.column("value").to_pylist()
    assert any(v is not None for v in values), "iadb: all values null"


def test_cell_grids_have_columns(spec_ids):
    """Workbook cell-grids must carry the (sheet,row,col,value) shape."""
    for sid in _download_ids(spec_ids):
        if sid == _IADB_ID:
            continue
        table = load_raw_parquet(sid)
        names = set(table.column_names)
        assert {"sheet", "row", "col", "value"}.issubset(names), (
            f"{sid}: missing cell-grid columns, has {names}"
        )

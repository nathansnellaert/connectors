"""Health invariants for the Bundesbank download step.

Each raw asset is a Bundesbank-CSV ZIP for one whole dataflow. We check the
payload is a valid, non-empty ZIP whose member CSVs actually carry bytes —
the failure modes file-existence misses (empty bodies, truncated downloads,
a silent switch away from the ZIP surface, error JSON saved as data)."""
import io
import zipfile

from subsets_utils import load_raw_file


def test_all_raw_assets_are_valid_zips(spec_ids):
    """Every spec's raw asset opens as a ZIP with >=1 member CSV holding data."""
    for sid in spec_ids:
        content = load_raw_file(sid, extension="zip", binary=True)
        assert content, f"{sid}: raw zip is empty"
        assert content[:2] == b"PK", f"{sid}: raw asset is not a ZIP (head={content[:16]!r})"
        with zipfile.ZipFile(io.BytesIO(content)) as zf:
            infos = zf.infolist()
            assert infos, f"{sid}: ZIP has no members"
            assert any(zi.file_size > 0 for zi in infos), \
                f"{sid}: all ZIP members are empty"
            # Members are CSVs in Bundesbank's wide layout.
            assert any(zi.filename.lower().endswith(".csv") for zi in infos), \
                f"{sid}: ZIP holds no .csv members ({[z.filename for z in infos][:3]})"

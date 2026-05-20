import json
from deltalake import DeltaTable
from .config import subsets_uri, get_storage_options


def publish(dataset_name: str, metadata: dict):
    """Publish metadata to a Delta table."""
    if 'id' not in metadata:
        raise ValueError("Missing required field: 'id'")
    if 'title' not in metadata:
        raise ValueError("Missing required field: 'title'")

    uri = subsets_uri(dataset_name)
    storage_opts = get_storage_options()
    dt = DeltaTable(uri, storage_options=storage_opts) if storage_opts else DeltaTable(uri)

    # Idempotent: skip if metadata unchanged
    existing = json.loads(dt.metadata().description or "{}")
    if existing == metadata:
        print(f"Metadata unchanged for {dataset_name}")
        return

    # Validate column descriptions against actual schema
    schema = dt.schema().to_pyarrow() if hasattr(dt.schema(), 'to_pyarrow') else dt.schema().to_arrow()
    actual_columns = {field.name for field in schema}

    if 'column_descriptions' in metadata:
        col_descs = json.loads(metadata['column_descriptions']) if isinstance(
            metadata['column_descriptions'], str
        ) else metadata['column_descriptions']
        invalid = set(col_descs.keys()) - actual_columns
        if invalid:
            raise ValueError(f"Invalid columns in descriptions: {sorted(invalid)}")
        undescribed = actual_columns - set(col_descs.keys())
        if undescribed:
            print(f"  Warning: {len(undescribed)} column(s) without descriptions: {sorted(undescribed)}")
    else:
        print(f"  Warning: no column_descriptions provided ({len(actual_columns)} columns undescribed)")

    # Delta's table description field has a hard 4000-char cap. Column
    # descriptions are the usual culprit when they push us past it; drop them
    # rather than crash the run, and surface the omission as a warning.
    desc_json = json.dumps(metadata)
    if len(desc_json) > 4000:
        slim = {k: v for k, v in metadata.items() if k != "column_descriptions"}
        desc_json = json.dumps(slim)
        if len(desc_json) > 4000:
            raise ValueError(
                f"{dataset_name}: metadata JSON is {len(desc_json)} chars "
                f"even after dropping column_descriptions; delta cap is 4000"
            )
        print(f"  Warning: column_descriptions omitted for {dataset_name} (metadata exceeded 4000 chars)")

    dt.alter.set_table_description(desc_json)
    print(f"Published metadata for {dataset_name}")

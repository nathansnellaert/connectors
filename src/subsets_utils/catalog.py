"""Catalog sync logic for catalog-type connectors.

Each connector implements fetch_catalog() → dict[str, {"title": str, "metadata": dict}].
This module handles diff detection, status management, and persistence.
"""
import json
from datetime import datetime, timezone
from pathlib import Path


STATUS_FILE = Path("catalog/status.json")


def sync_catalog(catalog_items: dict, catalog_source: str, status_file: Path = STATUS_FILE):
    """Sync upstream catalog items into status.json.

    - New items added as disabled with reason "awaiting curation"
    - Existing items: metadata updated, status/reason/node preserved
    - Removed items: left as-is (manual archive)

    Args:
        catalog_items: {id: {"title": str, "metadata": dict}} from fetch_catalog()
        catalog_source: URL or description of the catalog source
        status_file: Path to status.json (default: catalog/status.json)
    """
    if status_file.exists():
        status = json.loads(status_file.read_text())
    else:
        status = {"_meta": {}, "datasets": {}}

    datasets = status.get("datasets", {})
    known_ids = set(datasets.keys())
    catalog_ids = set(catalog_items.keys())

    new = catalog_ids - known_ids
    removed = known_ids - catalog_ids

    # Update metadata for existing (preserve status/reason/node)
    for ds_id in known_ids & catalog_ids:
        item = catalog_items[ds_id]
        existing = datasets[ds_id]
        datasets[ds_id] = {
            "title": item["title"],
            "metadata": item["metadata"],
            "status": existing.get("status", "disabled"),
            "reason": existing.get("reason"),
            "node": existing.get("node"),
        }

    # Add new as awaiting curation
    for ds_id in new:
        item = catalog_items[ds_id]
        datasets[ds_id] = {
            "title": item["title"],
            "metadata": item["metadata"],
            "status": "disabled",
            "reason": "awaiting curation",
            "node": None,
        }

    status["datasets"] = datasets
    status["_meta"] = {
        "catalog_source": catalog_source,
        "last_synced": datetime.now(timezone.utc).isoformat(),
    }

    status_file.parent.mkdir(parents=True, exist_ok=True)
    status_file.write_text(json.dumps(status, indent=2, ensure_ascii=False))

    print(f"Synced: {len(catalog_items)} in catalog, {len(new)} new, {len(removed)} removed")
    return {"new": new, "removed": removed, "total": len(catalog_items)}

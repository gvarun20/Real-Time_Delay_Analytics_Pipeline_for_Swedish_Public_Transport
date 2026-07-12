#!/usr/bin/env python3
"""Print a human-readable summary of the raw landing zone."""

from __future__ import annotations

import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import DATA_RAW_DIR, OPERATOR, OPERATOR_NAME  # noqa: E402


def main() -> int:
    print(f"Operator: {OPERATOR_NAME} ({OPERATOR})")
    print(f"Raw base: {DATA_RAW_DIR}")
    print()

    static_root = DATA_RAW_DIR / "static"
    if static_root.exists():
        print("=== Static GTFS ===")
        for date_dir in sorted(static_root.iterdir()):
            if not date_dir.is_dir():
                continue
            zip_path = date_dir / "gtfs.zip"
            meta_path = date_dir / "metadata.json"
            if zip_path.exists():
                mb = zip_path.stat().st_size / (1024 * 1024)
                print(f"  {date_dir.name}: gtfs.zip ({mb:.1f} MB)")
            if meta_path.exists():
                meta = json.loads(meta_path.read_text(encoding="utf-8"))
                print(f"    pulled: {meta.get('pulled_at_utc', '?')}")
    else:
        print("=== Static GTFS === (none)")

    print()
    rt_root = DATA_RAW_DIR / "realtime"
    if rt_root.exists():
        print("=== Realtime TripUpdates ===")
        for date_dir in sorted(rt_root.iterdir()):
            if not date_dir.is_dir():
                continue
            snapshots = sorted(d for d in date_dir.iterdir() if d.is_dir())
            print(f"  {date_dir.name}: {len(snapshots)} snapshot(s)")
            if snapshots:
                latest = snapshots[-1]
                pb = latest / "tripupdates.pb"
                meta_path = latest / "metadata.json"
                if pb.exists():
                    print(f"    latest: {latest.name}/tripupdates.pb ({pb.stat().st_size:,} bytes)")
                if meta_path.exists():
                    meta = json.loads(meta_path.read_text(encoding="utf-8"))
                    print(f"    entities: {meta.get('record_count', '?')}")
    else:
        print("=== Realtime TripUpdates === (none)")

    print()
    audit = PROJECT_ROOT / "logs" / "ingest_runs.jsonl"
    if audit.exists():
        lines = audit.read_text(encoding="utf-8").strip().splitlines()
        print(f"Ingest audit log: {len(lines)} run(s) in logs/ingest_runs.jsonl")
    else:
        print("Ingest audit log: (none yet)")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

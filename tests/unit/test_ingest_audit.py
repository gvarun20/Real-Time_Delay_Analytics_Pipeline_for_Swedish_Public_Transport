"""Tests for ingest audit logging."""

import json

from jobs.ingest.audit import record_ingest_run


def test_record_ingest_run_appends_jsonl(tmp_path, monkeypatch):
    log_file = tmp_path / "ingest_runs.jsonl"
    monkeypatch.setattr("jobs.ingest.audit.INGEST_LOG", log_file)

    record_ingest_run(
        feed_type="static_gtfs",
        service_date="2026-07-11",
        file_path="/data/raw/static/2026-07-11/gtfs.zip",
        bytes_written=1000,
        record_count=5,
        dag_id="gtfs_static_ingest",
        task_id="fetch_static_gtfs",
    )

    lines = log_file.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 1
    entry = json.loads(lines[0])
    assert entry["feed_type"] == "static_gtfs"
    assert entry["bytes"] == 1000
    assert entry["dag_id"] == "gtfs_static_ingest"

# Week 1 Completion Checklist

Use this to confirm Week 1 is **fully done** before starting PySpark (Week 2).

## Environment

- [ ] Docker Desktop running
- [ ] `docker compose ps` shows healthy `transit-delay-pipeline-*` containers
- [ ] Airflow UI opens at http://localhost:8081 (`admin` / `admin`)
- [ ] `.env` has both API keys (not placeholders)
- [ ] `STATIC_FEED=gtfs_sweden_3` and `REALTIME_FEED=gtfs_regional`

## API access

- [ ] `scripts/test_api_key.py` returns **OK: Both keys work**
- [ ] Static test: HTTP 200
- [ ] Realtime test: HTTP 200

## Raw data landing

- [ ] `data/raw/static/YYYY-MM-DD/gtfs.zip` exists (large file, ~800MB+)
- [ ] `data/raw/static/YYYY-MM-DD/metadata.json` exists
- [ ] `data/raw/realtime/YYYY-MM-DD/HH-mm-ss/tripupdates.pb` exists
- [ ] `data/raw/realtime/.../metadata.json` exists with `record_count` > 0

## Airflow

- [ ] `airflow dags list` shows `gtfs_static_ingest` and `gtfs_realtime_ingest`
- [ ] Both DAGs are **unpaused**
- [ ] Manual trigger of `gtfs_static_ingest` succeeds (green)
- [ ] Manual trigger of `gtfs_realtime_ingest` succeeds (green)
- [ ] Task logs show file paths under `data/raw/`

## Database (schema only — Week 1)

- [ ] Postgres on `localhost:5433` accepts connection (`transit` / `transit`)
- [ ] Tables exist: `dim_date`, `dim_route`, `dim_stop`, `dim_vehicle_type`, `fact_trip_delay`, `pipeline_runs`
- [ ] `dim_date` has rows (2024–2027 seeded)
- [ ] `fact_trip_delay` is **empty** (expected until Week 2)

## Tests & docs

- [ ] `pytest` passes locally
- [ ] `scripts/verify_week1.ps1` passes
- [ ] Read [01-project-purpose-and-goals.md](01-project-purpose-and-goals.md)

## Quick verify command

```powershell
.\scripts\verify_week1.ps1
```

---

**Week 1 phase gate (from master plan):**  
> Two Airflow tasks land raw static + realtime files on schedule; folder structure is deterministic.

When all boxes above are checked → proceed to **Week 2: PySpark transform**.

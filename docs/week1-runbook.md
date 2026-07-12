# Week 1 Runbook — Ingestion Operations

Day-to-day guide for running and verifying the GTFS ingestion layer.

## Start the stack

```powershell
cd E:\SUMMER_3RD_PROJECT
.\scripts\bootstrap.ps1
```

Or if already initialized:

```powershell
docker compose up -d
```

## Configure API keys (one-time)

Edit `.env`:

```env
TRAFIKLAB_STATIC_API_KEY=<from GTFS Sweden 3 Static data>
TRAFIKLAB_REALTIME_API_KEY=<from GTFS Regional Realtime>
STATIC_FEED=gtfs_sweden_3
REALTIME_FEED=gtfs_regional
OPERATOR=sl
```

Reload containers after any `.env` change:

```powershell
docker compose up -d --force-recreate airflow-scheduler airflow-webserver
```

## Verify API access

```powershell
docker compose exec airflow-scheduler python /opt/airflow/project/scripts/test_api_key.py --operator sl
```

Expected output ends with: `OK: Both keys work.`

## Manual download (without Airflow)

```powershell
.\scripts\run_first_download.ps1
```

## Airflow operations

| Action | How |
|---|---|
| Open UI | http://localhost:8081 |
| List DAGs | `docker compose exec airflow-scheduler airflow dags list` |
| Trigger static DAG | UI → `gtfs_static_ingest` → Trigger DAG |
| Trigger realtime DAG | UI → `gtfs_realtime_ingest` → Trigger DAG |
| View task logs | UI → DAG → Graph → Task → Log |

### Expected schedules

| DAG | Schedule | Typical runtime |
|---|---|---|
| `gtfs_static_ingest` | Daily midnight UTC | 2–5 min (large zip) |
| `gtfs_realtime_ingest` | Every 15 minutes | < 10 sec |

## Inspect landed data

```powershell
# Summary of raw landing zone
docker compose exec airflow-scheduler python /opt/airflow/project/scripts/ingest_summary.py

# List files
Get-ChildItem -Recurse data\raw | Where-Object { -not $_.PSIsContainer }
```

## Ingest audit log

Each successful ingest appends a line to `logs/ingest_runs.jsonl`:

```json
{"feed_type": "static_gtfs", "service_date": "2026-07-11", "file_path": "...", "bytes": 815038011, "record_count": 42}
```

## Troubleshooting

| Symptom | Fix |
|---|---|
| `403 Forbidden` on static | Wrong key in `TRAFIKLAB_STATIC_API_KEY`; need GTFS Sweden 3 Static key |
| `403 Forbidden` on realtime | Wrong endpoint; set `REALTIME_FEED=gtfs_regional` |
| `Key does not have access to file` | Subscribe to correct API product on developer.trafiklab.se |
| Airflow UI not loading | Check port **8081** (not 8080); `docker compose ps` |
| Postgres connection refused on 5432 | Use port **5433** for analytics DB |
| DAG not visible | `docker compose logs airflow-scheduler` for import errors |
| Static download very slow | Normal — full Sweden zip is ~800MB |

## Stop the stack

```powershell
docker compose down
```

Data in `data/raw/` and Docker volumes persist. To wipe volumes: `docker compose down -v` (destructive).

## Week 1 full verification

```powershell
.\scripts\verify_week1.ps1
```

See [WEEK1_CHECKLIST.md](WEEK1_CHECKLIST.md) for the complete gate criteria.

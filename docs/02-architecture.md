# System Architecture

## High-level overview

```mermaid
flowchart TB
    subgraph Sources["External sources"]
        TL_S[Trafiklab GTFS Sweden 3 Static]
        TL_R[Trafiklab GTFS Regional Realtime]
    end

    subgraph Orchestration["Apache Airflow (Docker)"]
        DAG_S[gtfs_static_ingest<br/>@daily]
        DAG_R[gtfs_realtime_ingest<br/>every 15 min]
    end

    subgraph Raw["Raw landing zone"]
        STATIC[data/raw/static/YYYY-MM-DD/gtfs.zip]
        RT[data/raw/realtime/YYYY-MM-DD/HH-mm-ss/tripupdates.pb]
    end

    subgraph Transform["PySpark (Week 2)"]
        SPARK[transform_gtfs.py]
    end

    subgraph Warehouse["PostgreSQL transit_dw"]
        DIMS[dim_date · dim_route · dim_stop · dim_vehicle_type]
        FACT[fact_trip_delay]
    end

    subgraph Serve["Streamlit (Week 3)"]
        DASH[Dashboard]
    end

    TL_S --> DAG_S --> STATIC
    TL_R --> DAG_R --> RT
    STATIC --> SPARK
    RT --> SPARK
    SPARK --> DIMS
    SPARK --> FACT
    FACT --> DASH
    DIMS --> DASH
```

## Component map

| Component | Role | Port (local) |
|---|---|---|
| `gtfs_static_ingest` | Daily pull of Sweden GTFS zip | — |
| `gtfs_realtime_ingest` | 15-min TripUpdates snapshots for SL | — |
| Airflow webserver | UI + DAG management | **8081** |
| Postgres `transit_dw` | Star schema warehouse | **5433** |
| Postgres `airflow` | Airflow metadata (internal) | internal |
| Raw landing zone | Immutable ingest snapshots | `data/raw/` |

## Data contracts

### Static landing
```
data/raw/static/{YYYY-MM-DD}/gtfs.zip
data/raw/static/{YYYY-MM-DD}/metadata.json
```

### Realtime landing
```
data/raw/realtime/{YYYY-MM-DD}/{HH-mm-ss}/tripupdates.pb
data/raw/realtime/{YYYY-MM-DD}/{HH-mm-ss}/metadata.json
```

### API key mapping (Trafiklab)

| Product | Env variable | Endpoint pattern |
|---|---|---|
| GTFS Sweden 3 Static | `TRAFIKLAB_STATIC_API_KEY` | `/gtfs-sweden/sweden.zip` |
| GTFS Regional Realtime | `TRAFIKLAB_REALTIME_API_KEY` | `/gtfs-rt/{operator}/TripUpdates.pb` |

See [decisions/002-dual-api-keys.md](decisions/002-dual-api-keys.md).

## Star schema (target state — Week 2+)

**Fact grain:** one row per `(trip_id, stop_id, service_date, stop_sequence)`

| Table | Purpose |
|---|---|
| `dim_date` | Calendar attributes (weekend, day name) |
| `dim_route` | Route metadata + operator |
| `dim_stop` | Stop name, lat/lon |
| `dim_vehicle_type` | Bus, metro, rail, etc. |
| `fact_trip_delay` | Scheduled vs actual arrival, `delay_seconds` |

DDL: [../sql/schema.sql](../sql/schema.sql)

## Why two ingest DAGs (not one)

Static GTFS changes **daily** (~815 MB for all Sweden). Realtime TripUpdates change every **15 seconds** but we snapshot every **15 minutes** to stay within API quotas.

Different schedules → separate DAGs:

- `gtfs_static_ingest` — `@daily`
- `gtfs_realtime_ingest` — `*/15 * * * *`

Week 2 will add `gtfs_transform` DAG (or extend a master DAG) that runs **after** raw files exist.

## Failure handling (Week 1)

- **Retries:** 3 attempts, 5-minute delay (`dags/common.py`)
- **on_failure_callback:** writes to `logs/failures/`
- **Ingest audit:** append-only `logs/ingest_runs.jsonl`

## Repository layout

```
dags/           Airflow DAG definitions
jobs/ingest/    Download + land GTFS files
jobs/           transform_gtfs.py (Week 2), validate (Week 3)
config/         Settings, URL builders
sql/            Star schema DDL
scripts/        Bootstrap, verify, test API
docs/           Purpose, architecture, runbooks
data/raw/       Raw landing zone (gitignored)
```

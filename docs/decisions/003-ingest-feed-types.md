# ADR 003: Static and Realtime Feed URL Types

**Status:** Accepted  
**Date:** 2026-07-11

## Context

Trafiklab has multiple GTFS API families with **different URL paths** and **different API key products**:

| API family | Static URL | Realtime URL |
|---|---|---|
| GTFS Sweden 3 / Sweden Realtime | `/gtfs-sweden/sweden.zip` | `/gtfs-rt-sweden/{op}/TripUpdatesSweden.pb` |
| GTFS Regional | `/gtfs/{op}/{op}.zip` | `/gtfs-rt/{op}/TripUpdates.pb` |

Our keys map to **Sweden 3 Static** + **Regional Realtime**, so we need a hybrid configuration.

## Decision

Environment variables control feed type:

```env
STATIC_FEED=gtfs_sweden_3      # uses /gtfs-sweden/sweden.zip
REALTIME_FEED=gtfs_regional    # uses /gtfs-rt/sl/TripUpdates.pb
```

Implemented in `config/settings.py` → `static_gtfs_url()` and `trip_updates_url()`.

## Consequences

- Week 2 PySpark must **filter Sweden-wide static zip to SL operator** (agency_id 275 / operator SL)
- If user later adds GTFS Regional Static key, they can set `STATIC_FEED=gtfs_regional` for smaller per-operator zips
- Documentation and test scripts must reference feed types, not a single generic "GTFS API"

# ADR 002: Separate Trafiklab API Keys

**Status:** Accepted  
**Date:** 2026-07-11

## Context

Trafiklab issues **different API keys per product** on developer.trafiklab.se. A single `TRAFIKLAB_API_KEY` env var caused 403 errors when the realtime key was used for static endpoints (or vice versa).

## Decision

Use two environment variables:

| Variable | Trafiklab product |
|---|---|
| `TRAFIKLAB_STATIC_API_KEY` | GTFS Sweden 3 Static data |
| `TRAFIKLAB_REALTIME_API_KEY` | GTFS Regional Realtime |

`TRAFIKLAB_API_KEY` remains as an optional fallback for both.

## Consequences

- `.env` must document both keys clearly
- Docker Compose passes both variables to Airflow containers
- Fetch scripts use the correct key per feed type
- Test script (`scripts/test_api_key.py`) validates each key independently

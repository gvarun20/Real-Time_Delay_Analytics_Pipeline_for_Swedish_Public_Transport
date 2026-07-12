# Architecture Decision: Primary Operator

**Decision:** Start with **SL (Stockholm)** as the primary operator (`operator=sl`).

**Rationale:**
- High data volume and strong GTFS Sweden 3 + realtime coverage
- Well-documented Trafiklab feeds
- Recognizable name for portfolio reviewers

**Alternatives considered:** Östgötatrafiken (`otraf`) for a local Linköping angle.

**Consequences:** All dimension `operator` fields default to `SL`. Multi-operator support is a Week 2+ optional extension.

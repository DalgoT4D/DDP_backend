# ChromaDB Sizing — Dashboard Chat Feature

## Context

- Embedding model: `text-embedding-3-small` — **1536 dimensions**
- One ChromaDB collection per org per version (multi-tenant)
- Collection name format: `org_{id}__{timestamp}` — new collection created on every dbt docs rebuild
- Active sessions pin their collection — up to **2 collections per org** can be live simultaneously (old pinned by active sessions + new from latest rebuild)
- Old collections are garbage collected after **24 hours** of session inactivity
- Concurrent users: **~4 per org**
- Target scale: **50 orgs**
- ChromaDB runs as a remote HTTP server (`AI_DASHBOARD_CHAT_CHROMA_HOST`)

---

## Memory Calculation (50 orgs)

```
total vectors    = 50 orgs × 300 docs × 2 collections = 30,000 vectors
raw payload      = 30,000 × 1,536 dims × 4 bytes      = ~185 MB
with +30% overhead                                     = ~240 MB
ChromaDB process base                                  = ~300 MB
OS reservation                                         = ~1 GB
─────────────────────────────────────────────────────────────────
Total                                                  = ~1.5 GB
```

---

## CPU Calculation (50 orgs, peak)

```
4 users/org × 10 active orgs × 3 queries/turn = 120 concurrent queries
```

Chroma queries parallelise up to vCPU count. Beyond that, queries queue.

---

## Machine Recommendation

### Recommended: `t3.medium` (2 vCPU, 4 GB RAM, ~$31/month)

- 4 GB RAM comfortably covers 1.5 GB with headroom
- 2 vCPUs means queries queue at peak but requests do not fail — just slower during bursts
- **Schedule dbt rebuilds off-hours** — rebuild is bursty (all dbt models ingested at once) and will saturate 2 vCPUs while it runs

### If query latency at peak matters: `t3.xlarge` (4 vCPU, 16 GB RAM, ~$122/month)

- 4 vCPUs eliminates most queuing at 120 concurrent queries
- ~$90/month more than t3.medium

---

## Why a Separate Machine

1. **Platform isolation** — memory spikes during vector rebuild must not OOM the Django process serving all orgs
2. **Independent scaling** — memory grows linearly with org count; scale ChromaDB independently
3. **Already designed for it** — `AI_DASHBOARD_CHAT_CHROMA_HOST` and `AI_DASHBOARD_CHAT_CHROMA_PORT` env vars are in place

---

## LRU Cache — Required

Without LRU, all accessed collections stay in memory permanently. Must be configured:

```bash
CHROMA_SEGMENT_CACHE_POLICY=LRU
CHROMA_MEMORY_LIMIT_BYTES=3000000000  # 3 GB
```

> **Warning:** LRU enforcement is unreliable in Chroma v0.5.x (GitHub issue #1323). Verify your version and monitor memory in production.

---

## Sources

- [ChromaDB Single-Node Performance Benchmarks](https://docs.trychroma.com/guides/deploy/performance)
- [ChromaDB Resource Requirements — Cookbook](https://cookbook.chromadb.dev/core/resources/)
- [ChromaDB Memory Management (LRU)](https://cookbook.chromadb.dev/strategies/memory-management/)

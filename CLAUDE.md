# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Distributed web crawling pipeline that crawls the Tranco Top 1M websites using a 2-machine setup:
- **Mac M2**: Fast HTTP ingestor (httpx, 500 concurrent requests) → produces to Kafka
- **Linux Desktop**: 4-stage Kafka stream pipeline (Router → Fast/Rich Processors → Storage → Embedding)

The project also has an alternative **Sharded Crawler** mode using Redis-based distributed work queues.

## Commands

All primary operations are managed through `Makefile` (run from `crawler-challenge/`):

```bash
# Crawling
make test-crawl                    # 100 URLs, 1 worker
make test-crawl CRAWL_LIMIT=500 WORKERS=4
make test-crawl-full               # Full 1M URLs, 4 workers
make status                        # Service status
make restart-desktop               # Restart pipeline services
make logs SVC=router               # View specific service logs
```

```bash
# Install
pip install -e ".[mac]"            # Mac ingestor
pip install -e ".[desktop]"        # Full desktop pipeline
pip install -e ".[dev]"            # Dev tools (pytest, mypy)
pip install -e ".[api]"            # FastAPI backend
```

```bash
# Infrastructure
cd crawler-challenge
docker compose -f docker/docker-compose.stream.yml up -d
```

```bash
# Tests
pytest tests/                      # All tests
pytest tests/test_kafka_url_embedding.py tests/test_entrypoint_separation.py  # Regression suite
```

## Architecture

### Stream Pipeline (Main Path)

```
Mac Ingestor (httpx, 500 concurrent)
    → Kafka: raw.page
    → Router (scores 0–100, threshold 80)
        → Kafka: process.fast  → Fast Worker (BeautifulSoup, 6 workers, ~150ms/page)
        → Kafka: process.rich  → Rich Worker (Crawl4AI, 2 workers, ~2–5s/page)
    → Kafka: processed.final
    → Storage: MinIO (markdown) + PostgreSQL (metadata)
    → Embedding: pgvector (optional, RAG search)
```

The **Router** scores each page on content structure, size, and language. Pages scoring ≥80 go to `process.fast`; below 80 to `process.rich`.

The **URL feedback loop**: `discovered.urls` Kafka topic → `url_queue_consumer` → Redis shards → re-queued for crawling.

### Sharded Crawler (Alternative Mode)

- `runners/sharded_master.py`: loads Tranco URLs into 3 Redis shards with 4 priority levels (high/medium/normal/low for top-100 / top-1K / top-10K / rest)
- `runners/sharded_worker.py`: async HTTP crawlers consuming from all shards
- Workers go through `src/core/polite_crawler.py` which enforces robots.txt and rate limiting

### Key Source Directories

- `src/common/` — Kafka config (env-driven), Zstd compression, URL normalization; shared by all components
- `src/ingestor/` — httpx async crawler + Kafka producer (Mac layer)
- `src/router/` — Kafka consumer/producer with `page_analyzer.py` + `scoring.py`
- `src/processor/` — `fast_worker.py` (BeautifulSoup) and `rich_worker.py` (Crawl4AI), both extend `base_worker.py`
- `src/storage/` — `hybrid_storage.py` orchestrates `minio_writer.py` + `postgres_writer.py`
- `src/embedding/` — chunk → embed → pgvector; supports local (all-MiniLM-L6-v2) or OpenAI backend
- `src/managers/` — `sharded_queue_manager.py` (Redis 3-shard), `tranco_manager.py`, `url_queue_consumer.py`
- `runners/` — standalone entry points for each pipeline stage
- `desktop/start.sh` — single command to start all desktop services

### Infrastructure (docker-compose.stream.yml)

- **Kafka** + Kafka UI (localhost:8080)
- **Redis** (3 logical DBs as shards: DB1, DB2, DB3)
- **PostgreSQL 15 + pgvector** — schema in `docker/init-stream.sql`; tables: `pages`, `crawl_stats`, `page_chunks`
- **MinIO** — Console at localhost:9001; buckets: `raw-html`, `processed-markdown`, `crawl-metadata`
- **Prometheus** (localhost:9090) + **Grafana** (localhost:3000)

## Key Environment Variables

| Variable | Default | Purpose |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | localhost:9092 | Kafka connection |
| `ROUTER_STATIC_THRESHOLD` | 80 | fast vs rich routing cutoff |
| `INGESTOR_MAX_CONCURRENT` | 500 | Mac crawl parallelism |
| `INGESTOR_TIMEOUT` | 15.0 | Per-request timeout (s) |
| `EMBED_BACKEND` | local | `local` or `openai` |
| `EMBED_MODEL_NAME` | all-MiniLM-L6-v2 | 384-dim embedding model |
| `MINIO_ENDPOINT` | localhost:9000 | Object storage |
| `POSTGRES_HOST` | localhost | DB host |

## CI/CD

GitHub Actions (`.github/workflows/deploy.yml`):
- **Push to master**: runs regression tests only (no deploy)
- **workflow_dispatch with `accept`**: tests + deploys to self-hosted Mac and Desktop runners
- Mac deploy: Docker Compose sharded crawler
- Desktop deploy: `pip install` + service restart via `desktop/start.sh`

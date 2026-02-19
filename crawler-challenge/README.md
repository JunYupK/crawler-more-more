# Crawler Stream Pipeline

고처리량 웹 크롤링 스트림 파이프라인. Mac(크롤링) + Desktop(처리/저장/임베딩) 분산 아키텍처.

## Architecture

```
╔══════════════════════════════════════════════════════════════════════════════╗
║  Mac  (Layer 1 — Ingestor)                                                  ║
║                                                                              ║
║  ┌─────────────────────────────────────────┐                                ║
║  │  httpx  ·  500 concurrent  ·  15s timeout│                               ║
║  │  Zstd compress (level 3)                 │                               ║
║  │  → Kafka  raw.page  (10 MB msg limit)    │                               ║
║  └──────────────────────┬──────────────────┘                                ║
╚═════════════════════════│════════════════════════════════════════════════════╝
                          │  Kafka (KRaft, 12 partitions)
╔═════════════════════════▼════════════════════════════════════════════════════╗
║  Desktop  (Layers 2 – 6)                                                    ║
║                                                                              ║
║  Layer 2 ─ Router  ──────────────────────────────────────────────────────   ║
║  │  page_analyzer  →  scoring (threshold 80)                                ║
║  │  score ≥ 80  →  process.fast  (≈70% traffic)                            ║
║  └  score  < 80  →  process.rich  (≈30% traffic)                           ║
║                                                                              ║
║  Layer 3 ─ Processors  ──────────────────────────────────────────────────   ║
║  │  FastProcessor  (×6 workers)   BeautifulSoup + lxml  →  Markdown        ║
║  └  RichProcessor  (×2 workers)   Crawl4AI + Playwright  →  Markdown       ║
║                   ↓  processed.final  (Markdown + metadata)                 ║
║                                                                              ║
║  Layer 4 ─ Storage  ─────────────────────────────────────────────────────   ║
║  │  MinIO        raw HTML / processed Markdown (S3-compatible)              ║
║  └  PostgreSQL   pages 테이블  (메타데이터 · 중복·DLQ 관리)                 ║
║                                                                              ║
║  Layer 5 ─ Embedding  ───────────────────────────────────────────────────   ║
║  │  Chunker       Heading 기준 분할  ·  최대 500자  ·  URL당 20청크        ║
║  │  Embedder      local: all-MiniLM-L6-v2 (384d)                           ║
║  │                openai: text-embedding-3-small (1536d)                    ║
║  └  pgvector      IVFFlat index  ·  cosine similarity  ·  UPSERT           ║
║                                                                              ║
║  Layer 6 ─ Search API  ──────────────────────────────────────────────────   ║
║     FastAPI + uvicorn  :8600                                                 ║
║     GET /search   자연어 → 임베딩 → pgvector 검색 → JSON                   ║
║     GET /stats    임베딩 현황 (URL 수 · 청크 수)                            ║
║     GET /chunk    특정 URL의 전체 청크 조회                                  ║
║     GET /health   DB + 모델 상태 확인                                        ║
║     GET /docs     Swagger UI (자동 생성)                                     ║
║                                                                              ║
║  URL 피드백 루프  ────────────────────────────────────────────────────────   ║
║  Processor  →  discovered.urls (Kafka)  →  URL Queue Consumer               ║
║  →  Redis priority_low 큐  →  Mac Ingestor 재수집                           ║
╚══════════════════════════════════════════════════════════════════════════════╝

Infrastructure
  Kafka (KRaft)    메시지 버스     :9092   │  Kafka UI    :8080
  MinIO            오브젝트 스토리지 :9000   │  Console     :9001
  PostgreSQL+pgv   메타데이터·벡터  :5432
  Redis            URL 큐          :6379
  Prometheus       메트릭 수집      :9090   │  Grafana     :3000
  Search API       RAG HTTP API    :8600   │  Swagger     :8600/docs
```

### URL 자동 재공급 (피드백 루프)

Fast/Rich Processor가 페이지 처리 중 발견한 링크를 `discovered.urls` 토픽으로 전송하고,
URL Queue Consumer가 이를 소비하여 Redis 크롤러 큐의 `priority_low`에 자동 적재합니다.

---

## Quick Start

### Prerequisites

- Python 3.10+
- Docker & Docker Compose (Desktop 인프라용)

### 1. 패키지 설치

```bash
cd crawler-challenge

# Mac (Ingestor만 실행)
pip install -e ".[mac]"

# Desktop (Router/Processor/Storage/Embedding 실행)
pip install -e ".[desktop]"

# 개발 환경 (테스트 포함)
pip install -e ".[mac,desktop,dev]"
```

### 2. Desktop 인프라 시작

```bash
# Kafka + MinIO + PostgreSQL+pgvector + Redis + 모니터링 시작
docker compose -f docker/docker-compose.stream.yml up -d
```

### 3. 파이프라인 실행

**Desktop — 스크립트로 한번에 실행:**
```bash
# 필수 서비스 (Router, Fast/Rich Processor, Storage)
./desktop/start.sh

# 옵션 추가
./desktop/start.sh --with-url-queue    # + 발견 URL 자동 Redis 재공급
./desktop/start.sh --with-embedding    # + pgvector 벡터 임베딩 (모델 ~2GB)
./desktop/start.sh --all               # 전체 실행

# 관리
./desktop/start.sh status              # 실행 상태 확인
./desktop/start.sh logs router         # 특정 서비스 로그
./desktop/start.sh stop                # 전체 중지
```

**Mac — 인제스터:**
```bash
# Desktop Kafka IP 지정 실행
./mac/start.sh --kafka-servers <Desktop-IP>:9092

# 테스트 모드 (100개)
./mac/start.sh --test

# URL 수 지정
./mac/start.sh --limit 50000 --kafka-servers <Desktop-IP>:9092
```

**개별 실행이 필요한 경우** (`runners/` 전체 옵션 사용):
```bash
python runners/router_runner.py
python runners/fast_processor_runner.py --workers 4
python runners/rich_processor_runner.py --workers 2
python runners/storage_runner.py
python runners/url_queue_runner.py
python runners/embedding_runner.py
```

---

## Project Structure

```
crawler-challenge/
├── src/
│   ├── common/              # 공유 모듈
│   │   ├── compression.py   # Zstd 압축/해제
│   │   ├── kafka_config.py  # 파이프라인 전체 설정 (토픽, 브로커 등)
│   │   └── url_extractor.py # URL 정규화 + 트래킹 파라미터 제거
│   ├── core/                # Sharded Crawler 핵심
│   │   ├── polite_crawler.py
│   │   └── database.py
│   ├── ingestor/            # Layer 1: HTTP 크롤링 + Kafka produce
│   │   ├── httpx_crawler.py
│   │   └── kafka_producer.py
│   ├── router/              # Layer 2: 콘텐츠 분석 + 라우팅
│   │   ├── page_analyzer.py # 프레임워크 감지, 콘텐츠 통계
│   │   ├── scoring.py       # 점수 계산 (≥80 → fast, <80 → rich)
│   │   └── smart_router.py
│   ├── processor/           # Layer 3: HTML → Markdown 변환
│   │   ├── base_worker.py   # 공통 Kafka Consumer 로직
│   │   ├── fast_worker.py   # BeautifulSoup 처리
│   │   └── rich_worker.py   # Crawl4AI 처리
│   ├── storage/             # Layer 4: 저장
│   │   ├── hybrid_storage.py
│   │   ├── minio_writer.py
│   │   └── postgres_writer.py
│   ├── embedding/           # Layer 5: 벡터 임베딩 + RAG 검색
│   │   ├── chunker.py       # Markdown → 청크 분할
│   │   ├── embedder.py      # 임베딩 모델 추상화 (local / OpenAI)
│   │   ├── embedding_worker.py  # Kafka Consumer → pgvector UPSERT
│   │   └── rag_search.py    # 코사인 유사도 벡터 검색
│   ├── api/                 # Layer 6: HTTP Search API
│   │   └── search_api.py    # FastAPI 앱 (search / stats / chunk / health)
│   ├── managers/            # 큐 / 상태 관리
│   │   ├── sharded_queue_manager.py  # Redis 3-Shard 큐
│   │   ├── url_queue_consumer.py     # discovered.urls → Redis 적재
│   │   ├── tranco_manager.py         # URL 우선순위 부여
│   │   └── progress_tracker.py
│   └── monitoring/
│       └── metrics.py       # Prometheus 메트릭 (23개)
│
├── runners/                 # 실행 진입점 (전체 CLI 옵션 포함)
│   ├── sharded_master.py
│   ├── sharded_worker.py
│   ├── ingestor_runner.py
│   ├── router_runner.py
│   ├── fast_processor_runner.py
│   ├── rich_processor_runner.py
│   ├── storage_runner.py
│   ├── url_queue_runner.py
│   └── embedding_runner.py
│
├── mac/                     # Mac 전용 실행 패키지
│   ├── run.py               # 인제스터 진입점
│   ├── start.sh             # 실행 스크립트 (Kafka 연결 확인 포함)
│   └── requirements.txt
│
├── desktop/                 # Desktop 전용 실행 패키지
│   ├── start.sh             # 파이프라인 일괄 실행/중지/상태 관리
│   ├── run_router.py
│   ├── run_fast_processor.py
│   ├── run_rich_processor.py
│   ├── run_storage.py
│   ├── run_url_queue.py     # discovered.urls → Redis 큐
│   ├── run_embedding.py     # pgvector 임베딩 + RAG 검색
│   ├── run_api.py           # Search API 서버 (FastAPI + uvicorn :8600)
│   └── requirements.txt
│
├── docker/
│   ├── docker-compose.stream.yml  # 전체 인프라 (Kafka, MinIO, PG, Redis 등)
│   ├── init-stream.sql            # 스트림 파이프라인 스키마 (pages 테이블 등)
│   ├── init.sql                   # Sharded Crawler 스키마
│   ├── migrations/
│   │   ├── 001_add_crawl_results_table.sql
│   │   └── 002_add_pgvector.sql   # pgvector 확장 + page_chunks 테이블
│   ├── prometheus-stream.yml
│   └── scripts/
│       └── create-topics.sh       # Kafka 토픽 자동 생성
│
├── tests/
├── docs/
├── scripts/
└── pyproject.toml
```

---

## Kafka Topics

| 토픽 | 생산자 | 소비자 | 설명 |
|------|--------|--------|------|
| `raw.page` | Ingestor | Router | HTML 원문 (Zstd 압축) |
| `process.fast` | Router | FastProcessor | 정적 페이지 |
| `process.rich` | Router | RichProcessor | 동적 페이지 (JS 렌더링 필요) |
| `processed.final` | Fast/RichProcessor | Storage, Embedding | 처리 완료 (Markdown + 메타) |
| `discovered.urls` | Fast/RichProcessor | URL Queue Consumer | 크롤링 중 발견된 링크 |
| `*.dlq` | 각 레이어 | — | Dead Letter Queue |

---

## Environment Variables

### Kafka

| 변수 | 기본값 | 설명 |
|------|--------|------|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka 브로커 주소 |
| `KAFKA_PRODUCER_ACKS` | `all` | Producer ack 수준 |
| `KAFKA_PRODUCER_COMPRESSION` | `lz4` | Kafka 메시지 압축 |
| `KAFKA_CONSUMER_GROUP_PREFIX` | `crawler-stream` | Consumer group 접두사 |

### MinIO (Object Storage)

| 변수 | 기본값 | 설명 |
|------|--------|------|
| `MINIO_ENDPOINT` | `localhost:9000` | MinIO 엔드포인트 |
| `MINIO_ACCESS_KEY` | `minioadmin` | 접근 키 |
| `MINIO_SECRET_KEY` | `minioadmin123` | 시크릿 키 |

> **보안 주의**: 운영 환경에서는 `MINIO_SECRET_KEY`, `POSTGRES_PASSWORD`를 반드시 환경변수로 설정하세요.

### PostgreSQL

| 변수 | 기본값 | 설명 |
|------|--------|------|
| `POSTGRES_HOST` | `localhost` | DB 호스트 |
| `POSTGRES_PORT` | `5432` | DB 포트 |
| `POSTGRES_DB` | `crawler_stream` | 데이터베이스명 |
| `POSTGRES_USER` | `crawler` | DB 사용자 |
| `POSTGRES_PASSWORD` | `crawler123` | DB 비밀번호 |

### Ingestor

| 변수 | 기본값 | 설명 |
|------|--------|------|
| `INGESTOR_MAX_CONCURRENT` | `500` | 최대 동시 요청 수 |
| `INGESTOR_TIMEOUT` | `15.0` | HTTP 요청 타임아웃 (초) |
| `INGESTOR_COMPRESSION_LEVEL` | `3` | Zstd 압축 레벨 (1-22) |
| `INGESTOR_BATCH_SIZE` | `1000` | Kafka 배치 크기 |

### Router / Processor

| 변수 | 기본값 | 설명 |
|------|--------|------|
| `ROUTER_STATIC_THRESHOLD` | `80` | 정적 페이지 판정 점수 |
| `PROCESSOR_FAST_WORKERS` | `6` | Fast Worker 수 |
| `PROCESSOR_RICH_WORKERS` | `2` | Rich Worker 수 |
| `PROCESSOR_BROWSER_POOL` | `5` | 브라우저 풀 크기 |

### Embedding

| 변수 | 기본값 | 설명 |
|------|--------|------|
| `EMBED_BACKEND` | `local` | `local` 또는 `openai` |
| `EMBED_MODEL_NAME` | `all-MiniLM-L6-v2` | 로컬 임베딩 모델명 (384차원) |
| `OPENAI_API_KEY` | — | OpenAI 임베딩 사용 시 필수 |

### URL Queue Consumer

| 변수 | 기본값 | 설명 |
|------|--------|------|
| `URL_QUEUE_TOTAL_LIMIT` | `5000000` | 전체 pending URL 상한 |
| `URL_QUEUE_DOMAIN_LIMIT` | `100` | 도메인별 최대 적재 URL 수 |
| `REDIS_HOST` | `localhost` | Redis 호스트 |

### Docker Compose 전용

| 변수 | 기본값 | 설명 |
|------|--------|------|
| `KAFKA_EXTERNAL_HOST` | `localhost` | Kafka 외부 접근 호스트 (Mac → Desktop 연결 시 Desktop IP) |
| `KAFKA_UI_USER` | `admin` | Kafka UI 로그인 ID |
| `KAFKA_UI_PASSWORD` | `admin123` | Kafka UI 로그인 비밀번호 |
| `GRAFANA_PASSWORD` | `admin123` | Grafana 관리자 비밀번호 |

---

## Monitoring

인프라 시작 후 접속 가능한 웹 UI:

| 서비스 | URL | 설명 |
|--------|-----|------|
| Kafka UI | http://localhost:8080 | Kafka 토픽/컨슈머/메시지 모니터링 |
| MinIO Console | http://localhost:9001 | Object Storage 관리 |
| Prometheus | http://localhost:9090 | 메트릭 수집 |
| Grafana | http://localhost:3000 | 대시보드 |
| Search API | http://localhost:8600 | RAG 벡터 검색 REST API |
| Swagger UI | http://localhost:8600/docs | Search API 문서 (자동 생성) |

---

## Embedding & RAG Search

```bash
# 임베딩 워커 실행 (processed.final 소비 → pgvector 저장)
python desktop/run_embedding.py

# 저장된 벡터로 유사 문서 검색
python desktop/run_embedding.py --search "검색할 내용"

# 연결 상태 확인
python desktop/run_embedding.py --test-connection

# 통계 조회
python desktop/run_embedding.py --stats
```

### 청크 처리 방식

```
Markdown 입력
    ↓  heading 기준 분할 + 최대 500자 단위 청크
  청크 생성 (URL당 최대 20개)
    ↓  배치 32개 단위 임베딩
  벡터 생성 (all-MiniLM-L6-v2 → 384차원)
    ↓  UPSERT (url + chunk_index 기준 중복 방지)
  page_chunks 테이블 저장
```

---

## Search API (HTTP)

임베딩된 문서를 HTTP로 검색할 수 있는 REST API 서버. Swagger UI가 자동 생성됩니다.

### 실행

```bash
# 직접 실행 (기본 포트 8600)
python desktop/run_api.py

# 포트 지정
python desktop/run_api.py --port 8601

# Docker Compose로 실행 (PostgreSQL과 함께)
docker compose -f desktop/docker-compose.yml up search-api

# 설치 (별도 환경)
pip install -e ".[desktop,api]"
```

### Endpoints

| Method | Path | 설명 |
|--------|------|------|
| `GET` | `/search?q=...` | 자연어 벡터 검색 |
| `GET` | `/stats` | 임베딩 현황 통계 |
| `GET` | `/chunk?url=...` | 특정 URL의 전체 청크 조회 |
| `GET` | `/health` | DB + 모델 상태 확인 |
| `GET` | `/docs` | Swagger UI |

### 사용 예시

```bash
# 자연어 검색 (기본 top_k=5)
curl "http://localhost:8600/search?q=크롤러 성능 최적화"

# 도메인 필터 + 결과 수 조정
curl "http://localhost:8600/search?q=machine+learning&domain=arxiv.org&top_k=10"

# 최소 유사도 설정 (0.0~1.0, 기본 0.3)
curl "http://localhost:8600/search?q=vector+database&min_similarity=0.5"

# 임베딩 통계
curl "http://localhost:8600/stats"

# 특정 URL의 원문 청크 조회
curl "http://localhost:8600/chunk?url=https://example.com/article"

# 헬스 체크
curl "http://localhost:8600/health"
```

### Search 응답 예시

```json
{
  "query": "크롤러 성능 최적화",
  "top_k": 5,
  "total": 3,
  "elapsed_ms": 42.7,
  "results": [
    {
      "url": "https://example.com/crawling-tips",
      "title": "웹 크롤러 최적화 가이드",
      "domain": "example.com",
      "chunk_text": "비동기 HTTP 클라이언트를 활용하면...",
      "heading_ctx": "## 동시 요청 최적화",
      "chunk_index": 2,
      "similarity": 0.8412,
      "language": "ko",
      "crawled_at": "2025-01-15 10:23:41"
    }
  ]
}
```

---

## Testing

```bash
# 전체 테스트 실행
python -m pytest tests/ -v

# 특정 레이어 테스트
python -m pytest tests/test_ingestor.py -v
python -m pytest tests/test_router.py -v
python -m pytest tests/test_processor.py -v
python -m pytest tests/test_storage.py -v
```

---

## Shared Modules (`src/common/`)

모든 레이어에서 공유하는 모듈.

- **`compression.py`**: Zstd 압축/해제 (`compress()`, `decompress_to_str()`)
- **`kafka_config.py`**: 파이프라인 전체 설정 (Kafka, MinIO, PostgreSQL, 토픽명 등)
- **`url_extractor.py`**: URL 정규화, 트래킹 파라미터 제거, 유효성 필터링

```python
# 권장 import 방식
from src.common.compression import compress, decompress_to_str
from src.common.kafka_config import get_config
from src.common.url_extractor import URLExtractor
```

> 기존 경로(`config/kafka_config.py`, `src/ingestor/compression.py`)는 하위호환 shim으로 유지됩니다.

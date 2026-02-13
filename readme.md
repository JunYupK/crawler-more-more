<a id="readme-top"></a>

<!-- PROJECT SHIELDS -->
<div align="center">

[![Python](https://img.shields.io/badge/Python-3.10-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://python.org)
[![Kafka](https://img.shields.io/badge/Kafka-Stream-231F20?style=for-the-badge&logo=apachekafka&logoColor=white)](https://kafka.apache.org)
[![Redis](https://img.shields.io/badge/Redis-7.0-DC382D?style=for-the-badge&logo=redis&logoColor=white)](https://redis.io)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-pgvector-4169E1?style=for-the-badge&logo=postgresql&logoColor=white)](https://postgresql.org)
[![MinIO](https://img.shields.io/badge/MinIO-Object_Storage-C72E49?style=for-the-badge&logo=minio&logoColor=white)](https://min.io)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=for-the-badge&logo=docker&logoColor=white)](https://docker.com)

</div>

<!-- PROJECT TITLE -->
<div align="center">
  <h1>🕷️ Distributed Web Crawling Pipeline</h1>
  <p>
    <strong>M2 MacBook 8GB × Linux Desktop으로 Tranco Top 1M 크롤링하기</strong>
  </p>
  <p>
    수집(Ingest) · 처리(Process) · 저장(Store) · 벡터화(Embed) 의 4단계 분산 파이프라인
  </p>

  <a href="./crawler-challenge/docs/10k_crawling_report.md">📊 성능 보고서</a>
  ·
  <a href="./crawler-challenge/docs/TEST_REPORT.md">🧪 테스트 보고서</a>
  ·
  <a href="#quick-start">🚀 Quick Start</a>
</div>

---

<!-- TABLE OF CONTENTS -->
<details>
  <summary>📑 Table of Contents</summary>
  <ol>
    <li><a href="#features">Features</a></li>
    <li><a href="#architecture">Architecture</a></li>
    <li><a href="#quick-start">Quick Start</a></li>
    <li><a href="#kafka-pipeline">Kafka Stream Pipeline</a></li>
    <li><a href="#sharded-crawler">Sharded Crawler</a></li>
    <li><a href="#embedding--rag">Embedding & RAG</a></li>
    <li><a href="#monitoring">Monitoring</a></li>
    <li><a href="#cicd">CI/CD</a></li>
    <li><a href="#project-structure">Project Structure</a></li>
    <li><a href="#troubleshooting">Troubleshooting</a></li>
  </ol>
</details>

---

## Features

⚡ **이중 크롤링 전략** — Mac 고속 인제스터(500 동시) + Docker Sharded Crawler 병행 운용

🔀 **Kafka 스트림 파이프라인** — raw.page → Router → Processor → Storage 4단계 처리

🧠 **지능형 라우팅** — 페이지 분석 점수 기반 BeautifulSoup / Crawl4AI 자동 선택

🔁 **URL 자동 재공급** — 크롤링 중 발견된 URL을 Redis 큐에 자동 적재 (피드백 루프)

🗄️ **하이브리드 저장** — MinIO(Markdown 원문) + PostgreSQL(메타데이터) 이중 저장

🔍 **벡터 검색(RAG)** — pgvector 기반 임베딩 저장 및 코사인 유사도 검색

📈 **실시간 모니터링** — Kafka UI · MinIO Console · Prometheus · Grafana

🛡️ **DLQ 시스템** — 각 레이어별 Dead Letter Queue로 에러 데이터 격리

🔄 **CI/CD 파이프라인** — GitHub Actions 4개 워크플로우

<p align="right">(<a href="#readme-top">back to top</a>)</p>

---

## Architecture

### 전체 시스템 구성

두 머신이 Tailscale VPN으로 연결되어 역할을 분담합니다.

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                           Tailscale Mesh VPN                                 │
└──────────────────────────────────────────────────────────────────────────────┘
          │                                         │
          ▼                                         ▼
┌─────────────────────┐               ┌─────────────────────────────────────────┐
│   Mac (M2, 8GB)     │               │   Desktop (Linux)                       │
│                     │               │                                         │
│  ┌───────────────┐  │               │  ┌─────────────────────────────────┐    │
│  │   Ingestor    │  │               │  │     Kafka Stream Pipeline       │    │
│  │  HTTP 크롤링   │──┼──Kafka───────►│  │                                 │    │
│  │  Zstd 압축    │  │  raw.page     │  │  Router → Fast/Rich Processor   │    │
│  │  500 동시요청  │  │               │  │       → Storage (MinIO+PG)      │    │
│  └───────────────┘  │               │  │       → Embedding (pgvector)    │    │
│                     │               │  │       → URL Queue → Redis ◄─┐  │    │
│  ┌───────────────┐  │               │  └─────────────────────────────┼──┘    │
│  │Sharded Crawler│  │               │                                │        │
│  │ Master+Worker │  │               │  ┌─────────────────────────────┘        │
│  │ Redis 3-Shard │  │               │  │  Docker Infra                        │
│  │ → PostgreSQL  │  │               │  │  Kafka · MinIO · PostgreSQL+pgvector │
│  └───────────────┘  │               │  │  Redis · Prometheus · Grafana        │
└─────────────────────┘               │  └──────────────────────────────────────┘
                                      └─────────────────────────────────────────┘
```

### Kafka 토픽 흐름

```
Mac Ingestor
    │
    ▼  raw.page (HTML + 메타데이터, Zstd 압축)
  Router ──────── 점수 < 80 ──────────────────────► process.rich
    │                                                     │
    └─── 점수 ≥ 80 ──────────► process.fast              │
                                    │                     │
                              FastProcessor         RichProcessor
                              (BeautifulSoup)       (Crawl4AI)
                                    │                     │
                                    └──────┬──────────────┘
                                           ▼
                                    processed.final (Markdown + 메타)
                                           │
                          ┌────────────────┼────────────────┐
                          ▼                ▼                ▼
                       Storage         Embedding        URL Queue
                    (MinIO + PG)     (pgvector RAG)  (discovered.urls
                                                       → Redis 큐)
```

<p align="right">(<a href="#readme-top">back to top</a>)</p>

---

## Quick Start

### 사전 준비

- Python 3.10+
- Docker & Docker Compose
- (Mac용) Tailscale 설치 후 Desktop IP 확인

### 1. Desktop — 인프라 시작

```bash
cd crawler-challenge

# Kafka, MinIO, PostgreSQL+pgvector, Redis, 모니터링 스택 시작
docker compose -f docker/docker-compose.stream.yml up -d
```

### 2. Desktop — 스트림 파이프라인 실행

```bash
# Python 의존성 설치
pip install -e ".[desktop]"

# 필수 4개 서비스 한번에 시작 (Router + FastProcessor + RichProcessor + Storage)
./desktop/start.sh

# 선택 옵션
./desktop/start.sh --with-url-queue    # + 발견 URL 자동 Redis 재공급
./desktop/start.sh --with-embedding    # + pgvector 임베딩 (모델 ~2GB)
./desktop/start.sh --all               # 전체 실행

# 관리
./desktop/start.sh status              # 실행 상태 확인
./desktop/start.sh logs router         # 특정 서비스 로그 확인
./desktop/start.sh stop                # 전체 중지
```

### 3. Mac — 인제스터 실행

```bash
pip install -e ".[mac]"

# Desktop Kafka IP를 지정해서 실행
./mac/start.sh --kafka-servers <Desktop-IP>:9092

# 테스트 모드 (100개)
./mac/start.sh --test
```

### 4. (선택) Docker Sharded Crawler 실행

```bash
# Master + Worker 8개 시작
python runners/sharded_master.py --count 1000000 --workers 8
```

<p align="right">(<a href="#readme-top">back to top</a>)</p>

---

## Kafka Pipeline

### 서비스별 역할

| 서비스 | 파일 | 입력 | 출력 | 실행 위치 |
|--------|------|------|------|-----------|
| **Ingestor** | `mac/run.py` | Tranco 리스트 | `raw.page` | Mac |
| **Router** | `desktop/run_router.py` | `raw.page` | `process.fast` / `process.rich` | Desktop |
| **Fast Processor** | `desktop/run_fast_processor.py` | `process.fast` | `processed.final` | Desktop |
| **Rich Processor** | `desktop/run_rich_processor.py` | `process.rich` | `processed.final` | Desktop |
| **Storage** | `desktop/run_storage.py` | `processed.final` | MinIO + PostgreSQL | Desktop |
| **URL Queue** | `desktop/run_url_queue.py` | `discovered.urls` | Redis 크롤러 큐 | Desktop |
| **Embedding** | `desktop/run_embedding.py` | `processed.final` | pgvector | Desktop |

### Kafka 토픽

| 토픽 | 생산자 | 소비자 | 설명 |
|------|--------|--------|------|
| `raw.page` | Ingestor | Router | HTML 원문 (Zstd 압축) |
| `process.fast` | Router | FastProcessor | 정적 페이지 |
| `process.rich` | Router | RichProcessor | 동적 페이지 (JS 렌더링) |
| `processed.final` | Fast/RichProcessor | Storage, Embedding | 처리 완료 (Markdown) |
| `discovered.urls` | Fast/RichProcessor | URL Queue Consumer | 크롤링 중 발견된 URL |
| `*.dlq` | 각 레이어 | — | Dead Letter Queue |

<p align="right">(<a href="#readme-top">back to top</a>)</p>

---

## Sharded Crawler

독립 운용 가능한 Redis 기반 분산 크롤러. Kafka 파이프라인과 별개로 동작합니다.

### Master-Worker 구조

```
                   ┌─────────────────────┐
                   │    Sharded Master   │
                   │    :8000 (metrics)  │
                   └──────────┬──────────┘
                              │  URL 분배
          ┌───────────────────┼───────────────────┐
          ▼                   ▼                   ▼
   ┌──────────┐        ┌──────────┐        ┌──────────┐
   │ Shard 0  │        │ Shard 1  │        │ Shard 2  │
   │ (DB 1)   │        │ (DB 2)   │        │ (DB 3)   │
   └──────────┘        └──────────┘        └──────────┘
          └───────────────────┼───────────────────┘
                              │
                ┌─────────────▼───────────────┐
                │      Workers (1 ~ N개)       │
                │   모든 워커가 모든 샤드 접근  │
                │   --workers 옵션으로 조절     │
                └─────────────────────────────┘
                              │
                        PostgreSQL
```

### Redis 큐 구조

```
┌───────────────────────────────────────────────────────┐
│  각 샤드 내부 (Sorted Set — 우선순위 큐)               │
│  ├─ priority_high    : Top 100  사이트  (score 900+)  │
│  ├─ priority_medium  : Top 1K   사이트  (score 800+)  │
│  ├─ priority_normal  : Top 10K  사이트  (score 700+)  │
│  └─ priority_low     : 나머지 + 발견 URL (score 700-) │
│                                                       │
│  State (Set)                                         │
│  ├─ completed  : 완료 (URL 해시)                     │
│  ├─ processing : 처리 중                              │
│  ├─ failed     : 실패 + 에러 정보                    │
│  └─ retry      : 재시도 대기                         │
└───────────────────────────────────────────────────────┘
```

<p align="right">(<a href="#readme-top">back to top</a>)</p>

---

## Embedding & RAG

`processed.final` 토픽의 Markdown을 청크 단위로 분할하여 벡터 임베딩 후 pgvector에 저장합니다.

### 파이프라인

```
processed.final (Markdown)
    │
    ▼ Chunker (최대 500자 / URL당 20청크)
  청크 분할
    │
    ▼ Embedder (배치 32개)
  벡터 생성 (384차원 / 768차원)
    │
    ▼ pgvector UPSERT
  page_chunks 테이블 저장
```

### 사용법

```bash
# 임베딩 워커 실행
python desktop/run_embedding.py

# 벡터 검색 (RAG)
python desktop/run_embedding.py --search "검색할 내용"

# 백엔드 선택
EMBED_BACKEND=local python desktop/run_embedding.py        # 로컬 모델 (기본)
EMBED_BACKEND=openai python desktop/run_embedding.py       # OpenAI API

# 모델 변경 (로컬)
EMBED_MODEL_NAME=all-mpnet-base-v2 python desktop/run_embedding.py
```

| 환경변수 | 기본값 | 설명 |
|----------|--------|------|
| `EMBED_BACKEND` | `local` | `local` 또는 `openai` |
| `EMBED_MODEL_NAME` | `all-MiniLM-L6-v2` | 로컬 모델명 (384차원) |
| `OPENAI_API_KEY` | — | OpenAI 임베딩 사용 시 필수 |

> 운영 권장: 비용/요금 이슈가 있다면 `EMBED_BACKEND=local`만 사용하세요.
>
> 주의: `page_chunks.embedding` 컬럼은 기본 마이그레이션에서 `vector(384)`입니다.
> 다른 차원 모델(예: 768/1536)로 변경하면 차원 불일치가 발생하며,
> 워커/검색기 시작 시점에 안전하게 실패하도록 검증됩니다.

<p align="right">(<a href="#readme-top">back to top</a>)</p>

---

## Monitoring

인프라 시작 후 접속 가능한 웹 UI:

| 서비스 | URL | 설명 |
|--------|-----|------|
| **Kafka UI** | http://localhost:8080 | 토픽 / 컨슈머 / 메시지 모니터링 |
| **MinIO Console** | http://localhost:9001 | Object Storage 관리 |
| **Prometheus** | http://localhost:9090 | 메트릭 수집 |
| **Grafana** | http://localhost:3000 | 크롤러 대시보드 |

### Grafana 대시보드

```
┌─────────────┬─────────────┬─────────────┬─────────────────┐
│ Running     │ Total CPU   │ Total       │ Redis Queue     │
│ Workers     │ Usage       │ Memory      │ Length          │
├─────────────┴─────────────┴─────────────┴─────────────────┤
│ Queue by Priority              │ Processing Rate          │
├────────────────────────────────┴──────────────────────────┤
│ CPU per Worker    │ Memory per Worker │ Error Rate        │
└───────────────────┴───────────────────┴───────────────────┘
```

<p align="right">(<a href="#readme-top">back to top</a>)</p>

---

## CI/CD

```
Code Push ──► CI Test ──► Docker Build ──► Registry
                │              │
                ▼              ▼
            Lint/Test      ghcr.io
            Security       Trivy Scan
```

| Workflow | 트리거 | 동작 |
|----------|--------|------|
| `ci.yml` | 모든 push/PR | Flake8, Radon, Bandit |
| `docker-build.yml` | main 머지, 태그 | Multi-stage 빌드, ghcr.io 푸시 |
| `pr-automation.yml` | PR 생성 | Conventional Commits 검증, 라벨링 |
| `release.yml` | 버전 태그 | GitHub Release, Changelog |

<p align="right">(<a href="#readme-top">back to top</a>)</p>

---

## Project Structure

```
crawler-challenge/
├── src/
│   ├── common/              # 공유 모듈
│   │   ├── compression.py   # Zstd 압축/해제
│   │   ├── kafka_config.py  # 파이프라인 전체 설정
│   │   └── url_extractor.py # URL 정규화 + 필터링
│   ├── core/                # Sharded Crawler 핵심
│   │   ├── polite_crawler.py
│   │   └── database.py
│   ├── ingestor/            # Layer 1: HTTP 크롤링
│   ├── router/              # Layer 2: 콘텐츠 분석 + 라우팅
│   ├── processor/           # Layer 3: HTML → Markdown
│   ├── storage/             # Layer 4: MinIO + PostgreSQL
│   ├── embedding/           # Layer 5: 벡터 임베딩 + RAG 검색
│   │   ├── chunker.py       # Markdown 청크 분할
│   │   ├── embedder.py      # 임베딩 모델 추상화
│   │   ├── embedding_worker.py  # Kafka Consumer → pgvector
│   │   └── rag_search.py    # 벡터 유사도 검색
│   ├── managers/            # 큐 관리
│   │   ├── sharded_queue_manager.py  # Redis 3-Shard
│   │   ├── url_queue_consumer.py     # discovered.urls → Redis
│   │   └── tranco_manager.py
│   └── monitoring/
│       └── metrics.py       # Prometheus 메트릭
│
├── runners/                 # 실행 진입점 (전체 옵션)
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
├── mac/                     # Mac 전용
│   ├── run.py               # 인제스터 진입점
│   ├── start.sh             # 실행 스크립트 (연결 확인 포함)
│   └── requirements.txt
│
├── desktop/                 # Desktop 전용
│   ├── start.sh             # 파이프라인 일괄 실행/중지/상태
│   ├── run_router.py
│   ├── run_fast_processor.py
│   ├── run_rich_processor.py
│   ├── run_storage.py
│   ├── run_url_queue.py
│   ├── run_embedding.py
│   └── requirements.txt
│
├── docker/
│   ├── docker-compose.stream.yml  # 전체 인프라
│   ├── init-stream.sql            # 스트림 파이프라인 스키마
│   ├── init.sql                   # Sharded Crawler 스키마
│   ├── migrations/
│   │   ├── 001_add_crawl_results_table.sql
│   │   └── 002_add_pgvector.sql   # pgvector 확장 + page_chunks
│   └── prometheus-stream.yml
│
├── tests/
├── docs/
└── pyproject.toml
```

<p align="right">(<a href="#readme-top">back to top</a>)</p>

---

## Troubleshooting

| 문제 | 원인 | 해결 |
|------|------|------|
| **8GB 메모리 부족** | 크롤러 + 모니터링 리소스 경쟁 | 머신 분리 (Mac/Desktop) |
| **네트워크 분리** | 서로 다른 네트워크 | Tailscale VPN 도입 |
| **Hot Shard** | 도메인 해시 기반 부하 집중 | 랜덤 샤딩으로 전략 변경 |
| **성공률 0%** | robots.txt 로직 오류 | 경로별 개별 판단으로 수정 |
| **무한 롤백** | NUL 바이트 데이터 | DLQ 시스템으로 에러 격리 |
| **pgvector 저장 실패** | asyncpg 타입 불일치 | `[f1,f2,...]` 명시적 포맷 변환 |
| **asyncio 경고** | `get_event_loop()` deprecated | `get_running_loop()` 교체 |
| **stats race condition** | executor 스레드에서 직접 수정 | 결과 코드 반환 후 async에서 업데이트 |

<p align="right">(<a href="#readme-top">back to top</a>)</p>

---

## Built With

| 영역 | 기술 |
|------|------|
| Language | Python 3.10 (asyncio) |
| Message Queue | Apache Kafka (aiokafka) |
| Cache / Queue | Redis 7 (3-shard) |
| Database | PostgreSQL 15 + pgvector |
| Object Storage | MinIO |
| HTTP | httpx (비동기) |
| HTML 파싱 | BeautifulSoup4 |
| JS 렌더링 | Crawl4AI |
| Embedding | sentence-transformers / OpenAI API |
| Container | Docker, Docker Compose |
| Monitoring | Prometheus, Grafana, Kafka UI |
| Network | Tailscale (Mesh VPN) |
| CI/CD | GitHub Actions |

<p align="right">(<a href="#readme-top">back to top</a>)</p>

---

<div align="center">
  <sub>비싼 장비 없이도 꽤 많은 걸 할 수 있다는 걸 보여주고 싶었습니다.</sub>
  <br>
  <sub>제한된 환경에서 병목을 찾고 해결하는 과정이 오히려 더 많은 걸 배우게 해준 것 같습니다.</sub>
</div>

# Throughput-First Stream Pipeline 구현 계획서

> **핵심 철학:** DB를 거치지 않고 Kafka 스트림을 통해 데이터를 흐르게 하여 처리 속도를 극대화한다.

## 1. 프로젝트 배경

### 1.1 현재 상태 (AS-IS)
- **HTTP Client:** aiohttp (비동기)
- **Queue System:** Redis Sorted Set (3-샤드)
- **Processing:** BeautifulSoup만 사용
- **Storage:** PostgreSQL (직접 저장)
- **문제점:** DB I/O 병목, 동적 페이지 처리 불가

### 1.2 목표 상태 (TO-BE)
- **HTTP Client:** httpx (HTTP/2 지원)
- **Queue System:** Kafka Topics (스트림 처리)
- **Routing:** Smart Router (정적/동적 분류)
- **Processing:** BeautifulSoup + Crawl4AI
- **Storage:** PostgreSQL (메타) + MinIO (콘텐츠)

---

## 2. 아키텍처 설계

```
┌─────────────────────────────────────────────────────────────────────┐
│                    Layer 1: High-Speed Ingestor (Mac)               │
│  ┌─────────────┐    ┌──────────────┐    ┌─────────────────────┐    │
│  │   Tranco    │ -> │    httpx     │ -> │   Kafka Producer    │    │
│  │  URL List   │    │ Async Crawler│    │   (raw.page topic)  │    │
│  └─────────────┘    └──────────────┘    └─────────────────────┘    │
│                            │ Zstd 압축                              │
└────────────────────────────│────────────────────────────────────────┘
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    Layer 2: Intelligent Router (Desktop)            │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │                     Smart Router                             │   │
│  │  ┌──────────────┐                    ┌──────────────┐       │   │
│  │  │ Page Analyzer │ --> Score >= 80 --> │ process.fast │       │   │
│  │  │ (정적/동적)   │                    └──────────────┘       │   │
│  │  │              │                    ┌──────────────┐       │   │
│  │  │              │ --> Score < 80 --> │ process.rich │       │   │
│  │  └──────────────┘                    └──────────────┘       │   │
│  └─────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────┘
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    Layer 3: Parallel Processing (Desktop)           │
│  ┌───────────────────────┐    ┌───────────────────────┐            │
│  │     Fast Workers      │    │     Rich Workers      │            │
│  │   (BeautifulSoup)     │    │     (Crawl4AI)        │            │
│  │                       │    │                       │            │
│  │  - HTML Parsing       │    │  - Browser Rendering  │            │
│  │  - html2text 변환     │    │  - JS 실행            │            │
│  │  - 메타데이터 추출    │    │  - Markdown 추출      │            │
│  │                       │    │                       │            │
│  │  예상 처리량: 70%     │    │  예상 처리량: 30%     │            │
│  └───────────────────────┘    └───────────────────────┘            │
│               │                           │                         │
│               └───────────┬───────────────┘                         │
│                           ▼                                         │
│               ┌─────────────────────┐                               │
│               │  processed.final    │                               │
│               │      topic          │                               │
│               └─────────────────────┘                               │
└─────────────────────────────────────────────────────────────────────┘
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    Layer 4: Hybrid Storage (Desktop)                │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │                    Storage Writer                            │   │
│  │  ┌──────────────────┐      ┌──────────────────┐             │   │
│  │  │   PostgreSQL     │      │      MinIO       │             │   │
│  │  │   (Metadata)     │      │    (Content)     │             │   │
│  │  │                  │      │                  │             │   │
│  │  │  - URL, Domain   │      │  - Raw HTML      │             │   │
│  │  │  - Title, Desc   │      │  - Markdown      │             │   │
│  │  │  - Score, Stats  │      │  - 압축 저장     │             │   │
│  │  │  - MinIO 참조키  │      │                  │             │   │
│  │  └──────────────────┘      └──────────────────┘             │   │
│  └─────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 3. Kafka Topics 설계

| Topic | Partitions | Retention | 용도 |
|-------|------------|-----------|------|
| `raw.page` | 12 | 6h | 크롤링된 HTML (Zstd 압축) |
| `raw.page.dlq` | 3 | 24h | 크롤링 실패 |
| `process.fast` | 8 | 2h | 정적 페이지 라우팅 |
| `process.rich` | 4 | 2h | 동적 페이지 라우팅 |
| `processed.final` | 12 | 4h | 처리 완료 데이터 |
| `processed.dlq` | 3 | 24h | 처리 실패 |
| `storage.saved` | 6 | 1h | 저장 완료 이벤트 |
| `storage.dlq` | 3 | 24h | 저장 실패 |

---

## 4. 구현 단계

### Stage 1: 인프라 구축 ✅
- [x] Docker Compose 설정 (Kafka, Zookeeper, MinIO)
- [x] PostgreSQL 스키마 설계
- [x] Kafka 토픽 생성 스크립트
- [x] 설정 파일 구조 (kafka_config.py)
- [x] 프로젝트 디렉토리 구조

### Stage 2: Layer 1 - Ingestor (Mac)
- [ ] httpx 비동기 크롤러 구현
- [ ] Zstd 압축 모듈 구현
- [ ] Kafka Producer 구현
- [ ] Tranco URL 로더 연동
- [ ] 에러 핸들링 (DLQ 전송)

### Stage 3: Layer 2 - Router
- [ ] Page Analyzer 구현 (정적/동적 판별)
- [ ] 점수 계산 로직 구현
- [ ] Smart Router 구현 (Kafka Consumer/Producer)
- [ ] 라우팅 로직 단위 테스트

### Stage 4: Layer 3 - Processors
- [ ] Base Worker 인터페이스 정의
- [ ] Fast Worker (BeautifulSoup) 구현
- [ ] Rich Worker (Crawl4AI) 구현
- [ ] Markdown 변환 로직
- [ ] 처리 결과 Kafka 전송

### Stage 5: Layer 4 - Storage
- [ ] MinIO Writer 구현
- [ ] PostgreSQL Writer 구현
- [ ] Hybrid Storage 통합
- [ ] 배치 저장 최적화

### Stage 6: 통합 및 최적화
- [ ] End-to-End 테스트
- [ ] 성능 튜닝
- [ ] 모니터링 대시보드 (Grafana)
- [ ] 문서화

---

## 5. 파일 구조

```
crawler-challenge/
├── src/
│   ├── core/                    # 기존 (레거시)
│   │   ├── polite_crawler.py
│   │   └── database.py
│   ├── ingestor/                # Layer 1
│   │   ├── __init__.py
│   │   ├── httpx_crawler.py
│   │   ├── kafka_producer.py
│   │   └── compression.py
│   ├── router/                  # Layer 2
│   │   ├── __init__.py
│   │   ├── smart_router.py
│   │   ├── page_analyzer.py
│   │   └── scoring.py
│   ├── processor/               # Layer 3
│   │   ├── __init__.py
│   │   ├── base_worker.py
│   │   ├── fast_worker.py
│   │   └── rich_worker.py
│   ├── storage/                 # Layer 4
│   │   ├── __init__.py
│   │   ├── postgres_writer.py
│   │   ├── minio_writer.py
│   │   └── hybrid_storage.py
│   └── managers/                # 기존
├── runners/
│   ├── ingestor_runner.py       # Mac
│   ├── router_runner.py         # Desktop
│   ├── fast_processor_runner.py # Desktop
│   ├── rich_processor_runner.py # Desktop
│   └── storage_runner.py        # Desktop
├── config/
│   ├── settings.py              # 기존
│   └── kafka_config.py          # 신규
├── docker/
│   ├── docker-compose.yml       # 기존
│   ├── docker-compose.stream.yml # 신규
│   ├── init-stream.sql          # 신규
│   ├── prometheus-stream.yml    # 신규
│   └── scripts/
│       └── create-topics.sh     # 신규
└── tests/
```

---

## 6. 배포 전략

### 6.1 Mac (Ingestor)
```bash
# 환경 변수 설정
export KAFKA_BOOTSTRAP_SERVERS=desktop-ip:9092
export INGESTOR_MAX_CONCURRENT=500

# 실행
python runners/ingestor_runner.py
```

### 6.2 Desktop (Router + Processors + Storage)
```bash
# 인프라 실행
docker compose -f docker/docker-compose.stream.yml up -d

# 토픽 생성
docker exec kafka bash /scripts/create-topics.sh

# 워커 실행 (별도 터미널)
python runners/router_runner.py
python runners/fast_processor_runner.py
python runners/rich_processor_runner.py
python runners/storage_runner.py
```

---

## 7. 정적/동적 페이지 판별 기준

### 7.1 동적 페이지 지표 (감점)
| 패턴 | 감점 | 설명 |
|------|------|------|
| `__NEXT_DATA__` | -30 | Next.js (SSR 가능하나 확인 필요) |
| `window.__NUXT__` | -20 | Nuxt.js |
| `ng-app`, `ng-controller` | -40 | Angular |
| `data-reactroot` | -15 | React (SSR 가능) |
| `<script.*bundle` | -25 | 번들 JS |
| `<noscript>` | -10 | JS 의존성 힌트 |
| `loading`, `spinner` | -20 | 로딩 UI |

### 7.2 정적 페이지 지표 (가점)
| 패턴 | 가점 | 설명 |
|------|------|------|
| `<article>` | +20 | 글 구조 |
| `<main.*content` | +15 | 메인 콘텐츠 영역 |
| 텍스트 > 3000자 | +10 | 풍부한 콘텐츠 |
| 텍스트 < 500자 | -30 | 콘텐츠 부족 |

### 7.3 라우팅 결정
- **Score >= 80:** `process.fast` (BeautifulSoup)
- **Score < 80:** `process.rich` (Crawl4AI)

---

## 8. 의존성 패키지

```txt
# requirements-stream.txt

# HTTP Client
httpx[http2]>=0.25.0

# Message Queue
aiokafka>=0.10.0
confluent-kafka>=2.3.0

# Compression
zstandard>=0.22.0

# Object Storage
minio>=7.2.0

# HTML Processing
beautifulsoup4>=4.12.0
lxml>=5.0.0
html2text>=2024.2.26

# Browser Automation (Rich Worker)
crawl4ai>=0.3.0  # Crawl4AI

# Database
asyncpg>=0.29.0
psycopg[binary,pool]>=3.1.0

# Serialization
msgpack>=1.0.7

# Monitoring
prometheus-client>=0.20.0
```

---

## 9. Crawl4AI 설치 가이드

### 9.1 기본 설치
```bash
pip install crawl4ai
crawl4ai-setup  # Playwright 브라우저 설치
```

### 9.2 Docker 환경
```dockerfile
FROM mcr.microsoft.com/playwright/python:v1.40.0-jammy

RUN pip install crawl4ai
RUN crawl4ai-setup
```

### 9.3 사용 예시
```python
from crawl4ai import AsyncWebCrawler

async def crawl_dynamic_page(url: str) -> str:
    async with AsyncWebCrawler(headless=True) as crawler:
        result = await crawler.arun(url=url)
        return result.markdown
```

---

## 10. 예상 처리량

| 구성요소 | 예상 처리량 | 병목 요소 |
|----------|-------------|-----------|
| Ingestor (Mac) | 500-1000 req/s | 네트워크, CPU |
| Router | 2000+ msg/s | CPU (가벼운 분석) |
| Fast Worker | 200+ pages/s | CPU |
| Rich Worker | 5-10 pages/s | 브라우저 메모리 |
| Storage | 1000+ records/s | 디스크 I/O |

### 10.1 예상 분포
- **정적 페이지 (Fast):** 70%
- **동적 페이지 (Rich):** 30%

---

## 11. 모니터링 메트릭

### 11.1 Kafka 메트릭
- `kafka_consumer_lag`: Consumer 지연
- `kafka_producer_record_send_rate`: 전송률
- `kafka_consumer_records_consumed_rate`: 소비율

### 11.2 Application 메트릭
- `ingestor_requests_total`: 크롤링 요청 수
- `router_messages_routed_total`: 라우팅 메시지 수
- `processor_pages_processed_total`: 처리 완료 페이지
- `storage_records_saved_total`: 저장 완료 레코드

### 11.3 System 메트릭
- `system_cpu_usage`: CPU 사용률
- `system_memory_usage`: 메모리 사용률
- `system_disk_io`: 디스크 I/O

---

## 12. 핵심 Trade-off 결정

| 결정 | 선택 | 이유 |
|------|------|------|
| HTTP Client | httpx (not aiohttp) | HTTP/2 지원, 더 나은 API |
| Message Queue | Kafka (not Redis) | 스트림 처리, 내구성, 재처리 |
| Serialization | msgpack (not JSON) | 더 작은 크기, 더 빠른 처리 |
| Compression | Zstd (not gzip) | 더 빠른 압축/해제, 더 좋은 비율 |
| Object Storage | MinIO (not S3 직접) | 로컬 개발 용이, S3 호환 |

---

## 13. 다음 단계

1. **Stage 2 시작:** `src/ingestor/` 모듈 구현
2. **테스트 환경:** 1000개 URL로 파이프라인 검증
3. **점진적 확장:** 10만 → 100만 URL 처리

---

*최종 수정: 2025-01-15*

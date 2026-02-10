# Crawler Stream Pipeline

고처리량 웹 크롤링 스트림 파이프라인. Mac(크롤링) + Desktop(처리/저장) 분산 아키텍처.

## Architecture

```
Mac (Layer 1)                          Desktop (Layer 2-4)
┌──────────────┐                       ┌──────────────────────────────────────┐
│   Ingestor   │                       │  Router → Processor → Storage       │
│  HTTP Crawl  │──── Kafka ──────────> │                                      │
│  + Zstd 압축  │   (raw.page)         │  ┌─ process.fast → FastProcessor    │
└──────────────┘                       │  │    (BeautifulSoup)                │
                                       │  └─ process.rich → RichProcessor    │
                                       │       (Crawl4AI)                     │
                                       │          ↓                           │
                                       │  processed.final → HybridStorage    │
                                       │    (MinIO + PostgreSQL)              │
                                       └──────────────────────────────────────┘
```

## Project Structure

```
crawler-challenge/
├── src/
│   ├── common/          # 공유 모듈 (compression, kafka_config)
│   ├── ingestor/        # Layer 1: HTTP 크롤링 + Kafka produce
│   ├── router/          # Layer 2: 콘텐츠 분석 + 라우팅
│   ├── processor/       # Layer 3: HTML → Markdown 변환
│   └── storage/         # Layer 4: MinIO + PostgreSQL 저장
├── runners/             # 전체 실행 스크립트 (entry points)
├── mac/                 # Mac 전용 간소화 실행기
├── desktop/             # Desktop 전용 간소화 실행기
├── config/              # 설정 (하위호환 shim)
├── docker/              # Docker, SQL, Prometheus 설정
├── tests/               # 테스트 (pytest)
└── pyproject.toml       # 패키지 설정
```

## Quick Start

### Prerequisites

- Python 3.10+
- Docker & Docker Compose (Desktop 인프라용)

### 1. 패키지 설치

```bash
cd crawler-challenge

# Mac (Ingestor만 실행)
pip install -e ".[mac]"

# Desktop (Router/Processor/Storage 실행)
pip install -e ".[desktop]"

# 개발 환경 (테스트 포함)
pip install -e ".[mac,desktop,dev]"
```

### 2. Desktop 인프라 시작

```bash
# Kafka + MinIO + PostgreSQL + 모니터링 시작
docker compose -f docker/docker-compose.stream.yml up -d
```

### 3. 파이프라인 실행

**Mac (Ingestor)**:
```bash
# 기본 실행 (Tranco Top 1M)
python mac/run.py

# 테스트 모드
python mac/run.py --test --limit 100

# Kafka 서버 지정 (데스크탑 IP)
python mac/run.py --kafka-servers 192.168.x.x:9092
```

**Desktop (Router + Processor + Storage)** - 각 터미널에서:
```bash
# Terminal 1: Router
python desktop/run_router.py

# Terminal 2: Fast Processor
python desktop/run_fast_processor.py --workers 4

# Terminal 3: Rich Processor (Crawl4AI)
python desktop/run_rich_processor.py --workers 2

# Terminal 4: Storage
python desktop/run_storage.py
```

또는 `runners/` 디렉토리의 전체 실행 스크립트 사용:
```bash
python runners/ingestor_runner.py
python runners/router_runner.py
python runners/fast_processor_runner.py
python runners/rich_processor_runner.py
python runners/storage_runner.py
```

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

> **보안 주의**: `MINIO_SECRET_KEY`, `POSTGRES_PASSWORD` 미설정 시 기본값이 사용되며 경고 로그가 출력됩니다. 운영 환경에서는 반드시 환경변수를 설정하세요.

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

### Docker Compose 전용

| 변수 | 기본값 | 설명 |
|------|--------|------|
| `KAFKA_EXTERNAL_HOST` | `localhost` | Kafka 외부 접근 호스트 |
| `KAFKA_UI_USER` | `admin` | Kafka UI 로그인 ID |
| `KAFKA_UI_PASSWORD` | `admin123` | Kafka UI 로그인 비밀번호 |
| `GRAFANA_PASSWORD` | `admin123` | Grafana 관리자 비밀번호 |

## Monitoring

인프라 시작 후 접속 가능한 웹 UI:

| 서비스 | URL | 설명 |
|--------|-----|------|
| Kafka UI | http://localhost:8080 | Kafka 토픽/컨슈머 모니터링 |
| MinIO Console | http://localhost:9001 | Object Storage 관리 |
| Prometheus | http://localhost:9090 | 메트릭 수집 |
| Grafana | http://localhost:3000 | 대시보드 |

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

## Shared Modules (`src/common/`)

리팩토링을 통해 모든 레이어에서 공유하는 코드를 `src/common/`으로 통합:

- **`compression.py`**: Zstd 압축/해제 유틸리티 (`compress()`, `decompress_to_str()`)
- **`kafka_config.py`**: 파이프라인 전체 설정 (Kafka, MinIO, PostgreSQL 등)

기존 경로(`config/kafka_config.py`, `src/ingestor/compression.py`)는 하위호환을 위해 shim으로 유지됩니다.

```python
# 권장 import 방식
from src.common.compression import compress, decompress_to_str
from src.common.kafka_config import get_config, reset_config
```

## Kafka Topics

| 토픽 | 설명 |
|------|------|
| `raw.page` | Layer 1 → 2: 크롤링된 원본 페이지 (Zstd 압축) |
| `process.fast` | Layer 2 → 3: 정적 페이지 (BeautifulSoup 처리) |
| `process.rich` | Layer 2 → 3: 동적 페이지 (Crawl4AI 처리) |
| `processed.final` | Layer 3 → 4: 처리 완료 결과 |
| `storage.saved` | 저장 완료 이벤트 |
| `*.dlq` | 각 레이어의 Dead Letter Queue |

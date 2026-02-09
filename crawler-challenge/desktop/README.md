# Desktop Processing Pipeline

데스크탑에서 Router/Processor/Storage 레이어(Layer 2-4)를 실행하기 위한 패키지.

## Setup

```bash
cd crawler-challenge

# 인프라 시작 (Kafka, MinIO, PostgreSQL)
docker compose -f docker/docker-compose.stream.yml up -d

# Python 의존성 설치
pip install -e ".[desktop]"
# 또는
pip install -r desktop/requirements.txt
```

## Usage

각 레이어를 별도 터미널에서 실행:

```bash
# Terminal 1: Router
python desktop/run_router.py

# Terminal 2: Fast Processor
python desktop/run_fast_processor.py --workers 4

# Terminal 3: Rich Processor
python desktop/run_rich_processor.py --workers 2

# Terminal 4: Storage
python desktop/run_storage.py
```

## Architecture

```
Kafka(raw.page)
  → Router (분석 + 라우팅)
    → process.fast → FastProcessor (BeautifulSoup)
    → process.rich → RichProcessor (Crawl4AI)
      → processed.final → Storage (MinIO + PostgreSQL)
```

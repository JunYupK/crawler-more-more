# Mac Crawler (Sharded)

맥북에서 크롤링 레이어(Layer 1)를 실행하기 위한 패키지.
Sharded 분산 아키텍처로 Redis 큐를 통해 Master-Worker 구조로 동작.

## Setup

```bash
cd crawler-challenge
pip install -e ".[mac]"
```

## Usage

### Makefile (권장)

```bash
# 테스트 크롤링 (100개 URL, 워커 1개)
make test-crawl

# URL 개수 및 워커 수 지정
make test-crawl CRAWL_LIMIT=500 WORKERS=2

# 전체 크롤링 (Tranco Top 1M, 워커 4개)
make test-crawl-full
```

### 직접 실행

```bash
# 1. Master: Redis 큐에 URL 로딩 + 진행 모니터링
python runners/sharded_master.py --count 100 --auto-start

# 2. Worker (별도 터미널): 실제 HTTP 크롤링 수행
python runners/sharded_worker.py --worker-id 1

# 다수 워커 실행 예시 (워커 4개)
for i in 1 2 3 4; do
    python runners/sharded_worker.py --worker-id $i &
done
```

## Architecture

```
Mac (Master)  — Redis 샤드 큐에 URL 배포
Mac (Worker)  — Redis 큐 소비 → HTTP 크롤링 → Kafka produce
                                                    │
Desktop (Router → Processor → Storage) ←───────────┘
```

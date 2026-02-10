# Mac Ingestor

맥북에서 크롤링 레이어(Layer 1)만 실행하기 위한 패키지.

## Setup

```bash
cd crawler-challenge
pip install -e ".[mac]"
# 또는
pip install -r mac/requirements.txt
```

## Usage

```bash
# 기본 실행 (Tranco Top 1M)
python mac/run.py

# 테스트 모드
python mac/run.py --test --limit 100

# Kafka 서버 지정 (데스크탑 IP)
python mac/run.py --kafka-servers 192.168.x.x:9092

# URL 파일 지정
python mac/run.py --url-file urls.txt
```

## Architecture

```
Mac (Ingestor) ──Kafka──> Desktop (Router → Processor → Storage)
```

Mac에서는 HTTP 크롤링 + Zstd 압축 + Kafka produce만 수행.
나머지 처리는 데스크탑에서 진행.

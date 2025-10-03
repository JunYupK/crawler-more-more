#!/bin/bash
set -e

# 환경 변수 기본값 설정
CRAWLER_MODE=${CRAWLER_MODE:-worker}
WORKER_ID=${WORKER_ID:-1}
REDIS_HOST=${REDIS_HOST:-redis}
POSTGRES_HOST=${POSTGRES_HOST:-postgres}
POSTGRES_DB=${POSTGRES_DB:-crawler_db}
POSTGRES_USER=${POSTGRES_USER:-postgres}
POSTGRES_PASSWORD=${POSTGRES_PASSWORD:-postgres}
BATCH_SIZE=${BATCH_SIZE:-25}
WORKER_THREADS=${WORKER_THREADS:-2}
WORKER_COUNT=${WORKER_COUNT:-4}

echo "Starting crawler in $CRAWLER_MODE mode..."

# Redis와 PostgreSQL 연결 대기
echo "Waiting for Redis at $REDIS_HOST:6379..."
while ! nc -z $REDIS_HOST 6379; do
  sleep 1
done

echo "Waiting for PostgreSQL at $POSTGRES_HOST:5432..."
while ! nc -z $POSTGRES_HOST 5432; do
  sleep 1
done

sleep 5  # 추가 대기 시간

if [ "$CRAWLER_MODE" = "master" ]; then
    echo "Starting master crawler..."
    python distributed_crawler.py \
        --mode master \
        --count 400 \
        --workers $WORKER_COUNT
elif [ "$CRAWLER_MODE" = "worker" ]; then
    echo "Starting worker $WORKER_ID..."
    python distributed_crawler.py \
        --mode worker \
        --worker-id $WORKER_ID \
        --batch-size $BATCH_SIZE \
        --threads $WORKER_THREADS
else
    echo "Unknown crawler mode: $CRAWLER_MODE"
    exit 1
fi
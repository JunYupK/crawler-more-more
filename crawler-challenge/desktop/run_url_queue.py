#!/usr/bin/env python3
"""
Desktop URL Queue Entry Point
==============================

discovered.urls Kafka 토픽을 소비하여 발견된 URL을 Redis 크롤러 큐에 적재합니다.

Usage:
    python desktop/run_url_queue.py
    python desktop/run_url_queue.py --test-connection
    python desktop/run_url_queue.py --stats

환경변수:
    KAFKA_BOOTSTRAP_SERVERS   Kafka 브로커 주소 (기본: localhost:9092)
    URL_QUEUE_TOTAL_LIMIT     총 pending URL 상한 (기본: 5,000,000)
    URL_QUEUE_DOMAIN_LIMIT    도메인별 최대 추가 수 (기본: 100)
    REDIS_HOST                Redis 호스트 (기본: localhost)
"""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from runners.url_queue_runner import main
import asyncio

if __name__ == '__main__':
    asyncio.run(main())

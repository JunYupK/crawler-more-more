#!/usr/bin/env python3
"""
URL Queue Runner - discovered.urls Kafka → Redis Queue 적재기 실행기
===================================================================

FastWorker/RichWorker가 페이지 처리 중 발견한 URL을
Redis 크롤러 큐에 자동으로 적재합니다.

Usage:
    # 기본 실행
    python runners/url_queue_runner.py

    # 환경변수로 안전장치 조정
    URL_QUEUE_TOTAL_LIMIT=1000000 \\
    URL_QUEUE_DOMAIN_LIMIT=50 \\
    python runners/url_queue_runner.py

    # 연결 테스트
    python runners/url_queue_runner.py --test-connection

    # 통계만 출력
    python runners/url_queue_runner.py --stats

환경변수:
    KAFKA_BOOTSTRAP_SERVERS   Kafka 브로커 주소 (기본: localhost:9092)
    URL_QUEUE_TOTAL_LIMIT     총 pending URL 상한 (기본: 5,000,000)
    URL_QUEUE_DOMAIN_LIMIT    도메인별 최대 추가 수 (기본: 100)
    URL_QUEUE_REDIS_HOST      URL 재적재 대상 Redis 호스트 (기본: REDIS_HOST 또는 localhost)
    URL_QUEUE_REDIS_PORT      URL 재적재 대상 Redis 포트 (기본: 6379)
    URL_QUEUE_REDIS_DB_START  샤드 시작 DB 번호 (기본: 1 → 1,2,3 사용)
    REDIS_HOST                Redis 호스트 (기본: localhost)
"""

import asyncio
import argparse
import os
import logging
import signal
import sys
import time
from pathlib import Path
from typing import Optional

# 프로젝트 루트를 path에 추가
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.managers.url_queue_consumer import URLQueueConsumer
from src.common.kafka_config import get_config

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
logger = logging.getLogger(__name__)


class URLQueueRunner:
    """URL Queue Consumer 실행기"""

    def __init__(
        self,
        kafka_servers: Optional[str] = None,
    ):
        self.kafka_servers = kafka_servers or get_config().kafka.bootstrap_servers
        self._consumer: Optional[URLQueueConsumer] = None
        self._running = False

        # 종료 시그널 핸들러 등록
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """종료 시그널 처리"""
        logger.info(f"Received signal {signum}, shutting down...")
        self._running = False
        if self._consumer:
            self._consumer._running = False

    async def run(self) -> None:
        """Consumer 실행"""
        self._running = True
        start_time = time.time()

        logger.info("=" * 60)
        logger.info("URL Queue Runner starting")
        logger.info(f"  Kafka: {self.kafka_servers}")
        logger.info(
            "  Redis target: "
            f"{os.getenv('URL_QUEUE_REDIS_HOST', os.getenv('REDIS_HOST', 'localhost'))}:"
            f"{os.getenv('URL_QUEUE_REDIS_PORT', '6379')}"
        )
        logger.info(f"  Total queue limit: {URLQueueConsumer.TOTAL_QUEUE_LIMIT:,}")
        logger.info(f"  Domain limit: {URLQueueConsumer.MAX_DOMAIN_URLS}")
        logger.info("=" * 60)

        try:
            self._consumer = URLQueueConsumer(
                bootstrap_servers=self.kafka_servers,
            )

            await self._consumer.start()
            await self._consumer.run()

        except Exception as e:
            logger.error(f"Error running URL queue consumer: {e}", exc_info=True)

        finally:
            if self._consumer:
                await self._consumer.stop()
                stats = self._consumer.get_stats()

                elapsed = time.time() - start_time
                logger.info("=" * 60)
                logger.info("URL Queue Runner stopped")
                logger.info(f"  Total runtime: {elapsed:.1f}s")
                logger.info(f"  Messages consumed: {stats['messages_consumed']:,}")
                logger.info(f"  URLs received: {stats['urls_received']:,}")
                logger.info(f"  URLs added to queue: {stats['urls_added']:,}")
                logger.info(f"  Skipped (dedup): {stats['urls_skipped_dedup']:,}")
                logger.info(f"  Skipped (domain limit): {stats['urls_skipped_domain_limit']:,}")
                logger.info(f"  Skipped (queue limit): {stats['urls_skipped_queue_limit']:,}")
                logger.info(f"  Skipped (invalid URL): {stats['urls_skipped_invalid']:,}")
                logger.info("=" * 60)

    async def test_connection(self) -> bool:
        """Kafka 및 Redis 연결 테스트"""
        logger.info("Testing connections...")

        # Kafka 연결 테스트
        try:
            from aiokafka import AIOKafkaConsumer
            config = get_config()
            consumer = AIOKafkaConsumer(
                config.topics.discovered_urls,
                bootstrap_servers=self.kafka_servers,
            )
            await consumer.start()
            await consumer.stop()
            logger.info(f"Kafka connection: OK ({self.kafka_servers})")
            kafka_ok = True
        except Exception as e:
            logger.error(f"Kafka connection: FAILED - {e}")
            kafka_ok = False

        # Redis 연결 테스트
        try:
            from src.managers.sharded_queue_manager import ShardedRedisQueueManager
            qm = ShardedRedisQueueManager()
            redis_ok = qm.test_connection()
            if redis_ok:
                logger.info("Redis connection: OK (all shards)")
        except Exception as e:
            logger.error(f"Redis connection: FAILED - {e}")
            redis_ok = False

        return kafka_ok and redis_ok


def parse_args():
    """명령줄 인자 파싱"""
    parser = argparse.ArgumentParser(
        description='URL Queue Runner: discovered.urls Kafka → Redis Queue',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        '--kafka-servers',
        type=str,
        default=None,
        help='Kafka 브로커 주소 (예: localhost:9092)',
    )
    parser.add_argument(
        '--test-connection',
        action='store_true',
        help='Kafka/Redis 연결만 테스트하고 종료',
    )
    parser.add_argument(
        '--stats',
        action='store_true',
        help='현재 Redis 큐 통계를 출력하고 종료',
    )
    parser.add_argument(
        '--debug',
        action='store_true',
        help='디버그 로깅 활성화',
    )

    return parser.parse_args()


async def main():
    """메인 함수"""
    args = parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    runner = URLQueueRunner(
        kafka_servers=args.kafka_servers,
    )

    # 연결 테스트 모드
    if args.test_connection:
        ok = await runner.test_connection()
        sys.exit(0 if ok else 1)

    # 큐 통계 출력 모드
    if args.stats:
        from src.managers.sharded_queue_manager import ShardedRedisQueueManager
        qm = ShardedRedisQueueManager()
        stats = qm.get_queue_stats()
        logger.info("Redis queue stats:")
        logger.info(f"  Total pending: {stats.get('total_pending', 0):,}")
        logger.info(f"  Processing: {stats.get('processing', 0):,}")
        logger.info(f"  Completed: {stats.get('completed', 0):,}")
        logger.info(f"  Failed: {stats.get('failed', 0):,}")
        logger.info(f"  Retry: {stats.get('retry', 0):,}")
        sys.exit(0)

    # 정상 실행
    await runner.run()


if __name__ == '__main__':
    asyncio.run(main())

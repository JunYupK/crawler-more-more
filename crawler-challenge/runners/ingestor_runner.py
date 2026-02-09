#!/usr/bin/env python3
"""
Ingestor Runner - Mac용 고속 크롤러 실행기
==========================================

Mac에서 실행하여 대량의 URL을 크롤링하고 Kafka로 전송

Usage:
    # 기본 실행 (Tranco 리스트 사용)
    python runners/ingestor_runner.py

    # URL 파일 지정
    python runners/ingestor_runner.py --url-file urls.txt

    # 테스트 모드 (소량)
    python runners/ingestor_runner.py --test --limit 100

    # Kafka 서버 지정
    python runners/ingestor_runner.py --kafka-servers desktop:9092
"""

import asyncio
import argparse
import logging
import signal
import sys
import time
from pathlib import Path
from typing import Optional

# 프로젝트 루트를 path에 추가
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.ingestor.httpx_crawler import HighSpeedIngestor
from src.ingestor.kafka_producer import KafkaPageProducer
from src.managers.tranco_manager import TrancoManager
from src.common.kafka_config import get_config

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
logger = logging.getLogger(__name__)


class IngestorRunner:
    """Ingestor 실행기"""

    def __init__(
        self,
        kafka_servers: Optional[str] = None,
        max_concurrent: int = 500,
        batch_size: int = 100,
    ):
        self.kafka_servers = kafka_servers or get_config().kafka.bootstrap_servers
        self.max_concurrent = max_concurrent
        self.batch_size = batch_size

        self._running = False
        self._ingestor: Optional[HighSpeedIngestor] = None
        self._producer: Optional[KafkaPageProducer] = None

        # 종료 시그널 핸들러
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """종료 시그널 처리"""
        logger.info(f"Received signal {signum}, shutting down...")
        self._running = False

    async def run_with_tranco(
        self,
        limit: Optional[int] = None,
        start_rank: int = 1,
    ) -> None:
        """
        Tranco Top 1M 리스트로 크롤링 실행

        Args:
            limit: 크롤링할 URL 수 제한 (None이면 전체)
            start_rank: 시작 순위
        """
        logger.info("Loading Tranco Top 1M list...")

        # Tranco 리스트 로드
        tranco_manager = TrancoManager()
        urls = tranco_manager.get_urls(limit=limit, start_rank=start_rank)

        if not urls:
            logger.error("Failed to load Tranco list")
            return

        logger.info(f"Loaded {len(urls):,} URLs from Tranco list")
        await self._run_crawl(urls)

    async def run_with_file(self, file_path: str) -> None:
        """
        파일에서 URL 로드하여 크롤링 실행

        Args:
            file_path: URL 파일 경로 (한 줄에 하나씩)
        """
        logger.info(f"Loading URLs from {file_path}...")

        urls = []
        with open(file_path, 'r') as f:
            for line in f:
                url = line.strip()
                if url and not url.startswith('#'):
                    # http:// 또는 https:// 없으면 추가
                    if not url.startswith(('http://', 'https://')):
                        url = f'https://{url}'
                    urls.append(url)

        if not urls:
            logger.error("No URLs found in file")
            return

        logger.info(f"Loaded {len(urls):,} URLs from file")
        await self._run_crawl(urls)

    async def run_with_urls(self, urls: list[str]) -> None:
        """
        URL 리스트로 크롤링 실행

        Args:
            urls: 크롤링할 URL 리스트
        """
        logger.info(f"Running with {len(urls):,} URLs")
        await self._run_crawl(urls)

    async def _run_crawl(self, urls: list[str]) -> None:
        """크롤링 실행 (내부 메서드)"""
        self._running = True
        start_time = time.time()

        try:
            # Kafka Producer 시작
            logger.info(f"Connecting to Kafka: {self.kafka_servers}")
            self._producer = KafkaPageProducer(bootstrap_servers=self.kafka_servers)
            await self._producer.start()

            # Ingestor 시작
            logger.info(f"Starting Ingestor: max_concurrent={self.max_concurrent}")
            self._ingestor = HighSpeedIngestor()
            self._ingestor.set_kafka_producer(self._producer)
            await self._ingestor.start()

            # 크롤링 실행
            total_urls = len(urls)
            success_total = 0
            fail_total = 0

            # 배치 단위로 처리
            for i in range(0, total_urls, self.batch_size * 10):
                if not self._running:
                    logger.info("Stopping due to shutdown signal...")
                    break

                batch_urls = urls[i:i + self.batch_size * 10]

                success, fail = await self._ingestor.crawl_and_produce(
                    batch_urls,
                    batch_size=self.batch_size,
                )

                success_total += success
                fail_total += fail

                # 중간 통계
                elapsed = time.time() - start_time
                processed = i + len(batch_urls)
                rps = processed / elapsed if elapsed > 0 else 0

                logger.info(
                    f"[Progress] {processed:,}/{total_urls:,} "
                    f"({processed/total_urls*100:.1f}%) | "
                    f"Success: {success_total:,} | "
                    f"Failed: {fail_total:,} | "
                    f"RPS: {rps:.1f}"
                )

                # 메트릭 전송
                await self._producer.send_metrics(
                    metric_type='throughput',
                    data={
                        'processed': processed,
                        'total': total_urls,
                        'success': success_total,
                        'failed': fail_total,
                        'rps': rps,
                    }
                )

            # 최종 통계
            elapsed = time.time() - start_time
            logger.info("=" * 60)
            logger.info("Crawling completed!")
            logger.info(f"Total URLs: {total_urls:,}")
            logger.info(f"Successful: {success_total:,} ({success_total/total_urls*100:.1f}%)")
            logger.info(f"Failed: {fail_total:,} ({fail_total/total_urls*100:.1f}%)")
            logger.info(f"Total time: {elapsed:.1f}s")
            logger.info(f"Average RPS: {total_urls/elapsed:.1f}")
            logger.info(f"Ingestor stats: {self._ingestor.stats}")
            logger.info(f"Producer stats: {self._producer.stats}")
            logger.info("=" * 60)

        except Exception as e:
            logger.error(f"Error during crawling: {e}", exc_info=True)

        finally:
            # 리소스 정리
            if self._ingestor:
                await self._ingestor.stop()
            if self._producer:
                await self._producer.stop()

    async def test_connection(self) -> bool:
        """Kafka 연결 테스트"""
        try:
            producer = KafkaPageProducer(bootstrap_servers=self.kafka_servers)
            await producer.start()
            await producer.stop()
            logger.info("Kafka connection test: SUCCESS")
            return True
        except Exception as e:
            logger.error(f"Kafka connection test: FAILED - {e}")
            return False


def parse_args():
    """명령줄 인자 파싱"""
    parser = argparse.ArgumentParser(
        description='High-Speed Ingestor for Stream Pipeline',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    # 입력 소스
    source_group = parser.add_mutually_exclusive_group()
    source_group.add_argument(
        '--url-file',
        type=str,
        help='URL 파일 경로 (한 줄에 하나씩)',
    )
    source_group.add_argument(
        '--urls',
        type=str,
        nargs='+',
        help='직접 URL 지정',
    )

    # Tranco 옵션
    parser.add_argument(
        '--limit',
        type=int,
        default=None,
        help='크롤링할 URL 수 제한',
    )
    parser.add_argument(
        '--start-rank',
        type=int,
        default=1,
        help='Tranco 시작 순위',
    )

    # Kafka 설정
    parser.add_argument(
        '--kafka-servers',
        type=str,
        default=None,
        help='Kafka 브로커 주소 (예: localhost:9092)',
    )

    # 성능 설정
    parser.add_argument(
        '--max-concurrent',
        type=int,
        default=500,
        help='최대 동시 요청 수',
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        default=100,
        help='Kafka 전송 배치 크기',
    )

    # 모드
    parser.add_argument(
        '--test',
        action='store_true',
        help='테스트 모드 (기본 100개)',
    )
    parser.add_argument(
        '--test-connection',
        action='store_true',
        help='Kafka 연결만 테스트',
    )

    # 로깅
    parser.add_argument(
        '--debug',
        action='store_true',
        help='디버그 로깅 활성화',
    )

    return parser.parse_args()


async def main():
    """메인 함수"""
    args = parse_args()

    # 디버그 로깅
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
        logging.getLogger('httpx').setLevel(logging.DEBUG)
        logging.getLogger('aiokafka').setLevel(logging.DEBUG)

    # Runner 생성
    runner = IngestorRunner(
        kafka_servers=args.kafka_servers,
        max_concurrent=args.max_concurrent,
        batch_size=args.batch_size,
    )

    # 연결 테스트
    if args.test_connection:
        await runner.test_connection()
        return

    # 테스트 모드
    if args.test:
        limit = args.limit or 100
        logger.info(f"Test mode: crawling {limit} URLs")
        await runner.run_with_tranco(limit=limit)
        return

    # URL 파일
    if args.url_file:
        await runner.run_with_file(args.url_file)
        return

    # 직접 URL 지정
    if args.urls:
        await runner.run_with_urls(args.urls)
        return

    # 기본: Tranco 리스트
    await runner.run_with_tranco(limit=args.limit, start_rank=args.start_rank)


if __name__ == '__main__':
    asyncio.run(main())

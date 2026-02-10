#!/usr/bin/env python3
"""
Storage Runner - Hybrid Storage 실행기
======================================

Desktop에서 실행하여 processed.final 토픽의 데이터를 저장

Usage:
    # 스토리지 서비스 실행
    python runners/storage_runner.py

    # 테스트 모드
    python runners/storage_runner.py --test --max-messages 100

    # 연결 테스트
    python runners/storage_runner.py --test-connection

    # 데모 (샘플 데이터 저장)
    python runners/storage_runner.py --demo
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

from src.storage.hybrid_storage import (
    HybridStorage,
    StorageWriter,
    StorageResult,
    test_storage_connections,
)
from src.common.kafka_config import get_config

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
logger = logging.getLogger(__name__)


class StorageRunner:
    """Hybrid Storage 실행기"""

    def __init__(
        self,
        kafka_servers: Optional[str] = None,
        store_raw_html: bool = False,
    ):
        self.kafka_servers = kafka_servers or get_config().kafka.bootstrap_servers
        self.store_raw_html = store_raw_html

        self._running = False
        self._storage: Optional[HybridStorage] = None

        # 종료 시그널 핸들러
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """종료 시그널 처리"""
        logger.info(f"Received signal {signum}, shutting down...")
        self._running = False

        if self._storage:
            self._storage._running = False

    async def run(self, max_messages: Optional[int] = None) -> None:
        """
        스토리지 서비스 실행

        Args:
            max_messages: 최대 처리 메시지 수
        """
        self._running = True
        start_time = time.time()
        stored_count = 0

        def on_stored(result: StorageResult):
            nonlocal stored_count
            if result.success:
                stored_count += 1

            if stored_count % 50 == 0:
                elapsed = time.time() - start_time
                logger.info(
                    f"[Progress] Stored: {stored_count:,} | "
                    f"Rate: {stored_count/elapsed:.1f} msg/s | "
                    f"MinIO: {result.minio_markdown is not None} | "
                    f"Postgres: {result.postgres_saved}"
                )

        try:
            logger.info(f"Connecting to Kafka: {self.kafka_servers}")
            logger.info(f"Store raw HTML: {self.store_raw_html}")

            self._storage = HybridStorage(
                bootstrap_servers=self.kafka_servers,
                store_raw_html=self.store_raw_html,
            )

            async with self._storage:
                await self._storage.run(
                    max_messages=max_messages,
                    callback=on_stored,
                )

            # 최종 통계 출력
            stats = self._storage.get_stats()
            elapsed = time.time() - start_time

            logger.info("=" * 60)
            logger.info("Hybrid Storage stopped!")
            logger.info(f"Total time: {elapsed:.1f}s")
            logger.info(f"Messages consumed: {stats['hybrid']['messages_consumed']:,}")
            logger.info(f"Messages stored: {stats['hybrid']['messages_stored']:,}")
            logger.info(f"Messages failed: {stats['hybrid']['messages_failed']:,}")
            logger.info(f"Success rate: {stats['hybrid']['success_rate']:.1%}")
            logger.info(f"Rate: {stats['hybrid']['messages_per_second']:.1f} msg/s")
            logger.info("-" * 40)
            logger.info("MinIO Stats:")
            logger.info(f"  Objects stored: {stats['minio']['objects_stored']:,}")
            logger.info(f"  Bytes stored: {stats['minio']['total_bytes_stored']:,}")
            logger.info(f"  Compression ratio: {stats['minio']['compression_ratio']:.1%}")
            logger.info("-" * 40)
            logger.info("PostgreSQL Stats:")
            logger.info(f"  Records inserted: {stats['postgres']['records_inserted']:,}")
            logger.info(f"  Records updated: {stats['postgres']['records_updated']:,}")
            logger.info(f"  Batch count: {stats['postgres']['batch_count']:,}")
            logger.info("=" * 60)

        except Exception as e:
            logger.error(f"Error running storage: {e}", exc_info=True)

    async def test_connections(self) -> bool:
        """스토리지 연결 테스트"""
        logger.info("Testing storage connections...")

        result = await test_storage_connections()

        print("\n" + "=" * 50)
        print("STORAGE CONNECTION TEST")
        print("=" * 50)

        # MinIO 결과
        minio = result.get('minio', {})
        print(f"\n📦 MinIO:")
        print(f"   Endpoint: {minio.get('endpoint', 'N/A')}")
        print(f"   Connected: {'✅ Yes' if minio.get('connected') else '❌ No'}")
        if minio.get('connected'):
            print(f"   Buckets: {', '.join(minio.get('buckets', []))}")
        if minio.get('error'):
            print(f"   Error: {minio.get('error')}")

        # PostgreSQL 결과
        postgres = result.get('postgres', {})
        print(f"\n🐘 PostgreSQL:")
        print(f"   Host: {postgres.get('host', 'N/A')}")
        print(f"   Database: {postgres.get('database', 'N/A')}")
        print(f"   Connected: {'✅ Yes' if postgres.get('connected') else '❌ No'}")
        if postgres.get('connected'):
            print(f"   Tables: {', '.join(postgres.get('tables', []))}")
        if postgres.get('error'):
            print(f"   Error: {postgres.get('error')}")

        # 전체 결과
        all_connected = result.get('all_connected', False)
        print(f"\n{'✅' if all_connected else '❌'} All systems: {'CONNECTED' if all_connected else 'FAILED'}")
        print("=" * 50 + "\n")

        return all_connected


async def demo_storage():
    """
    스토리지 데모

    Kafka 없이 직접 저장 테스트
    """
    logger.info("Running storage demo...")

    # 샘플 데이터
    sample_data = [
        {
            'url': 'https://example.com/article/1',
            'markdown': '# Sample Article 1\n\nThis is a sample article with some content.\n\n## Section 1\n\nMore content here.',
            'metadata': {
                'title': 'Sample Article 1',
                'description': 'A sample article for testing',
                'language': 'en',
            },
            'score': 85,
        },
        {
            'url': 'https://test.org/blog/post',
            'markdown': '# Blog Post\n\nWelcome to my blog post.\n\n## Introduction\n\nThis is the intro.\n\n## Main Content\n\nHere is the main content of the post.',
            'metadata': {
                'title': 'Blog Post',
                'description': 'A blog post about something',
                'language': 'en',
            },
            'score': 72,
        },
        {
            'url': 'https://docs.example.com/api/reference',
            'markdown': '# API Reference\n\n## Endpoints\n\n### GET /users\n\nReturns a list of users.\n\n### POST /users\n\nCreates a new user.',
            'metadata': {
                'title': 'API Reference',
                'description': 'API documentation',
                'language': 'en',
            },
            'score': 95,
        },
    ]

    print("\n" + "=" * 60)
    print("STORAGE DEMO")
    print("=" * 60)

    try:
        async with StorageWriter() as writer:
            for i, data in enumerate(sample_data, 1):
                print(f"\n[{i}/{len(sample_data)}] Storing: {data['url']}")

                result = await writer.save(
                    url=data['url'],
                    markdown=data['markdown'],
                    metadata=data['metadata'],
                    processor='fast' if data['score'] >= 80 else 'rich',
                    score=data['score'],
                )

                if result.success:
                    print(f"   ✅ Success!")
                    print(f"   📦 MinIO key: {result.minio_markdown.key if result.minio_markdown else 'N/A'}")
                    print(f"   🐘 PostgreSQL: {'Saved' if result.postgres_saved else 'Failed'}")
                    print(f"   ⏱️ Time: {result.processing_time_ms:.1f}ms")
                else:
                    print(f"   ❌ Failed: {result.error}")

        print("\n" + "-" * 60)
        print("Demo completed successfully!")
        print("=" * 60 + "\n")

    except Exception as e:
        print(f"\n❌ Demo failed: {e}")
        logger.error(f"Demo error: {e}", exc_info=True)


async def query_page(url: str):
    """
    저장된 페이지 조회

    Args:
        url: 조회할 URL
    """
    logger.info(f"Querying page: {url}")

    try:
        storage = HybridStorage()
        await storage.minio.initialize_buckets()
        await storage.postgres.start()

        try:
            page = await storage.get_page_with_content(url)

            print("\n" + "=" * 60)
            print("PAGE QUERY RESULT")
            print("=" * 60)

            if page:
                print(f"\n📌 URL: {page.get('url', 'N/A')}")
                print(f"🌐 Domain: {page.get('domain', 'N/A')}")
                print(f"📝 Title: {page.get('title', 'N/A')}")
                print(f"📄 Description: {(page.get('description') or 'N/A')[:100]}")
                print(f"🔧 Processor: {page.get('processor', 'N/A')}")
                print(f"📊 Score: {page.get('static_score', 'N/A')}")
                print(f"📦 MinIO Key: {page.get('markdown_key', 'N/A')}")

                if page.get('markdown'):
                    print(f"\n📄 Markdown Preview (first 500 chars):")
                    print("-" * 40)
                    print(page['markdown'][:500])
                    print("-" * 40)
                else:
                    print("\n⚠️ Markdown content not found in MinIO")
            else:
                print(f"\n❌ Page not found: {url}")

            print("=" * 60 + "\n")

        finally:
            await storage.postgres.stop()

    except Exception as e:
        print(f"\n❌ Query failed: {e}")
        logger.error(f"Query error: {e}", exc_info=True)


async def show_stats():
    """데이터베이스 통계 조회"""
    from src.storage.postgres_writer import PostgresWriter

    logger.info("Fetching database stats...")

    try:
        async with PostgresWriter() as writer:
            stats = await writer.get_stats_summary()

            print("\n" + "=" * 60)
            print("DATABASE STATISTICS")
            print("=" * 60)

            print(f"\n📊 Total pages: {stats.get('total_pages', 0):,}")
            print(f"🕐 Recent hour: {stats.get('recent_hour', 0):,}")
            print(f"📈 Average score: {stats.get('average_score', 0):.1f}" if stats.get('average_score') else "📈 Average score: N/A")

            processor_stats = stats.get('processor_stats', {})
            if processor_stats:
                print(f"\n📋 By processor:")
                for processor, count in processor_stats.items():
                    print(f"   {processor}: {count:,}")

            print("=" * 60 + "\n")

    except Exception as e:
        print(f"\n❌ Stats query failed: {e}")
        logger.error(f"Stats error: {e}", exc_info=True)


def parse_args():
    """명령줄 인자 파싱"""
    parser = argparse.ArgumentParser(
        description='Hybrid Storage (MinIO + PostgreSQL) Runner',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    # Kafka 설정
    parser.add_argument(
        '--kafka-servers',
        type=str,
        default=None,
        help='Kafka 브로커 주소',
    )

    # 저장 옵션
    parser.add_argument(
        '--store-html',
        action='store_true',
        help='Raw HTML도 MinIO에 저장',
    )

    # 실행 모드
    parser.add_argument(
        '--max-messages',
        type=int,
        default=None,
        help='최대 처리 메시지 수',
    )
    parser.add_argument(
        '--test',
        action='store_true',
        help='테스트 모드 (기본 100개)',
    )
    parser.add_argument(
        '--test-connection',
        action='store_true',
        help='스토리지 연결 테스트',
    )

    # 유틸리티
    parser.add_argument(
        '--demo',
        action='store_true',
        help='데모 실행 (Kafka 없이 직접 저장)',
    )
    parser.add_argument(
        '--query',
        type=str,
        help='저장된 페이지 조회 (URL)',
    )
    parser.add_argument(
        '--stats',
        action='store_true',
        help='데이터베이스 통계 조회',
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

    # 데모 모드
    if args.demo:
        await demo_storage()
        return

    # 페이지 조회
    if args.query:
        await query_page(args.query)
        return

    # 통계 조회
    if args.stats:
        await show_stats()
        return

    # Runner 생성
    runner = StorageRunner(
        kafka_servers=args.kafka_servers,
        store_raw_html=args.store_html,
    )

    # 연결 테스트
    if args.test_connection:
        await runner.test_connections()
        return

    # 테스트 모드
    max_messages = args.max_messages
    if args.test and not max_messages:
        max_messages = 100

    # 스토리지 실행
    await runner.run(max_messages=max_messages)


if __name__ == '__main__':
    asyncio.run(main())

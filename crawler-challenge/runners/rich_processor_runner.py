#!/usr/bin/env python3
"""
Rich Processor Runner - Crawl4AI 워커 실행기
=============================================

Desktop에서 실행하여 process.rich 토픽의 동적 페이지를 처리

Usage:
    # 단일 워커 실행
    python runners/rich_processor_runner.py

    # 다중 워커 실행 (2개 - 브라우저 리소스 제한)
    python runners/rich_processor_runner.py --workers 2

    # 테스트 모드
    python runners/rich_processor_runner.py --test --max-messages 50

    # Crawl4AI 설치 상태 확인
    python runners/rich_processor_runner.py --check-crawl4ai

    # URL 직접 처리 (실제 브라우저 사용)
    python runners/rich_processor_runner.py --process-url https://example.com
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

from src.processor.rich_worker import RichWorker, RichWorkerPool, check_crawl4ai_installation
from src.processor.base_worker import ProcessedResult
from src.common.kafka_config import get_config

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
logger = logging.getLogger(__name__)


class RichProcessorRunner:
    """Rich Processor 실행기"""

    def __init__(
        self,
        kafka_servers: Optional[str] = None,
        num_workers: int = 1,
        headless: bool = True,
    ):
        self.kafka_servers = kafka_servers or get_config().kafka.bootstrap_servers
        self.num_workers = num_workers
        self.headless = headless

        self._running = False
        self._pool: Optional[RichWorkerPool] = None
        self._worker: Optional[RichWorker] = None

        # 종료 시그널 핸들러
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """종료 시그널 처리"""
        logger.info(f"Received signal {signum}, shutting down...")
        self._running = False

        # 워커 중지 플래그 설정
        if self._worker:
            self._worker._running = False
        if self._pool:
            for worker in self._pool.workers:
                worker._running = False

    async def run(self, max_messages: Optional[int] = None) -> None:
        """
        프로세서 실행

        Args:
            max_messages: 최대 처리 메시지 수
        """
        self._running = True
        start_time = time.time()
        processed_count = 0

        def on_processed(result: ProcessedResult):
            nonlocal processed_count
            processed_count += 1

            if processed_count % 10 == 0:  # Rich는 느리므로 10개마다 로깅
                elapsed = time.time() - start_time
                logger.info(
                    f"[Progress] Processed: {processed_count:,} | "
                    f"Rate: {processed_count/elapsed:.2f} msg/s | "
                    f"Success: {result.success}"
                )

        try:
            # Crawl4AI 상태 확인
            crawl4ai_status = check_crawl4ai_installation()
            logger.info(f"Crawl4AI status: {crawl4ai_status}")

            if not crawl4ai_status['installed']:
                logger.warning("Crawl4AI not installed. Using fallback mode (BeautifulSoup).")
                logger.warning("Install with: pip install crawl4ai && crawl4ai-setup")

            logger.info(f"Connecting to Kafka: {self.kafka_servers}")
            logger.info(f"Number of workers: {self.num_workers}")
            logger.info(f"Headless mode: {self.headless}")

            if self.num_workers > 1:
                # 다중 워커 모드
                self._pool = RichWorkerPool(
                    num_workers=self.num_workers,
                    bootstrap_servers=self.kafka_servers,
                    headless=self.headless,
                )
                await self._pool.start()

                # 워커별 max_messages 계산
                per_worker = max_messages // self.num_workers if max_messages else None
                await self._pool.run(max_messages_per_worker=per_worker)

                # 최종 통계
                stats = self._pool.get_combined_stats()

            else:
                # 단일 워커 모드
                self._worker = RichWorker(
                    bootstrap_servers=self.kafka_servers,
                    worker_id=0,
                    headless=self.headless,
                )

                async with self._worker:
                    await self._worker.run(
                        max_messages=max_messages,
                        callback=on_processed,
                    )

                stats = self._worker.get_stats()

            # 최종 통계 출력
            elapsed = time.time() - start_time
            logger.info("=" * 60)
            logger.info("Rich Processor stopped!")
            logger.info(f"Total time: {elapsed:.1f}s")
            logger.info(f"Stats: {stats}")
            logger.info("=" * 60)

        except Exception as e:
            logger.error(f"Error running rich processor: {e}", exc_info=True)

        finally:
            if self._pool:
                await self._pool.stop()

    async def test_connection(self) -> bool:
        """Kafka 연결 테스트"""
        try:
            from aiokafka import AIOKafkaConsumer
            config = get_config()

            consumer = AIOKafkaConsumer(
                config.topics.process_rich,
                bootstrap_servers=self.kafka_servers,
            )
            await consumer.start()
            await consumer.stop()
            logger.info("Kafka connection test: SUCCESS")
            return True
        except Exception as e:
            logger.error(f"Kafka connection test: FAILED - {e}")
            return False


async def process_url(url: str, headless: bool = True) -> None:
    """
    URL 직접 처리 (실제 브라우저 사용)

    Args:
        url: 처리할 URL
        headless: 헤드리스 모드
    """
    logger.info(f"Processing URL: {url}")
    logger.info(f"Headless: {headless}")

    worker = RichWorker(
        worker_id=0,
        headless=headless,
    )

    # 브라우저 초기화를 위해 시작 필요
    await worker.start()

    try:
        result = await worker.process_html(
            html="",  # URL을 직접 방문하므로 비어 있음
            url=url,
            metadata={},
        )

        print("\n" + "=" * 60)
        print("PROCESSING RESULT")
        print("=" * 60)

        print(f"\n📊 Success: {result.success}")
        print(f"🔧 Processor: {result.processor_type.value}")
        print(f"⏱️ Processing time: {result.processing_time_ms:.1f}ms")

        if result.success:
            print(f"\n📝 Metadata:")
            print(f"   Title: {result.title or 'N/A'}")
            print(f"   Description: {(result.description or 'N/A')[:100]}...")

            print(f"\n📈 Content Stats:")
            print(f"   Markdown: {result.markdown_length:,} chars")
            print(f"   Links: {len(result.links or [])}")
            print(f"   Images: {len(result.images or [])}")

            if result.links:
                print(f"\n🔗 Sample Links:")
                for link in result.links[:5]:
                    print(f"   • {link[:80]}...")

            print(f"\n📄 Markdown Preview (first 1000 chars):")
            print("-" * 40)
            print((result.markdown or "")[:1000])
            print("-" * 40)

        else:
            print(f"\n❌ Error: {result.error_type}")
            print(f"   Message: {result.error_message}")

        print("\n" + "=" * 60)

    finally:
        await worker.stop()


def check_crawl4ai() -> None:
    """Crawl4AI 설치 상태 상세 확인"""
    print("\n" + "=" * 60)
    print("CRAWL4AI INSTALLATION CHECK")
    print("=" * 60)

    status = check_crawl4ai_installation()

    if status['installed']:
        print("\n✅ Crawl4AI is installed")
        print(f"   Version: {status.get('version', 'unknown')}")
        print(f"   Browser: {status.get('browser_available', 'unknown')}")

        if status.get('browser_available') == True:
            print("\n✅ Browser is available and working")
        elif status.get('browser_available') == False:
            print("\n⚠️ Browser not available")
            print("   Run: crawl4ai-setup")
        else:
            print(f"\n❓ Browser status: {status.get('browser_available')}")

    else:
        print("\n❌ Crawl4AI is NOT installed")
        print("\n   To install:")
        print("   1. pip install crawl4ai")
        print("   2. crawl4ai-setup")
        print("\n   Or with Docker:")
        print("   docker pull unclecode/crawl4ai:latest")

    if 'error' in status:
        print(f"\n⚠️ Error during check: {status['error']}")

    print("\n" + "=" * 60)


def demo_processing():
    """처리 데모 (폴백 모드)"""
    test_html = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Dynamic Page Demo</title>
        <meta name="description" content="A page that would normally need JavaScript">
    </head>
    <body>
        <div id="root">
            <h1>Dynamic Content</h1>
            <p>This content would normally be loaded by JavaScript,
            but we're demonstrating the fallback processing mode.</p>

            <div class="content">
                <h2>Section with Links</h2>
                <p>Here's a <a href="https://example.com/page1">link to page 1</a>
                and another <a href="https://example.com/page2">link to page 2</a>.</p>

                <h2>Section with Images</h2>
                <img src="https://example.com/image1.jpg" alt="Image 1">
                <img src="https://example.com/image2.jpg" alt="Image 2">
            </div>
        </div>
    </body>
    </html>
    """

    async def process():
        worker = RichWorker(worker_id=0)
        return await worker._process_fallback(
            html=test_html,
            url="https://example.com/dynamic",
            metadata={},
        )

    result = asyncio.run(process())

    print("\n" + "=" * 60)
    print("RICH WORKER DEMO (Fallback Mode)")
    print("=" * 60)

    print(f"\nSuccess: {result.success}")
    print(f"Title: {result.title}")
    print(f"Description: {result.description}")

    print(f"\nExtracted:")
    print(f"  Links: {result.links}")
    print(f"  Images: {result.images}")

    print(f"\nMarkdown (first 500 chars):")
    print("-" * 40)
    print((result.markdown or "")[:500])
    print("-" * 40)

    print("\n" + "=" * 60)


def parse_args():
    """명령줄 인자 파싱"""
    parser = argparse.ArgumentParser(
        description='Rich Processor (Crawl4AI) for Stream Pipeline',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    # Kafka 설정
    parser.add_argument(
        '--kafka-servers',
        type=str,
        default=None,
        help='Kafka 브로커 주소',
    )

    # 워커 설정
    parser.add_argument(
        '--workers',
        type=int,
        default=1,
        help='워커 수 (브라우저 리소스 제한으로 2-3 권장)',
    )
    parser.add_argument(
        '--no-headless',
        action='store_true',
        help='브라우저 UI 표시 (디버깅용)',
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
        help='테스트 모드 (기본 50개)',
    )
    parser.add_argument(
        '--test-connection',
        action='store_true',
        help='Kafka 연결만 테스트',
    )

    # Crawl4AI 관련
    parser.add_argument(
        '--check-crawl4ai',
        action='store_true',
        help='Crawl4AI 설치 상태 확인',
    )
    parser.add_argument(
        '--process-url',
        type=str,
        help='URL 직접 처리 (실제 브라우저 사용)',
    )
    parser.add_argument(
        '--demo',
        action='store_true',
        help='처리 데모 실행 (폴백 모드)',
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

    # Crawl4AI 확인
    if args.check_crawl4ai:
        check_crawl4ai()
        return

    # URL 처리 모드
    if args.process_url:
        await process_url(args.process_url, headless=not args.no_headless)
        return

    # 데모 모드
    if args.demo:
        demo_processing()
        return

    # Runner 생성
    runner = RichProcessorRunner(
        kafka_servers=args.kafka_servers,
        num_workers=args.workers,
        headless=not args.no_headless,
    )

    # 연결 테스트
    if args.test_connection:
        await runner.test_connection()
        return

    # 테스트 모드
    max_messages = args.max_messages
    if args.test and not max_messages:
        max_messages = 50  # Rich는 느리므로 50개

    # 프로세서 실행
    await runner.run(max_messages=max_messages)


if __name__ == '__main__':
    asyncio.run(main())

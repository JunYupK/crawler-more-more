#!/usr/bin/env python3
"""
Fast Processor Runner - BeautifulSoup 워커 실행기
=================================================

Desktop에서 실행하여 process.fast 토픽의 정적 페이지를 처리

Usage:
    # 단일 워커 실행
    python runners/fast_processor_runner.py

    # 다중 워커 실행 (4개)
    python runners/fast_processor_runner.py --workers 4

    # 테스트 모드
    python runners/fast_processor_runner.py --test --max-messages 100

    # HTML 파일 직접 처리
    python runners/fast_processor_runner.py --process-file page.html
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

from src.processor.fast_worker import FastWorker, FastWorkerPool
from src.processor.base_worker import ProcessedResult
from config.kafka_config import get_config

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
logger = logging.getLogger(__name__)


class FastProcessorRunner:
    """Fast Processor 실행기"""

    def __init__(
        self,
        kafka_servers: Optional[str] = None,
        num_workers: int = 1,
    ):
        self.kafka_servers = kafka_servers or get_config().kafka.bootstrap_servers
        self.num_workers = num_workers

        self._running = False
        self._pool: Optional[FastWorkerPool] = None
        self._worker: Optional[FastWorker] = None

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

            if processed_count % 50 == 0:
                elapsed = time.time() - start_time
                logger.info(
                    f"[Progress] Processed: {processed_count:,} | "
                    f"Rate: {processed_count/elapsed:.1f} msg/s | "
                    f"Success: {result.success}"
                )

        try:
            logger.info(f"Connecting to Kafka: {self.kafka_servers}")
            logger.info(f"Number of workers: {self.num_workers}")

            if self.num_workers > 1:
                # 다중 워커 모드
                self._pool = FastWorkerPool(
                    num_workers=self.num_workers,
                    bootstrap_servers=self.kafka_servers,
                )
                await self._pool.start()

                # 워커별 max_messages 계산
                per_worker = max_messages // self.num_workers if max_messages else None
                await self._pool.run(max_messages_per_worker=per_worker)

                # 최종 통계
                stats = self._pool.get_combined_stats()

            else:
                # 단일 워커 모드
                self._worker = FastWorker(
                    bootstrap_servers=self.kafka_servers,
                    worker_id=0,
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
            logger.info("Fast Processor stopped!")
            logger.info(f"Total time: {elapsed:.1f}s")
            logger.info(f"Stats: {stats}")
            logger.info("=" * 60)

        except Exception as e:
            logger.error(f"Error running fast processor: {e}", exc_info=True)

        finally:
            if self._pool:
                await self._pool.stop()

    async def test_connection(self) -> bool:
        """Kafka 연결 테스트"""
        try:
            from aiokafka import AIOKafkaConsumer
            config = get_config()

            consumer = AIOKafkaConsumer(
                config.topics.process_fast,
                bootstrap_servers=self.kafka_servers,
            )
            await consumer.start()
            await consumer.stop()
            logger.info("Kafka connection test: SUCCESS")
            return True
        except Exception as e:
            logger.error(f"Kafka connection test: FAILED - {e}")
            return False


def process_file(file_path: str) -> None:
    """
    HTML 파일 직접 처리 (오프라인 테스트용)

    Args:
        file_path: HTML 파일 경로
    """
    import asyncio

    logger.info(f"Processing file: {file_path}")

    with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
        html = f.read()

    async def process():
        worker = FastWorker(worker_id=0)

        result = await worker.process_html(
            html=html,
            url=f"file://{file_path}",
            metadata={},
        )

        return result

    result = asyncio.run(process())

    print("\n" + "=" * 60)
    print("PROCESSING RESULT")
    print("=" * 60)

    print(f"\n📊 Success: {result.success}")
    print(f"🔧 Processor: {result.processor_type.value}")
    print(f"⏱️ Processing time: {result.processing_time_ms:.1f}ms")

    print(f"\n📝 Metadata:")
    print(f"   Title: {result.title or 'N/A'}")
    print(f"   Description: {(result.description or 'N/A')[:100]}...")
    print(f"   Language: {result.language or 'N/A'}")

    print(f"\n📈 Content Stats:")
    print(f"   Original HTML: {result.content_length:,} bytes")
    print(f"   Markdown: {result.markdown_length:,} bytes")
    print(f"   Links: {len(result.links or [])}")
    print(f"   Images: {len(result.images or [])}")
    print(f"   Headings: {len(result.headings or [])}")

    if result.headings:
        print(f"\n📑 Headings:")
        for heading in result.headings[:5]:
            print(f"   • {heading}")

    print(f"\n📄 Markdown Preview (first 500 chars):")
    print("-" * 40)
    print((result.markdown or "")[:500])
    print("-" * 40)

    if not result.success:
        print(f"\n❌ Error: {result.error_type}: {result.error_message}")

    print("\n" + "=" * 60)


def demo_processing():
    """처리 데모"""
    import asyncio

    test_html = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <title>Sample Article</title>
        <meta name="description" content="This is a sample article for testing.">
        <meta name="keywords" content="sample, test, article">
    </head>
    <body>
        <nav>Navigation here</nav>
        <article>
            <h1>Welcome to the Sample Article</h1>
            <p>This is the first paragraph with some <strong>important</strong> content.
            It demonstrates how the fast worker processes static HTML pages.</p>

            <h2>Section One</h2>
            <p>More content here with a <a href="https://example.com">link</a> to another page.
            The worker will extract this link and include it in the results.</p>

            <img src="https://example.com/image.jpg" alt="Sample image">

            <h2>Section Two</h2>
            <ul>
                <li>First item</li>
                <li>Second item</li>
                <li>Third item</li>
            </ul>

            <h3>Subsection</h3>
            <p>Final paragraph with concluding thoughts about the article topic.</p>
        </article>
        <footer>Footer content</footer>
    </body>
    </html>
    """

    async def process():
        worker = FastWorker(worker_id=0)
        return await worker.process_html(
            html=test_html,
            url="https://example.com/article",
            metadata={},
        )

    result = asyncio.run(process())

    print("\n" + "=" * 60)
    print("FAST WORKER DEMO")
    print("=" * 60)

    print(f"\nSuccess: {result.success}")
    print(f"Title: {result.title}")
    print(f"Description: {result.description}")

    print(f"\nExtracted:")
    print(f"  Links: {len(result.links or [])} found")
    print(f"  Images: {len(result.images or [])} found")
    print(f"  Headings: {result.headings}")

    print(f"\nMarkdown output:")
    print("-" * 40)
    print(result.markdown)
    print("-" * 40)

    print("\n" + "=" * 60)


def parse_args():
    """명령줄 인자 파싱"""
    parser = argparse.ArgumentParser(
        description='Fast Processor (BeautifulSoup) for Stream Pipeline',
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
        help='워커 수',
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
        help='Kafka 연결만 테스트',
    )

    # 오프라인
    parser.add_argument(
        '--process-file',
        type=str,
        help='HTML 파일 직접 처리',
    )
    parser.add_argument(
        '--demo',
        action='store_true',
        help='처리 데모 실행',
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
        demo_processing()
        return

    # 파일 처리 모드
    if args.process_file:
        process_file(args.process_file)
        return

    # Runner 생성
    runner = FastProcessorRunner(
        kafka_servers=args.kafka_servers,
        num_workers=args.workers,
    )

    # 연결 테스트
    if args.test_connection:
        await runner.test_connection()
        return

    # 테스트 모드
    max_messages = args.max_messages
    if args.test and not max_messages:
        max_messages = 100

    # 프로세서 실행
    await runner.run(max_messages=max_messages)


if __name__ == '__main__':
    asyncio.run(main())

#!/usr/bin/env python3
"""
Embedding Runner - processed.final → 임베딩 → pgvector 실행기
==============================================================

EmbeddingWorker를 실행하여 Kafka processed.final 토픽을 소비하고
pgvector에 벡터 임베딩을 저장합니다.

Usage:
    # 기본 실행 (로컬 all-MiniLM-L6-v2 모델)
    python runners/embedding_runner.py

    # OpenAI 임베딩 사용
    EMBED_BACKEND=openai OPENAI_API_KEY=sk-... \\
    python runners/embedding_runner.py

    # 고품질 로컬 모델 사용
    EMBED_MODEL_NAME=all-mpnet-base-v2 \\
    python runners/embedding_runner.py

    # 연결 테스트
    python runners/embedding_runner.py --test-connection

    # 일정 수만 처리 (테스트)
    python runners/embedding_runner.py --max-messages 100

    # 임베딩 현황 통계
    python runners/embedding_runner.py --stats

    # page_id 백필
    python runners/embedding_runner.py --backfill

환경변수:
    EMBED_BACKEND           : "local" | "openai" (기본: local)
    EMBED_MODEL_NAME        : 모델명 (기본: all-MiniLM-L6-v2)
    EMBED_BATCH_SIZE        : 배치 크기 (기본: 32)
    EMBED_MAX_CHUNKS        : URL당 최대 청크 수 (기본: 20)
    EMBED_MAX_CHUNK_CHARS   : 청크 최대 문자 수 (기본: 500)
    EMBED_SKIP_SHORT        : 최소 마크다운 길이 (기본: 200)
    OPENAI_API_KEY          : OpenAI API 키 (EMBED_BACKEND=openai 시)
    KAFKA_BOOTSTRAP_SERVERS : Kafka 브로커
    POSTGRES_HOST/PORT/DB   : PostgreSQL 접속 정보
"""

import asyncio
import argparse
import logging
import signal
import sys
import time
from pathlib import Path
from typing import Optional

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.embedding.embedding_worker import EmbeddingWorker
from src.embedding.rag_search import RAGSearcher
from src.common.kafka_config import get_config

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
logger = logging.getLogger(__name__)


class EmbeddingRunner:
    """EmbeddingWorker 실행기"""

    def __init__(self, kafka_servers: Optional[str] = None):
        self.kafka_servers = kafka_servers or get_config().kafka.bootstrap_servers
        self._worker: Optional[EmbeddingWorker] = None
        self._running = False

        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        logger.info(f"Received signal {signum}, shutting down...")
        self._running = False
        if self._worker:
            self._worker._running = False

    async def run(self, max_messages: Optional[int] = None) -> None:
        """임베딩 워커 실행"""
        self._running = True
        start_time = time.time()

        logger.info("=" * 60)
        logger.info("Embedding Runner starting")
        logger.info(f"  Kafka: {self.kafka_servers}")
        logger.info(f"  Backend: {__import__('os').getenv('EMBED_BACKEND', 'local')}")
        logger.info(f"  Model: {__import__('os').getenv('EMBED_MODEL_NAME', 'all-MiniLM-L6-v2')}")
        logger.info(f"  Batch size: {EmbeddingWorker.BATCH_SIZE}")
        logger.info(f"  Max chunks/URL: {EmbeddingWorker.MAX_CHUNKS}")
        logger.info("=" * 60)

        try:
            self._worker = EmbeddingWorker(
                bootstrap_servers=self.kafka_servers,
            )
            await self._worker.start()
            await self._worker.run(max_messages=max_messages)

        except Exception as e:
            logger.error(f"Error running embedding worker: {e}", exc_info=True)
        finally:
            if self._worker:
                stats = self._worker.get_stats()
                elapsed = time.time() - start_time

                logger.info("=" * 60)
                logger.info("Embedding Runner stopped")
                logger.info(f"  Runtime: {elapsed:.1f}s")
                logger.info(f"  Messages consumed: {stats['messages_consumed']:,}")
                logger.info(f"  Messages processed: {stats['messages_processed']:,}")
                logger.info(f"  Chunks stored: {stats['chunks_stored']:,}")
                logger.info(f"  Avg embed time: {stats['avg_embed_ms']:.1f}ms/batch")
                logger.info(f"  Avg store time: {stats['avg_store_ms']:.1f}ms/batch")
                logger.info(f"  Throughput: {stats['messages_per_second']:.1f} msg/s")
                logger.info("=" * 60)

                await self._worker.stop()

    async def test_connection(self) -> bool:
        """Kafka, PostgreSQL, 임베딩 모델 연결 테스트"""
        all_ok = True

        # Kafka 연결 테스트
        try:
            from aiokafka import AIOKafkaConsumer
            config = get_config()
            c = AIOKafkaConsumer(
                config.topics.processed_final,
                bootstrap_servers=self.kafka_servers,
            )
            await c.start()
            await c.stop()
            logger.info(f"Kafka: OK ({self.kafka_servers})")
        except Exception as e:
            logger.error(f"Kafka: FAILED - {e}")
            all_ok = False

        # PostgreSQL + pgvector 연결 테스트
        try:
            import asyncpg
            config = get_config()
            conn = await asyncpg.connect(dsn=config.postgres.dsn)
            # pgvector 확장 확인
            result = await conn.fetchval(
                "SELECT installed_version FROM pg_available_extensions WHERE name = 'vector'"
            )
            if result:
                logger.info(f"PostgreSQL + pgvector: OK (version={result})")
            else:
                logger.warning("PostgreSQL: OK, but pgvector extension not found. Run 002_add_pgvector.sql")
            await conn.close()
        except Exception as e:
            logger.error(f"PostgreSQL: FAILED - {e}")
            all_ok = False

        # 임베딩 모델 로드 테스트
        try:
            from src.embedding.embedder import create_embedder
            embedder = create_embedder()
            test_vec = embedder.embed_batch(["test"])
            logger.info(
                f"Embedder: OK (model={embedder.model_name}, "
                f"dim={len(test_vec[0])})"
            )
        except Exception as e:
            logger.error(f"Embedder: FAILED - {e}")
            all_ok = False

        return all_ok

    async def show_stats(self) -> None:
        """임베딩 현황 통계 출력"""
        async with RAGSearcher() as searcher:
            stats = await searcher.get_embedding_stats()
            if stats:
                logger.info("Embedding stats:")
                logger.info(f"  URLs embedded: {stats.get('urls_embedded', 0):,}")
                logger.info(f"  Total chunks: {stats.get('total_chunks', 0):,}")
                logger.info(f"  First embedded: {stats.get('first_embedded_at', 'N/A')}")
                logger.info(f"  Last embedded: {stats.get('last_embedded_at', 'N/A')}")
            else:
                logger.info("No embedding data found (page_chunks table may be empty)")

    async def demo_search(self, query: str) -> None:
        """검색 데모"""
        logger.info(f"Searching: '{query}'")
        async with RAGSearcher() as searcher:
            results = await searcher.search(query, top_k=3)
            if results:
                logger.info(f"Found {len(results)} results:")
                for i, r in enumerate(results, 1):
                    logger.info(
                        f"  [{i}] {r.title or 'No title'} "
                        f"({r.domain}) similarity={r.similarity:.3f}"
                    )
                    logger.info(f"      {r.chunk_text[:100]}...")
            else:
                logger.info("No results found")


def parse_args():
    parser = argparse.ArgumentParser(
        description='Embedding Runner: processed.final → pgvector',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument('--kafka-servers', type=str, default=None)
    parser.add_argument('--max-messages', type=int, default=None,
                        help='처리할 최대 메시지 수')
    parser.add_argument('--test-connection', action='store_true',
                        help='연결 테스트 후 종료')
    parser.add_argument('--stats', action='store_true',
                        help='임베딩 현황 통계 출력 후 종료')
    parser.add_argument('--backfill', action='store_true',
                        help='page_id NULL 청크 백필 후 종료')
    parser.add_argument('--search', type=str, default=None,
                        help='검색 쿼리 데모')
    parser.add_argument('--debug', action='store_true')
    return parser.parse_args()


async def main():
    args = parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    runner = EmbeddingRunner(kafka_servers=args.kafka_servers)

    if args.test_connection:
        ok = await runner.test_connection()
        sys.exit(0 if ok else 1)

    if args.stats:
        await runner.show_stats()
        return

    if args.search:
        await runner.demo_search(args.search)
        return

    if args.backfill:
        worker = EmbeddingWorker(bootstrap_servers=runner.kafka_servers)
        await worker.start()
        count = await worker.backfill_page_ids()
        await worker.stop()
        logger.info(f"Backfill complete: {count} rows updated")
        return

    await runner.run(max_messages=args.max_messages)


if __name__ == '__main__':
    asyncio.run(main())

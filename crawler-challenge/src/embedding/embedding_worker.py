"""
Embedding Worker - processed.final → 임베딩 → pgvector 저장
=============================================================

HybridStorage와 동일한 processed.final 토픽을 별도 consumer group으로
소비하여 Markdown 텍스트를 청킹/임베딩한 후 PostgreSQL page_chunks에 저장합니다.

Consumer Group: "crawler-stream-embedding"
입력 토픽:      processed.final
저장 대상:      PostgreSQL page_chunks (pgvector)

환경변수:
  EMBED_BACKEND      : "local" | "openai" (기본: local)
  EMBED_MODEL_NAME   : 모델명 (기본: all-MiniLM-L6-v2)
  EMBED_BATCH_SIZE   : Kafka 배치 크기 (기본: 32)
  EMBED_MAX_CHUNKS   : URL당 최대 청크 수 (기본: 20)
  EMBED_MAX_CHUNK_CHARS : 청크 최대 문자 수 (기본: 500)
  EMBED_MIN_CHUNK_CHARS : 청크 최소 문자 수 (기본: 50)
  EMBED_SKIP_SHORT   : 최소 마크다운 길이 필터 (기본: 200)
"""

import os
import time
import asyncio
import logging
from typing import Optional, TYPE_CHECKING, Any

import msgpack
from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaError

from src.common.kafka_config import get_config
from src.embedding.chunker import MarkdownChunker, Chunk
from src.embedding.embedder import BaseEmbedder, create_embedder
from src.embedding.stats import EmbeddingStats

if TYPE_CHECKING:
    import asyncpg

logger = logging.getLogger(__name__)

# PostgreSQL UPSERT 쿼리
UPSERT_CHUNKS_QUERY = """
    INSERT INTO page_chunks (url, chunk_index, chunk_text, heading_ctx, embedding)
    VALUES ($1, $2, $3, $4, $5::vector)
    ON CONFLICT (url, chunk_index) DO UPDATE SET
        chunk_text  = EXCLUDED.chunk_text,
        heading_ctx = EXCLUDED.heading_ctx,
        embedding   = EXCLUDED.embedding
"""

# page_id 역방향 채우기 (pages 테이블이 이미 저장된 경우)
UPDATE_PAGE_ID_QUERY = """
    UPDATE page_chunks c
    SET page_id = p.id
    FROM pages p
    WHERE c.url = p.url
      AND c.page_id IS NULL
"""


class EmbeddingWorker:
    """
    processed.final Kafka 토픽 소비 → 임베딩 생성 → pgvector 저장

    설계 원칙:
    - HybridStorage와 독립적으로 동작 (별도 consumer group)
    - 임베딩 실패가 기존 저장 파이프라인에 영향 없음
    - 배치 처리로 임베딩 모델 효율 최대화
    """

    BATCH_SIZE = int(os.getenv("EMBED_BATCH_SIZE", "32"))
    MAX_CHUNKS = int(os.getenv("EMBED_MAX_CHUNKS", "20"))
    MAX_CHUNK_CHARS = int(os.getenv("EMBED_MAX_CHUNK_CHARS", "500"))
    MIN_CHUNK_CHARS = int(os.getenv("EMBED_MIN_CHUNK_CHARS", "50"))
    SKIP_SHORT_MARKDOWN = int(os.getenv("EMBED_SKIP_SHORT", "200"))

    def __init__(
        self,
        bootstrap_servers: Optional[str] = None,
        embedder: Optional[BaseEmbedder] = None,
        pg_dsn: Optional[str] = None,
    ):
        """
        Args:
            bootstrap_servers: Kafka 브로커 주소
            embedder: 임베딩 모델 (None이면 환경변수로 자동 선택)
            pg_dsn: PostgreSQL DSN (None이면 환경변수로 자동 설정)
        """
        config = get_config()
        self.bootstrap_servers = bootstrap_servers or config.kafka.bootstrap_servers
        self.pg_dsn = pg_dsn or config.postgres.dsn

        # 임베딩 모델 (지연 초기화 가능)
        self._embedder: Optional[BaseEmbedder] = embedder
        self._embedder_provided = embedder is not None

        # 청킹
        self._chunker = MarkdownChunker(
            max_chars=self.MAX_CHUNK_CHARS,
            min_chars=self.MIN_CHUNK_CHARS,
        )

        # Kafka/DB 클라이언트
        self._consumer: Optional[AIOKafkaConsumer] = None
        self._pg_pool: Optional[Any] = None
        self._running = False

        self.stats = EmbeddingStats()

        logger.info(
            f"EmbeddingWorker initialized: "
            f"batch_size={self.BATCH_SIZE}, "
            f"max_chunks={self.MAX_CHUNKS}"
        )

    async def start(self) -> None:
        """워커 시작 (Kafka consumer + DB pool + 임베딩 모델 초기화)"""
        config = get_config()

        # 임베딩 모델 초기화 (외부에서 주입 안 된 경우)
        if self._embedder is None:
            logger.info("Initializing embedder...")
            self._embedder = create_embedder()
            logger.info(
                f"Embedder ready: {self._embedder.model_name}, "
                f"dim={self._embedder.dimension}"
            )

        # PostgreSQL 연결 풀
        logger.info("Connecting to PostgreSQL...")
        import asyncpg

        self._pg_pool = await asyncpg.create_pool(
            dsn=self.pg_dsn,
            min_size=2,
            max_size=5,
            command_timeout=30,
        )
        logger.info("PostgreSQL pool ready")

        # Kafka Consumer
        self._consumer = AIOKafkaConsumer(
            config.topics.processed_final,
            bootstrap_servers=self.bootstrap_servers,
            group_id=f"{config.consumer.group_id_prefix}-embedding",
            value_deserializer=lambda m: msgpack.unpackb(m, raw=False),
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            max_poll_records=self.BATCH_SIZE,
            session_timeout_ms=60000,   # 임베딩이 오래 걸릴 수 있어 넉넉하게
            heartbeat_interval_ms=20000,
        )
        await self._consumer.start()
        self._running = True

        self.stats = EmbeddingStats()
        logger.info("EmbeddingWorker started")

    async def stop(self) -> None:
        """워커 중지"""
        self._running = False

        if self._consumer:
            await self._consumer.stop()
            self._consumer = None

        if self._pg_pool:
            await self._pg_pool.close()
            self._pg_pool = None

        logger.info(f"EmbeddingWorker stopped. {self.stats}")

    async def __aenter__(self) -> "EmbeddingWorker":
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.stop()

    async def run(self, max_messages: Optional[int] = None) -> None:
        """
        메인 처리 루프

        Args:
            max_messages: 처리할 최대 메시지 수 (None이면 무한 루프)
        """
        if not self._running:
            raise RuntimeError("Worker not started. Call start() first.")

        logger.info(
            f"EmbeddingWorker run loop started "
            f"(max_messages={max_messages or 'unlimited'})"
        )

        uncommitted = 0
        last_commit_time = time.time()
        COMMIT_INTERVAL = 10.0
        COMMIT_BATCH = self.BATCH_SIZE * 2

        message_buffer: list[dict] = []

        try:
            async for message in self._consumer:
                if not self._running:
                    break

                message_buffer.append(message.value)
                self.stats.messages_consumed += 1

                # 배치가 찼거나 타임아웃 시 처리
                now = time.time()
                buffer_full = len(message_buffer) >= self.BATCH_SIZE
                timeout = (now - last_commit_time) >= COMMIT_INTERVAL

                if buffer_full or timeout:
                    await self._process_batch(message_buffer)
                    message_buffer.clear()

                    uncommitted += 1
                    if uncommitted >= COMMIT_BATCH or timeout:
                        await self._consumer.commit()
                        uncommitted = 0
                        # _process_batch() 실행 후 실제 시간으로 갱신 (stale now 방지)
                        last_commit_time = time.time()
                        logger.info(f"EmbeddingWorker: {self.stats}")

                if max_messages and self.stats.messages_consumed >= max_messages:
                    logger.info(f"Reached max_messages={max_messages}")
                    break

        except asyncio.CancelledError:
            logger.info("EmbeddingWorker loop cancelled")
        except Exception as e:
            logger.error(f"Error in run loop: {e}", exc_info=True)
        finally:
            # 남은 버퍼 처리
            if message_buffer:
                await self._process_batch(message_buffer)

            if uncommitted > 0 and self._consumer:
                try:
                    await self._consumer.commit()
                except Exception:
                    pass

            logger.info(f"EmbeddingWorker run loop ended. {self.stats}")

    async def _process_batch(self, messages: list[dict]) -> None:
        """
        메시지 배치 처리:
        1. 유효한 메시지 필터링
        2. 청킹
        3. 배치 임베딩 (executor에서 실행)
        4. pgvector 저장
        """
        if not messages:
            return

        # 1. 유효성 필터 (success=True, markdown 있음, 최소 길이)
        valid_items: list[tuple[str, str]] = []  # (url, markdown)
        for msg in messages:
            if not msg.get('success'):
                self.stats.messages_skipped += 1
                continue
            url = msg.get('url', '')
            markdown = msg.get('markdown') or ''
            if not url or len(markdown) < self.SKIP_SHORT_MARKDOWN:
                self.stats.messages_skipped += 1
                continue
            valid_items.append((url, markdown))

        if not valid_items:
            return

        # 2. 청킹
        all_chunks: list[tuple[str, Chunk]] = []  # (url, Chunk)
        for url, markdown in valid_items:
            chunks = self._chunker.split(markdown)
            chunks = chunks[:self.MAX_CHUNKS]  # URL당 최대 청크 수 제한
            for chunk in chunks:
                all_chunks.append((url, chunk))

        if not all_chunks:
            return

        self.stats.chunks_created += len(all_chunks)

        # 3. 배치 임베딩 (I/O 블로킹 → executor)
        texts = [chunk.text for _, chunk in all_chunks]
        embed_start = time.time()
        try:
            loop = asyncio.get_running_loop()
            embeddings: list[list[float]] = await loop.run_in_executor(
                None,
                self._embedder.embed_batch,
                texts,
            )
        except Exception as e:
            logger.error(f"Embedding error: {e}", exc_info=True)
            self.stats.messages_failed += len(valid_items)
            return

        self.stats.embed_time_ms += (time.time() - embed_start) * 1000

        # 4. pgvector 저장
        store_start = time.time()
        stored = await self._store_chunks(all_chunks, embeddings)
        self.stats.store_time_ms += (time.time() - store_start) * 1000
        self.stats.chunks_stored += stored

    async def _store_chunks(
        self,
        all_chunks: list[tuple[str, Chunk]],
        embeddings: list[list[float]],
    ) -> int:
        """
        청크와 임베딩 벡터를 page_chunks 테이블에 UPSERT

        Returns:
            저장 성공한 청크 수
        """
        if not self._pg_pool:
            logger.error("PostgreSQL pool not available")
            return 0

        records = [
            (
                url,
                chunk.chunk_index,
                chunk.text,
                chunk.heading_ctx or None,
                # pgvector text input: '[f1,f2,...]' 형식으로 명시적 변환
                # Python str(list)는 공백 포함 → 명시적 포맷으로 안전성 확보
                f"[{','.join(str(x) for x in embedding)}]",
            )
            for (url, chunk), embedding in zip(all_chunks, embeddings)
        ]

        try:
            async with self._pg_pool.acquire() as conn:
                await conn.executemany(UPSERT_CHUNKS_QUERY, records)

            logger.debug(f"Stored {len(records)} chunks to page_chunks")
            return len(records)

        except Exception as e:
            logger.error(f"Error storing chunks: {e}", exc_info=True)
            return 0

    async def backfill_page_ids(self) -> int:
        """
        page_id가 NULL인 청크에 pages.id를 역방향으로 채우는 유지보수 작업

        HybridStorage보다 EmbeddingWorker가 먼저 처리된 경우 page_id가 NULL이 될 수 있음.
        주기적으로 또는 수동으로 호출.

        Returns:
            업데이트된 행 수
        """
        if not self._pg_pool:
            return 0
        try:
            async with self._pg_pool.acquire() as conn:
                result = await conn.execute(UPDATE_PAGE_ID_QUERY)
                count = int(result.split()[-1])
                logger.info(f"backfill_page_ids: updated {count} rows")
                return count
        except Exception as e:
            logger.error(f"Error in backfill_page_ids: {e}")
            return 0

    def get_stats(self) -> dict:
        """현재 통계 반환"""
        avg_embed = (
            self.stats.embed_time_ms / max(self.stats.messages_processed, 1)
        )
        avg_store = (
            self.stats.store_time_ms / max(self.stats.messages_processed, 1)
        )
        return {
            'messages_consumed': self.stats.messages_consumed,
            'messages_processed': self.stats.messages_processed,
            'messages_skipped': self.stats.messages_skipped,
            'messages_failed': self.stats.messages_failed,
            'chunks_created': self.stats.chunks_created,
            'chunks_stored': self.stats.chunks_stored,
            'avg_embed_ms': round(avg_embed, 1),
            'avg_store_ms': round(avg_store, 1),
            'messages_per_second': round(self.stats.mps, 2),
            'model': self._embedder.model_name if self._embedder else 'not_loaded',
        }

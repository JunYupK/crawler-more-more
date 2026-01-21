"""
Hybrid Storage - Unified Storage Interface
==========================================

PostgreSQL + MinIO를 통합한 하이브리드 스토리지
- Kafka Consumer로 processed.final 토픽 소비
- MinIO에 콘텐츠 저장
- PostgreSQL에 메타데이터 저장
"""

import asyncio
import time
import logging
from typing import Optional, Any, Callable
from dataclasses import dataclass, field

import msgpack
from aiokafka import AIOKafkaConsumer

import sys
sys.path.insert(0, '/home/user/crawler-more-more/crawler-challenge')
from config.kafka_config import get_config
from src.storage.minio_writer import MinIOWriter, StoredObject
from src.storage.postgres_writer import PostgresWriter, PageRecord

logger = logging.getLogger(__name__)


@dataclass
class StorageResult:
    """저장 결과"""
    url: str
    success: bool
    minio_markdown: Optional[StoredObject] = None
    minio_html: Optional[StoredObject] = None
    postgres_saved: bool = False
    error: Optional[str] = None
    processing_time_ms: float = 0.0

    def to_dict(self) -> dict:
        return {
            'url': self.url,
            'success': self.success,
            'minio': {
                'markdown_key': self.minio_markdown.key if self.minio_markdown else None,
                'html_key': self.minio_html.key if self.minio_html else None,
            },
            'postgres_saved': self.postgres_saved,
            'error': self.error,
            'processing_time_ms': self.processing_time_ms,
        }


@dataclass
class HybridStorageStats:
    """하이브리드 스토리지 통계"""
    messages_consumed: int = 0
    messages_stored: int = 0
    messages_failed: int = 0
    total_processing_time_ms: float = 0.0
    start_time: float = field(default_factory=time.time)

    @property
    def success_rate(self) -> float:
        total = self.messages_stored + self.messages_failed
        return self.messages_stored / total if total > 0 else 0

    @property
    def messages_per_second(self) -> float:
        elapsed = time.time() - self.start_time
        return self.messages_consumed / elapsed if elapsed > 0 else 0

    @property
    def average_processing_time_ms(self) -> float:
        return self.total_processing_time_ms / self.messages_stored if self.messages_stored > 0 else 0

    def __str__(self) -> str:
        return (
            f"HybridStorageStats("
            f"consumed={self.messages_consumed:,}, "
            f"stored={self.messages_stored:,}, "
            f"failed={self.messages_failed:,}, "
            f"rate={self.success_rate:.1%}, "
            f"mps={self.messages_per_second:.1f})"
        )


class HybridStorage:
    """
    하이브리드 스토리지

    processed.final 토픽에서 처리 완료된 데이터를 소비하여
    MinIO(콘텐츠)와 PostgreSQL(메타데이터)에 분산 저장

    데이터 흐름:
    1. Kafka에서 ProcessedResult 소비
    2. MinIO에 Markdown 저장 → 키 획득
    3. PostgreSQL에 메타데이터 + MinIO 키 저장
    """

    def __init__(
        self,
        bootstrap_servers: Optional[str] = None,
        group_id: Optional[str] = None,
        store_raw_html: bool = False,
    ):
        """
        Args:
            bootstrap_servers: Kafka 브로커 주소
            group_id: Consumer 그룹 ID
            store_raw_html: Raw HTML도 저장할지 여부
        """
        config = get_config()

        self.bootstrap_servers = bootstrap_servers or config.kafka.bootstrap_servers
        self.group_id = group_id or f"{config.consumer.group_id_prefix}-storage"
        self.topics = config.topics
        self.store_raw_html = store_raw_html

        # 스토리지 컴포넌트
        self.minio = MinIOWriter()
        self.postgres = PostgresWriter()

        # Kafka Consumer
        self._consumer: Optional[AIOKafkaConsumer] = None
        self._running = False

        # 통계
        self.stats = HybridStorageStats()

        logger.info(
            f"HybridStorage initialized: "
            f"bootstrap={self.bootstrap_servers}, "
            f"group={self.group_id}"
        )

    async def start(self) -> None:
        """스토리지 시작"""
        if self._running:
            logger.warning("Storage already running")
            return

        try:
            # MinIO 버킷 초기화
            await self.minio.initialize_buckets()

            # PostgreSQL 연결
            await self.postgres.start()

            # Kafka Consumer
            self._consumer = AIOKafkaConsumer(
                self.topics.processed_final,
                bootstrap_servers=self.bootstrap_servers,
                group_id=self.group_id,
                value_deserializer=lambda m: msgpack.unpackb(m, raw=False),
                auto_offset_reset='earliest',
                enable_auto_commit=False,
                max_poll_records=50,
                session_timeout_ms=30000,
            )
            await self._consumer.start()

            self._running = True
            self.stats = HybridStorageStats()

            logger.info("HybridStorage started")

        except Exception as e:
            logger.error(f"Failed to start HybridStorage: {e}")
            await self.stop()
            raise

    async def stop(self) -> None:
        """스토리지 중지"""
        self._running = False

        if self._consumer:
            await self._consumer.stop()
            self._consumer = None

        await self.postgres.stop()

        logger.info(f"HybridStorage stopped. {self.stats}")

    async def __aenter__(self) -> "HybridStorage":
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.stop()

    async def run(
        self,
        max_messages: Optional[int] = None,
        callback: Optional[Callable[[StorageResult], Any]] = None,
    ) -> None:
        """
        스토리지 실행 (메인 루프)

        Args:
            max_messages: 최대 처리 메시지 수
            callback: 저장 완료 콜백
        """
        if not self._running:
            raise RuntimeError("Storage not started")

        logger.info("Starting storage loop...")
        processed = 0

        try:
            async for message in self._consumer:
                if not self._running:
                    break

                try:
                    # 메시지 저장
                    result = await self._store_message(message.value)

                    if result:
                        if callback:
                            callback(result)

                    # 오프셋 커밋
                    await self._consumer.commit()

                    processed += 1

                    # 진행 상황 로깅
                    if processed % 100 == 0:
                        logger.info(f"Storage progress: {self.stats}")

                    # 최대 처리 수 확인
                    if max_messages and processed >= max_messages:
                        logger.info(f"Reached max messages: {max_messages}")
                        break

                except Exception as e:
                    logger.error(f"Error processing message: {e}", exc_info=True)
                    self.stats.messages_failed += 1

        except asyncio.CancelledError:
            logger.info("Storage loop cancelled")
        except Exception as e:
            logger.error(f"Error in storage loop: {e}", exc_info=True)
        finally:
            logger.info(f"Storage loop ended. Processed: {processed}")

    async def _store_message(self, message: dict) -> Optional[StorageResult]:
        """메시지 저장"""
        start_time = time.time()
        self.stats.messages_consumed += 1

        url = message.get('url', '')
        success = message.get('success', False)

        # 실패한 처리 결과는 건너뛰기
        if not success:
            logger.debug(f"Skipping failed result: {url}")
            return None

        try:
            minio_markdown = None
            minio_html = None

            # 1. MinIO에 Markdown 저장
            markdown = message.get('markdown')
            if markdown:
                minio_markdown = await self.minio.store_markdown(
                    url=url,
                    markdown=markdown,
                    metadata={
                        'processor': message.get('processor_type', 'unknown'),
                        'title': (message.get('metadata', {}).get('title') or '')[:100],
                    }
                )

            # 2. MinIO에 Raw HTML 저장 (옵션)
            if self.store_raw_html:
                # processed.final에는 HTML이 없을 수 있음
                # 필요시 별도 토픽에서 가져와야 함
                pass

            # 3. PostgreSQL에 메타데이터 저장
            record = PageRecord.from_processed_result(
                message,
                minio_keys={
                    'markdown': minio_markdown.key if minio_markdown else None,
                    'raw_html': minio_html.key if minio_html else None,
                }
            )

            await self.postgres.save(record)

            # 처리 시간
            processing_time_ms = (time.time() - start_time) * 1000
            self.stats.messages_stored += 1
            self.stats.total_processing_time_ms += processing_time_ms

            result = StorageResult(
                url=url,
                success=True,
                minio_markdown=minio_markdown,
                minio_html=minio_html,
                postgres_saved=True,
                processing_time_ms=processing_time_ms,
            )

            logger.debug(f"Stored: {url} in {processing_time_ms:.1f}ms")
            return result

        except Exception as e:
            self.stats.messages_failed += 1
            logger.error(f"Error storing {url}: {e}")

            return StorageResult(
                url=url,
                success=False,
                error=str(e),
                processing_time_ms=(time.time() - start_time) * 1000,
            )

    async def store_single(self, processed_result: dict) -> StorageResult:
        """
        단일 결과 저장 (테스트용)

        Args:
            processed_result: ProcessedResult 딕셔너리

        Returns:
            StorageResult
        """
        return await self._store_message(processed_result)

    async def get_page_with_content(self, url: str) -> Optional[dict]:
        """
        페이지 전체 데이터 조회 (메타데이터 + 콘텐츠)

        Args:
            url: 조회할 URL

        Returns:
            페이지 데이터 (메타데이터 + markdown)
        """
        # PostgreSQL에서 메타데이터 조회
        page_data = await self.postgres.get_page(url)
        if not page_data:
            return None

        # MinIO에서 Markdown 조회
        markdown = await self.minio.get_markdown(url)
        if markdown:
            page_data['markdown'] = markdown

        return page_data

    def get_stats(self) -> dict:
        """전체 통계 반환"""
        return {
            'hybrid': {
                'messages_consumed': self.stats.messages_consumed,
                'messages_stored': self.stats.messages_stored,
                'messages_failed': self.stats.messages_failed,
                'success_rate': self.stats.success_rate,
                'messages_per_second': self.stats.messages_per_second,
                'average_processing_time_ms': self.stats.average_processing_time_ms,
            },
            'minio': self.minio.get_stats(),
            'postgres': self.postgres.get_stats(),
        }


class StorageWriter:
    """
    간단한 저장 클라이언트

    Kafka 없이 직접 저장할 때 사용
    """

    def __init__(self):
        self.minio = MinIOWriter()
        self.postgres = PostgresWriter()

    async def start(self) -> None:
        await self.minio.initialize_buckets()
        await self.postgres.start()

    async def stop(self) -> None:
        await self.postgres.stop()

    async def __aenter__(self) -> "StorageWriter":
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.stop()

    async def save(
        self,
        url: str,
        markdown: str,
        metadata: dict,
        processor: str = "fast",
        score: Optional[int] = None,
    ) -> StorageResult:
        """
        데이터 저장

        Args:
            url: 원본 URL
            markdown: Markdown 콘텐츠
            metadata: 메타데이터 (title, description 등)
            processor: 프로세서 유형
            score: 정적 점수

        Returns:
            StorageResult
        """
        start_time = time.time()

        try:
            # MinIO 저장
            minio_result = await self.minio.store_markdown(url, markdown)

            # PostgreSQL 저장
            from urllib.parse import urlparse
            parsed = urlparse(url)

            record = PageRecord(
                url=url,
                domain=parsed.netloc,
                title=metadata.get('title'),
                description=metadata.get('description'),
                language=metadata.get('language'),
                processor=processor,
                static_score=score,
                markdown_key=minio_result.key if minio_result else None,
                markdown_size_bytes=len(markdown),
            )

            await self.postgres.save(record)
            await self.postgres.flush()

            return StorageResult(
                url=url,
                success=True,
                minio_markdown=minio_result,
                postgres_saved=True,
                processing_time_ms=(time.time() - start_time) * 1000,
            )

        except Exception as e:
            return StorageResult(
                url=url,
                success=False,
                error=str(e),
                processing_time_ms=(time.time() - start_time) * 1000,
            )


async def test_storage_connections() -> dict:
    """스토리지 연결 테스트"""
    from src.storage.minio_writer import test_minio_connection
    from src.storage.postgres_writer import test_postgres_connection

    minio_result = await test_minio_connection()
    postgres_result = await test_postgres_connection()

    return {
        'minio': minio_result,
        'postgres': postgres_result,
        'all_connected': minio_result['connected'] and postgres_result['connected'],
    }

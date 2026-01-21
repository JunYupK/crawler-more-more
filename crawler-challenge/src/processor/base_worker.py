"""
Base Worker - 공통 워커 인터페이스
==================================

Fast Worker와 Rich Worker의 공통 기반 클래스
- Kafka Consumer/Producer 통합
- 처리 결과 표준화
- 통계 수집
"""

import asyncio
import time
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Optional, Any, Callable
from enum import Enum

import msgpack
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.errors import KafkaError

import sys
sys.path.insert(0, '/home/user/crawler-more-more/crawler-challenge')
from config.kafka_config import get_config, TopicConfig

logger = logging.getLogger(__name__)


class ProcessorType(Enum):
    """프로세서 유형"""
    FAST = "fast"   # BeautifulSoup
    RICH = "rich"   # Crawl4AI


@dataclass
class ProcessedResult:
    """처리 결과"""
    url: str
    success: bool
    processor_type: ProcessorType

    # 콘텐츠
    markdown: Optional[str] = None
    plain_text: Optional[str] = None

    # 메타데이터
    title: Optional[str] = None
    description: Optional[str] = None
    language: Optional[str] = None
    keywords: Optional[list[str]] = None

    # 추출된 데이터
    links: Optional[list[str]] = None
    images: Optional[list[str]] = None
    headings: Optional[list[str]] = None

    # 처리 정보
    processing_time_ms: float = 0.0
    original_score: int = 0
    content_length: int = 0
    markdown_length: int = 0

    # 에러 (실패 시)
    error_type: Optional[str] = None
    error_message: Optional[str] = None

    # 타임스탬프
    original_timestamp: float = 0.0
    router_timestamp: float = 0.0
    processor_timestamp: float = field(default_factory=time.time)

    def to_dict(self) -> dict:
        """딕셔너리로 변환"""
        return {
            'url': self.url,
            'success': self.success,
            'processor_type': self.processor_type.value,
            'markdown': self.markdown,
            'plain_text': self.plain_text,
            'metadata': {
                'title': self.title,
                'description': self.description,
                'language': self.language,
                'keywords': self.keywords,
            },
            'extracted': {
                'links': self.links,
                'images': self.images,
                'headings': self.headings,
            },
            'stats': {
                'processing_time_ms': self.processing_time_ms,
                'original_score': self.original_score,
                'content_length': self.content_length,
                'markdown_length': self.markdown_length,
            },
            'timestamps': {
                'original': self.original_timestamp,
                'router': self.router_timestamp,
                'processor': self.processor_timestamp,
            },
            'error': {
                'type': self.error_type,
                'message': self.error_message,
            } if not self.success else None,
        }


@dataclass
class WorkerStats:
    """워커 통계"""
    messages_consumed: int = 0
    messages_processed: int = 0
    messages_failed: int = 0
    total_processing_time_ms: float = 0.0
    total_content_bytes: int = 0
    total_markdown_bytes: int = 0
    start_time: float = field(default_factory=time.time)

    @property
    def success_rate(self) -> float:
        total = self.messages_processed + self.messages_failed
        return self.messages_processed / total if total > 0 else 0

    @property
    def messages_per_second(self) -> float:
        elapsed = time.time() - self.start_time
        return self.messages_consumed / elapsed if elapsed > 0 else 0

    @property
    def average_processing_time_ms(self) -> float:
        return self.total_processing_time_ms / self.messages_processed if self.messages_processed > 0 else 0

    def record_success(self, processing_time_ms: float, content_bytes: int, markdown_bytes: int):
        self.messages_consumed += 1
        self.messages_processed += 1
        self.total_processing_time_ms += processing_time_ms
        self.total_content_bytes += content_bytes
        self.total_markdown_bytes += markdown_bytes

    def record_failure(self):
        self.messages_consumed += 1
        self.messages_failed += 1

    def __str__(self) -> str:
        return (
            f"WorkerStats("
            f"processed={self.messages_processed:,}, "
            f"failed={self.messages_failed:,}, "
            f"rate={self.success_rate:.1%}, "
            f"mps={self.messages_per_second:.1f}, "
            f"avg_time={self.average_processing_time_ms:.0f}ms)"
        )


class BaseWorker(ABC):
    """
    기본 워커 추상 클래스

    서브클래스에서 구현해야 할 메서드:
    - process_html(): HTML을 처리하여 결과 반환
    - get_processor_type(): 프로세서 유형 반환
    """

    def __init__(
        self,
        source_topic: str,
        bootstrap_servers: Optional[str] = None,
        group_id: Optional[str] = None,
        worker_id: int = 0,
    ):
        """
        Args:
            source_topic: 소스 Kafka 토픽 (process.fast or process.rich)
            bootstrap_servers: Kafka 브로커 주소
            group_id: Consumer 그룹 ID
            worker_id: 워커 식별자
        """
        config = get_config()

        self.source_topic = source_topic
        self.bootstrap_servers = bootstrap_servers or config.kafka.bootstrap_servers
        self.group_id = group_id or f"{config.consumer.group_id_prefix}-{self.get_processor_type().value}"
        self.worker_id = worker_id
        self.topics = config.topics

        # Kafka 클라이언트
        self._consumer: Optional[AIOKafkaConsumer] = None
        self._producer: Optional[AIOKafkaProducer] = None
        self._running = False

        # 통계
        self.stats = WorkerStats()

        logger.info(
            f"{self.__class__.__name__} initialized: "
            f"topic={source_topic}, "
            f"group={self.group_id}, "
            f"worker_id={worker_id}"
        )

    @abstractmethod
    def get_processor_type(self) -> ProcessorType:
        """프로세서 유형 반환"""
        pass

    @abstractmethod
    async def process_html(self, html: str, url: str, metadata: dict) -> ProcessedResult:
        """
        HTML 처리 (서브클래스에서 구현)

        Args:
            html: HTML 콘텐츠
            url: 페이지 URL
            metadata: 라우터에서 전달된 메타데이터

        Returns:
            ProcessedResult 객체
        """
        pass

    async def start(self) -> None:
        """워커 시작"""
        if self._running:
            logger.warning("Worker already running")
            return

        try:
            # Consumer 생성
            self._consumer = AIOKafkaConsumer(
                self.source_topic,
                bootstrap_servers=self.bootstrap_servers,
                group_id=self.group_id,
                value_deserializer=lambda m: msgpack.unpackb(m, raw=False),
                auto_offset_reset='earliest',
                enable_auto_commit=False,
                max_poll_records=10,  # 배치 크기 제한
                session_timeout_ms=30000,
                heartbeat_interval_ms=10000,
            )

            # Producer 생성
            self._producer = AIOKafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: msgpack.packb(v, use_bin_type=True),
                compression_type='lz4',
                linger_ms=20,
            )

            await self._consumer.start()
            await self._producer.start()

            self._running = True
            self.stats = WorkerStats()

            logger.info(f"{self.__class__.__name__} started")

        except Exception as e:
            logger.error(f"Failed to start worker: {e}")
            await self.stop()
            raise

    async def stop(self) -> None:
        """워커 중지"""
        self._running = False

        if self._consumer:
            await self._consumer.stop()
            self._consumer = None

        if self._producer:
            await self._producer.flush()
            await self._producer.stop()
            self._producer = None

        logger.info(f"{self.__class__.__name__} stopped. {self.stats}")

    async def __aenter__(self) -> "BaseWorker":
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.stop()

    async def run(
        self,
        max_messages: Optional[int] = None,
        callback: Optional[Callable[[ProcessedResult], Any]] = None,
    ) -> None:
        """
        워커 실행 (메인 루프)

        Args:
            max_messages: 최대 처리 메시지 수
            callback: 처리 완료 콜백
        """
        if not self._running:
            raise RuntimeError("Worker not started")

        logger.info(f"Starting processing loop (worker_id={self.worker_id})...")
        processed = 0

        try:
            async for message in self._consumer:
                if not self._running:
                    break

                try:
                    # 메시지 처리
                    result = await self._process_message(message.value)

                    if result:
                        # 결과 전송
                        await self._send_result(result)

                        # 콜백 호출
                        if callback:
                            callback(result)

                    # 오프셋 커밋
                    await self._consumer.commit()

                    processed += 1

                    # 진행 상황 로깅
                    if processed % 50 == 0:
                        logger.info(f"Worker {self.worker_id}: {self.stats}")

                    # 최대 처리 수 확인
                    if max_messages and processed >= max_messages:
                        logger.info(f"Reached max messages: {max_messages}")
                        break

                except Exception as e:
                    logger.error(f"Error processing message: {e}", exc_info=True)
                    self.stats.record_failure()

        except asyncio.CancelledError:
            logger.info("Worker loop cancelled")
        except Exception as e:
            logger.error(f"Error in worker loop: {e}", exc_info=True)
        finally:
            logger.info(f"Processing loop ended. Processed: {processed}")

    async def _process_message(self, message: dict) -> Optional[ProcessedResult]:
        """메시지 처리"""
        start_time = time.time()

        try:
            url = message.get('url', '')
            html = message.get('html', '')
            score = message.get('score', 0)
            original_timestamp = message.get('original_timestamp', 0)
            router_timestamp = message.get('router_timestamp', 0)
            metadata = message.get('metadata', {})
            analysis = message.get('analysis', {})

            if not html:
                logger.warning(f"Empty HTML for {url}")
                return None

            # HTML 처리 (서브클래스 구현)
            result = await self.process_html(html, url, {
                **metadata,
                'analysis': analysis,
            })

            # 타임스탬프 설정
            result.original_timestamp = original_timestamp
            result.router_timestamp = router_timestamp
            result.original_score = score

            # 처리 시간
            processing_time_ms = (time.time() - start_time) * 1000
            result.processing_time_ms = processing_time_ms

            # 통계 기록
            if result.success:
                self.stats.record_success(
                    processing_time_ms,
                    len(html),
                    len(result.markdown or ''),
                )
            else:
                self.stats.record_failure()

            return result

        except Exception as e:
            logger.error(f"Error processing HTML: {e}")
            self.stats.record_failure()

            return ProcessedResult(
                url=message.get('url', ''),
                success=False,
                processor_type=self.get_processor_type(),
                error_type=type(e).__name__,
                error_message=str(e),
            )

    async def _send_result(self, result: ProcessedResult) -> bool:
        """결과를 Kafka로 전송"""
        if result.success:
            target_topic = self.topics.processed_final
        else:
            target_topic = self.topics.processed_dlq

        try:
            await self._producer.send_and_wait(
                topic=target_topic,
                value=result.to_dict(),
                key=result.url.encode('utf-8'),
            )

            logger.debug(f"Sent result to {target_topic}: {result.url}")
            return True

        except KafkaError as e:
            logger.error(f"Failed to send result: {e}")
            return False

    def get_stats(self) -> dict:
        """현재 통계 반환"""
        return {
            'worker_id': self.worker_id,
            'processor_type': self.get_processor_type().value,
            'consumed': self.stats.messages_consumed,
            'processed': self.stats.messages_processed,
            'failed': self.stats.messages_failed,
            'success_rate': self.stats.success_rate,
            'messages_per_second': self.stats.messages_per_second,
            'average_processing_time_ms': self.stats.average_processing_time_ms,
        }

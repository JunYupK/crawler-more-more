"""
Kafka Producer for Stream Pipeline
===================================

크롤링된 페이지를 Kafka로 전송하는 비동기 프로듀서
- aiokafka 기반 비동기 전송
- msgpack 직렬화 (JSON보다 빠르고 작음)
- 배치 전송으로 처리량 최적화
"""

import asyncio
import time
import logging
from typing import Any, Optional
from dataclasses import dataclass, field
from contextlib import asynccontextmanager

import msgpack
from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError, KafkaConnectionError

from src.common.kafka_config import get_config, KafkaConfig, ProducerConfig, TopicConfig

logger = logging.getLogger(__name__)


@dataclass
class ProducerStats:
    """프로듀서 통계"""
    messages_sent: int = 0
    messages_failed: int = 0
    bytes_sent: int = 0
    start_time: float = field(default_factory=time.time)

    @property
    def messages_per_second(self) -> float:
        elapsed = time.time() - self.start_time
        return self.messages_sent / elapsed if elapsed > 0 else 0

    @property
    def success_rate(self) -> float:
        total = self.messages_sent + self.messages_failed
        return self.messages_sent / total if total > 0 else 0

    def __str__(self) -> str:
        return (
            f"ProducerStats(sent={self.messages_sent:,}, "
            f"failed={self.messages_failed:,}, "
            f"bytes={self.bytes_sent:,}, "
            f"rate={self.messages_per_second:.1f}/s, "
            f"success={self.success_rate:.1%})"
        )


class KafkaPageProducer:
    """
    크롤링된 페이지를 Kafka로 전송하는 프로듀서

    특징:
    - 비동기 전송 (aiokafka)
    - msgpack 직렬화
    - 자동 재연결
    - 배치 전송 최적화
    """

    def __init__(
        self,
        bootstrap_servers: Optional[str] = None,
        producer_config: Optional[ProducerConfig] = None,
    ):
        """
        Args:
            bootstrap_servers: Kafka 브로커 주소 (예: "localhost:9092")
            producer_config: Producer 설정 (없으면 기본값 사용)
        """
        config = get_config()

        self.bootstrap_servers = bootstrap_servers or config.kafka.bootstrap_servers
        self.producer_config = producer_config or config.producer
        self.topics = config.topics

        self._producer: Optional[AIOKafkaProducer] = None
        self._started = False
        self.stats = ProducerStats()

        logger.info(f"KafkaPageProducer initialized: {self.bootstrap_servers}")

    async def start(self) -> None:
        """프로듀서 시작"""
        if self._started:
            logger.warning("Producer already started")
            return

        try:
            self._producer = AIOKafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                # 직렬화
                value_serializer=self._serialize,
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                # 배치 설정
                linger_ms=self.producer_config.linger_ms,
                max_batch_size=self.producer_config.batch_size,  # aiokafka uses max_batch_size
                # 압축
                compression_type=self.producer_config.compression_type,
                # 신뢰성
                acks=self.producer_config.acks,
                retries=self.producer_config.retries,
                # 타임아웃
                request_timeout_ms=self.producer_config.request_timeout_ms,
                # 메모리
                max_request_size=10485760,  # 10MB
            )

            await self._producer.start()
            self._started = True
            self.stats = ProducerStats()

            logger.info("Kafka producer started successfully")

        except KafkaConnectionError as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            raise
        except Exception as e:
            logger.error(f"Failed to start producer: {e}")
            raise

    async def stop(self) -> None:
        """프로듀서 중지"""
        if self._producer and self._started:
            try:
                # 버퍼에 남은 메시지 전송
                await self._producer.flush()
                await self._producer.stop()
                self._started = False
                logger.info(f"Kafka producer stopped. {self.stats}")
            except Exception as e:
                logger.error(f"Error stopping producer: {e}")

    async def __aenter__(self) -> "KafkaPageProducer":
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.stop()

    def _serialize(self, value: Any) -> bytes:
        """msgpack으로 직렬화"""
        return msgpack.packb(value, use_bin_type=True)

    async def send_raw_page(
        self,
        url: str,
        html_compressed: bytes,
        status_code: int,
        headers: dict,
        crawl_time_ms: float,
        metadata: Optional[dict] = None,
    ) -> bool:
        """
        크롤링된 원본 페이지를 raw.page 토픽으로 전송

        Args:
            url: 크롤링한 URL
            html_compressed: Zstd 압축된 HTML
            status_code: HTTP 상태 코드
            headers: HTTP 응답 헤더
            crawl_time_ms: 크롤링 소요 시간 (ms)
            metadata: 추가 메타데이터

        Returns:
            성공 여부
        """
        message = {
            'url': url,
            'html_compressed': html_compressed,
            'status_code': status_code,
            'headers': dict(headers) if headers else {},
            'crawl_time_ms': crawl_time_ms,
            'timestamp': time.time(),
            'metadata': metadata or {},
        }

        return await self._send(
            topic=self.topics.raw_page,
            value=message,
            key=self._url_to_key(url),
        )

    async def send_to_dlq(
        self,
        url: str,
        error_type: str,
        error_message: str,
        metadata: Optional[dict] = None,
    ) -> bool:
        """
        실패한 크롤링을 DLQ로 전송

        Args:
            url: 실패한 URL
            error_type: 에러 유형 (timeout, connection_error 등)
            error_message: 에러 메시지
            metadata: 추가 메타데이터

        Returns:
            성공 여부
        """
        message = {
            'url': url,
            'error_type': error_type,
            'error_message': error_message,
            'timestamp': time.time(),
            'metadata': metadata or {},
        }

        return await self._send(
            topic=self.topics.raw_page_dlq,
            value=message,
            key=self._url_to_key(url),
        )

    async def send_metrics(
        self,
        metric_type: str,
        data: dict,
    ) -> bool:
        """
        메트릭 이벤트 전송

        Args:
            metric_type: 메트릭 유형 (throughput, latency 등)
            data: 메트릭 데이터

        Returns:
            성공 여부
        """
        message = {
            'metric_type': metric_type,
            'data': data,
            'timestamp': time.time(),
        }

        return await self._send(
            topic=self.topics.metrics_throughput,
            value=message,
        )

    async def _send(
        self,
        topic: str,
        value: dict,
        key: Optional[str] = None,
    ) -> bool:
        """
        내부 전송 메서드

        Args:
            topic: Kafka 토픽
            value: 전송할 데이터
            key: 파티션 키 (선택)

        Returns:
            성공 여부
        """
        if not self._started or not self._producer:
            logger.error("Producer not started")
            return False

        try:
            # 비동기 전송
            future = await self._producer.send_and_wait(
                topic=topic,
                value=value,
                key=key,
            )

            # 통계 업데이트 (serialize once, reuse for size)
            serialized = self._serialize(value)
            self.stats.messages_sent += 1
            self.stats.bytes_sent += len(serialized)

            logger.debug(
                f"Sent to {topic}: partition={future.partition}, "
                f"offset={future.offset}"
            )
            return True

        except KafkaError as e:
            self.stats.messages_failed += 1
            logger.error(f"Kafka error sending to {topic}: {e}")
            return False
        except Exception as e:
            self.stats.messages_failed += 1
            logger.error(f"Error sending to {topic}: {e}")
            return False

    async def send_batch(
        self,
        messages: list[dict],
        topic: Optional[str] = None,
    ) -> tuple[int, int]:
        """
        배치 전송

        Args:
            messages: 전송할 메시지 리스트
            topic: 대상 토픽 (기본값: raw.page)

        Returns:
            (성공 수, 실패 수) 튜플
        """
        topic = topic or self.topics.raw_page
        success = 0
        failed = 0

        # 병렬 전송
        tasks = [
            self._send(topic, msg, msg.get('url'))
            for msg in messages
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in results:
            if result is True:
                success += 1
            else:
                failed += 1

        return success, failed

    async def flush(self) -> None:
        """버퍼에 남은 메시지 강제 전송"""
        if self._producer:
            await self._producer.flush()

    @staticmethod
    def _url_to_key(url: str) -> str:
        """URL을 파티션 키로 변환 (도메인 기반)"""
        try:
            from urllib.parse import urlparse
            parsed = urlparse(url)
            return parsed.netloc or url
        except Exception:
            return url


@asynccontextmanager
async def create_producer(
    bootstrap_servers: Optional[str] = None,
) -> KafkaPageProducer:
    """
    컨텍스트 매니저로 프로듀서 생성

    Usage:
        async with create_producer() as producer:
            await producer.send_raw_page(...)
    """
    producer = KafkaPageProducer(bootstrap_servers=bootstrap_servers)
    try:
        await producer.start()
        yield producer
    finally:
        await producer.stop()

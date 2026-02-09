"""
High-Speed Ingestor - httpx Async Crawler
==========================================

Mac에서 실행될 고속 HTTP 크롤러
- httpx async로 대량 요청 (HTTP/2 지원)
- Zstd 압축으로 네트워크 대역폭 절약
- Kafka로 직접 produce (DB 미거침)
"""

import asyncio
import time
import logging
import random
from typing import Optional, AsyncGenerator
from dataclasses import dataclass, field
from enum import Enum
from urllib.parse import urlparse

import httpx

from src.common.compression import ZstdCompressor, CompressionStats
from src.common.kafka_config import get_config, IngestorConfig
from .kafka_producer import KafkaPageProducer

logger = logging.getLogger(__name__)


class CrawlErrorType(Enum):
    """크롤링 에러 유형"""
    TIMEOUT = "timeout"
    CONNECTION_ERROR = "connection_error"
    SSL_ERROR = "ssl_error"
    HTTP_ERROR = "http_error"
    DNS_ERROR = "dns_error"
    TOO_MANY_REDIRECTS = "too_many_redirects"
    READ_ERROR = "read_error"
    UNKNOWN = "unknown"


@dataclass
class CrawlResult:
    """크롤링 결과"""
    url: str
    success: bool
    status_code: Optional[int] = None
    html: Optional[bytes] = None
    html_compressed: Optional[bytes] = None
    headers: Optional[dict] = None
    crawl_time_ms: float = 0.0
    error_type: Optional[CrawlErrorType] = None
    error_message: Optional[str] = None
    redirect_url: Optional[str] = None


@dataclass
class IngestorStats:
    """Ingestor 통계"""
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    total_bytes_raw: int = 0
    total_bytes_compressed: int = 0
    total_crawl_time_ms: float = 0.0
    start_time: float = field(default_factory=time.time)

    # 에러 유형별 카운트
    errors_by_type: dict = field(default_factory=dict)

    @property
    def success_rate(self) -> float:
        return self.successful_requests / self.total_requests if self.total_requests > 0 else 0

    @property
    def requests_per_second(self) -> float:
        elapsed = time.time() - self.start_time
        return self.total_requests / elapsed if elapsed > 0 else 0

    @property
    def average_crawl_time_ms(self) -> float:
        return self.total_crawl_time_ms / self.successful_requests if self.successful_requests > 0 else 0

    @property
    def compression_ratio(self) -> float:
        return 1 - (self.total_bytes_compressed / self.total_bytes_raw) if self.total_bytes_raw > 0 else 0

    def record_success(self, crawl_time_ms: float, raw_bytes: int, compressed_bytes: int):
        self.total_requests += 1
        self.successful_requests += 1
        self.total_crawl_time_ms += crawl_time_ms
        self.total_bytes_raw += raw_bytes
        self.total_bytes_compressed += compressed_bytes

    def record_failure(self, error_type: CrawlErrorType):
        self.total_requests += 1
        self.failed_requests += 1
        self.errors_by_type[error_type.value] = self.errors_by_type.get(error_type.value, 0) + 1

    def __str__(self) -> str:
        return (
            f"IngestorStats("
            f"total={self.total_requests:,}, "
            f"success={self.successful_requests:,}, "
            f"failed={self.failed_requests:,}, "
            f"rate={self.success_rate:.1%}, "
            f"rps={self.requests_per_second:.1f}, "
            f"avg_time={self.average_crawl_time_ms:.0f}ms, "
            f"compression={self.compression_ratio:.1%})"
        )


class HighSpeedIngestor:
    """
    고속 HTTP 크롤러

    특징:
    - httpx AsyncClient (HTTP/2 지원)
    - 세마포어로 동시성 제어
    - Zstd 압축
    - Kafka 직접 전송
    """

    # User-Agent 로테이션
    USER_AGENTS = [
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    ]

    def __init__(
        self,
        kafka_producer: Optional[KafkaPageProducer] = None,
        config: Optional[IngestorConfig] = None,
    ):
        """
        Args:
            kafka_producer: Kafka 프로듀서 (없으면 나중에 설정)
            config: Ingestor 설정 (없으면 기본값 사용)
        """
        self.config = config or get_config().ingestor
        self.kafka_producer = kafka_producer

        # 압축기
        self.compressor = ZstdCompressor(level=self.config.compression_level)
        self.compression_stats = CompressionStats()

        # 동시성 제어
        self._semaphore: Optional[asyncio.Semaphore] = None
        self._client: Optional[httpx.AsyncClient] = None

        # 통계
        self.stats = IngestorStats()

        logger.info(
            f"HighSpeedIngestor initialized: "
            f"max_concurrent={self.config.max_concurrent_requests}, "
            f"timeout={self.config.request_timeout}s, "
            f"http2={self.config.use_http2}"
        )

    async def start(self) -> None:
        """Ingestor 시작"""
        self._semaphore = asyncio.Semaphore(self.config.max_concurrent_requests)

        # httpx 클라이언트 생성
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(
                connect=10.0,
                read=self.config.request_timeout,
                write=10.0,
                pool=5.0,
            ),
            limits=httpx.Limits(
                max_connections=self.config.max_concurrent_requests,
                max_keepalive_connections=self.config.max_keepalive_connections,
            ),
            follow_redirects=True,
            max_redirects=5,
            http2=self.config.use_http2,
        )

        self.stats = IngestorStats()
        logger.info("HighSpeedIngestor started")

    async def stop(self) -> None:
        """Ingestor 중지"""
        if self._client:
            await self._client.aclose()
            self._client = None

        logger.info(f"HighSpeedIngestor stopped. {self.stats}")
        logger.info(f"Compression: {self.compression_stats}")

    async def __aenter__(self) -> "HighSpeedIngestor":
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.stop()

    def set_kafka_producer(self, producer: KafkaPageProducer) -> None:
        """Kafka 프로듀서 설정"""
        self.kafka_producer = producer

    async def crawl(self, url: str) -> CrawlResult:
        """
        단일 URL 크롤링

        Args:
            url: 크롤링할 URL

        Returns:
            CrawlResult 객체
        """
        if not self._client or not self._semaphore:
            raise RuntimeError("Ingestor not started. Call start() first.")

        async with self._semaphore:
            return await self._fetch(url)

    async def _fetch(self, url: str) -> CrawlResult:
        """내부 fetch 메서드"""
        start_time = time.time()

        try:
            # 랜덤 User-Agent
            headers = {
                "User-Agent": random.choice(self.USER_AGENTS),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.5",
                "Accept-Encoding": "gzip, deflate, br",
                "Connection": "keep-alive",
            }

            response = await self._client.get(url, headers=headers)
            crawl_time_ms = (time.time() - start_time) * 1000

            # HTML 콘텐츠
            html = response.content
            html_compressed = self.compressor.compress(html)

            # 통계 기록
            self.stats.record_success(crawl_time_ms, len(html), len(html_compressed))
            self.compression_stats.record(len(html), len(html_compressed))

            return CrawlResult(
                url=url,
                success=True,
                status_code=response.status_code,
                html=html,
                html_compressed=html_compressed,
                headers=dict(response.headers),
                crawl_time_ms=crawl_time_ms,
                redirect_url=str(response.url) if str(response.url) != url else None,
            )

        except httpx.TimeoutException as e:
            self.stats.record_failure(CrawlErrorType.TIMEOUT)
            return CrawlResult(
                url=url,
                success=False,
                error_type=CrawlErrorType.TIMEOUT,
                error_message=str(e),
                crawl_time_ms=(time.time() - start_time) * 1000,
            )

        except httpx.ConnectError as e:
            error_type = CrawlErrorType.CONNECTION_ERROR
            if "SSL" in str(e) or "certificate" in str(e).lower():
                error_type = CrawlErrorType.SSL_ERROR
            elif "DNS" in str(e) or "getaddrinfo" in str(e):
                error_type = CrawlErrorType.DNS_ERROR

            self.stats.record_failure(error_type)
            return CrawlResult(
                url=url,
                success=False,
                error_type=error_type,
                error_message=str(e),
                crawl_time_ms=(time.time() - start_time) * 1000,
            )

        except httpx.TooManyRedirects as e:
            self.stats.record_failure(CrawlErrorType.TOO_MANY_REDIRECTS)
            return CrawlResult(
                url=url,
                success=False,
                error_type=CrawlErrorType.TOO_MANY_REDIRECTS,
                error_message=str(e),
                crawl_time_ms=(time.time() - start_time) * 1000,
            )

        except httpx.ReadError as e:
            self.stats.record_failure(CrawlErrorType.READ_ERROR)
            return CrawlResult(
                url=url,
                success=False,
                error_type=CrawlErrorType.READ_ERROR,
                error_message=str(e),
                crawl_time_ms=(time.time() - start_time) * 1000,
            )

        except Exception as e:
            self.stats.record_failure(CrawlErrorType.UNKNOWN)
            logger.error(f"Unexpected error crawling {url}: {e}")
            return CrawlResult(
                url=url,
                success=False,
                error_type=CrawlErrorType.UNKNOWN,
                error_message=str(e),
                crawl_time_ms=(time.time() - start_time) * 1000,
            )

    async def crawl_batch(
        self,
        urls: list[str],
        progress_callback: Optional[callable] = None,
    ) -> list[CrawlResult]:
        """
        배치 크롤링

        Args:
            urls: 크롤링할 URL 리스트
            progress_callback: 진행상황 콜백 (현재 완료 수, 전체 수)

        Returns:
            CrawlResult 리스트
        """
        results = []
        total = len(urls)

        # 병렬 실행
        tasks = [self.crawl(url) for url in urls]

        for i, coro in enumerate(asyncio.as_completed(tasks)):
            result = await coro
            results.append(result)

            if progress_callback:
                progress_callback(i + 1, total)

        return results

    async def crawl_and_produce(
        self,
        urls: list[str],
        batch_size: int = 100,
    ) -> tuple[int, int]:
        """
        크롤링 후 Kafka로 전송

        Args:
            urls: 크롤링할 URL 리스트
            batch_size: Kafka 전송 배치 크기

        Returns:
            (성공 수, 실패 수) 튜플
        """
        if not self.kafka_producer:
            raise RuntimeError("Kafka producer not set")

        success_count = 0
        fail_count = 0

        # 배치 단위로 처리
        for i in range(0, len(urls), batch_size):
            batch_urls = urls[i:i + batch_size]

            # 병렬 크롤링
            results = await self.crawl_batch(batch_urls)

            # Kafka 전송
            for result in results:
                if result.success:
                    sent = await self.kafka_producer.send_raw_page(
                        url=result.url,
                        html_compressed=result.html_compressed,
                        status_code=result.status_code,
                        headers=result.headers,
                        crawl_time_ms=result.crawl_time_ms,
                        metadata={
                            'redirect_url': result.redirect_url,
                            'raw_size': len(result.html),
                            'compressed_size': len(result.html_compressed),
                        }
                    )
                    if sent:
                        success_count += 1
                    else:
                        fail_count += 1
                else:
                    # 실패한 크롤링은 DLQ로
                    await self.kafka_producer.send_to_dlq(
                        url=result.url,
                        error_type=result.error_type.value if result.error_type else "unknown",
                        error_message=result.error_message or "",
                    )
                    fail_count += 1

            # 진행상황 로깅
            processed = min(i + batch_size, len(urls))
            logger.info(
                f"Progress: {processed}/{len(urls)} "
                f"({processed/len(urls)*100:.1f}%) - "
                f"Success: {success_count}, Failed: {fail_count}"
            )

        return success_count, fail_count

    async def stream_crawl(
        self,
        urls: AsyncGenerator[str, None],
        max_buffer: int = 1000,
    ) -> AsyncGenerator[CrawlResult, None]:
        """
        스트리밍 방식 크롤링 (메모리 효율적)

        Args:
            urls: URL을 yield하는 async generator
            max_buffer: 최대 버퍼 크기

        Yields:
            CrawlResult 객체
        """
        buffer = []

        async for url in urls:
            buffer.append(url)

            if len(buffer) >= max_buffer:
                results = await self.crawl_batch(buffer)
                for result in results:
                    yield result
                buffer.clear()

        # 남은 버퍼 처리
        if buffer:
            results = await self.crawl_batch(buffer)
            for result in results:
                yield result

    def get_stats(self) -> dict:
        """현재 통계 반환"""
        return {
            'total_requests': self.stats.total_requests,
            'successful_requests': self.stats.successful_requests,
            'failed_requests': self.stats.failed_requests,
            'success_rate': self.stats.success_rate,
            'requests_per_second': self.stats.requests_per_second,
            'average_crawl_time_ms': self.stats.average_crawl_time_ms,
            'compression_ratio': self.stats.compression_ratio,
            'total_bytes_raw': self.stats.total_bytes_raw,
            'total_bytes_compressed': self.stats.total_bytes_compressed,
            'errors_by_type': self.stats.errors_by_type,
        }

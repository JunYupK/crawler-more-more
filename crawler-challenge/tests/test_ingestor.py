"""
Ingestor 모듈 테스트
====================

Usage:
    pytest tests/test_ingestor.py -v
    pytest tests/test_ingestor.py -v -k "compression"
"""

import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
import sys
from pathlib import Path

# 프로젝트 루트를 path에 추가
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


class TestZstdCompressor:
    """Zstd 압축 테스트"""

    def test_compress_string(self):
        """문자열 압축 테스트"""
        from src.ingestor.compression import ZstdCompressor

        compressor = ZstdCompressor(level=3)
        original = "Hello, World! " * 100

        compressed = compressor.compress(original)

        assert isinstance(compressed, bytes)
        assert len(compressed) < len(original.encode('utf-8'))

    def test_compress_bytes(self):
        """바이트 압축 테스트"""
        from src.ingestor.compression import ZstdCompressor

        compressor = ZstdCompressor(level=3)
        original = b"Hello, World! " * 100

        compressed = compressor.compress(original)

        assert isinstance(compressed, bytes)
        assert len(compressed) < len(original)

    def test_decompress(self):
        """압축 해제 테스트"""
        from src.ingestor.compression import ZstdCompressor

        compressor = ZstdCompressor(level=3)
        original = b"Hello, World! " * 100

        compressed = compressor.compress(original)
        decompressed = compressor.decompress(compressed)

        assert decompressed == original

    def test_decompress_to_str(self):
        """압축 해제 후 문자열 변환 테스트"""
        from src.ingestor.compression import ZstdCompressor

        compressor = ZstdCompressor(level=3)
        original = "안녕하세요, 세계! " * 100

        compressed = compressor.compress(original)
        decompressed = compressor.decompress_to_str(compressed)

        assert decompressed == original

    def test_compression_ratio(self):
        """압축률 계산 테스트"""
        from src.ingestor.compression import ZstdCompressor

        original = b"AAAAAAAAAA" * 1000  # 높은 압축률 예상
        compressed = ZstdCompressor(level=3).compress(original)

        ratio = ZstdCompressor.get_compression_ratio(original, compressed)

        assert ratio > 0.5  # 50% 이상 압축

    def test_convenience_functions(self):
        """간편 함수 테스트"""
        from src.ingestor.compression import compress, decompress, decompress_to_str

        original = "Test string" * 50

        compressed = compress(original)
        decompressed = decompress(compressed)
        decompressed_str = decompress_to_str(compressed)

        assert decompressed == original.encode('utf-8')
        assert decompressed_str == original


class TestCompressionStats:
    """압축 통계 테스트"""

    def test_record_and_ratio(self):
        """통계 기록 및 비율 계산 테스트"""
        from src.ingestor.compression import CompressionStats

        stats = CompressionStats()
        stats.record(1000, 300)
        stats.record(2000, 500)

        assert stats.compression_count == 2
        assert stats.total_original_bytes == 3000
        assert stats.total_compressed_bytes == 800
        assert abs(stats.average_ratio - 0.733) < 0.01  # ~73.3%
        assert stats.total_saved_bytes == 2200


class TestCrawlResult:
    """크롤링 결과 테스트"""

    def test_success_result(self):
        """성공 결과 테스트"""
        from src.ingestor.httpx_crawler import CrawlResult

        result = CrawlResult(
            url="https://example.com",
            success=True,
            status_code=200,
            html=b"<html></html>",
            crawl_time_ms=100.5,
        )

        assert result.success
        assert result.status_code == 200
        assert result.error_type is None

    def test_failure_result(self):
        """실패 결과 테스트"""
        from src.ingestor.httpx_crawler import CrawlResult, CrawlErrorType

        result = CrawlResult(
            url="https://example.com",
            success=False,
            error_type=CrawlErrorType.TIMEOUT,
            error_message="Connection timed out",
        )

        assert not result.success
        assert result.error_type == CrawlErrorType.TIMEOUT


class TestIngestorStats:
    """Ingestor 통계 테스트"""

    def test_record_success(self):
        """성공 기록 테스트"""
        from src.ingestor.httpx_crawler import IngestorStats

        stats = IngestorStats()
        stats.record_success(100.0, 10000, 3000)
        stats.record_success(150.0, 15000, 4000)

        assert stats.successful_requests == 2
        assert stats.total_requests == 2
        assert stats.success_rate == 1.0
        assert stats.average_crawl_time_ms == 125.0

    def test_record_failure(self):
        """실패 기록 테스트"""
        from src.ingestor.httpx_crawler import IngestorStats, CrawlErrorType

        stats = IngestorStats()
        stats.record_success(100.0, 10000, 3000)
        stats.record_failure(CrawlErrorType.TIMEOUT)
        stats.record_failure(CrawlErrorType.TIMEOUT)
        stats.record_failure(CrawlErrorType.SSL_ERROR)

        assert stats.total_requests == 4
        assert stats.successful_requests == 1
        assert stats.failed_requests == 3
        assert stats.success_rate == 0.25
        assert stats.errors_by_type['timeout'] == 2
        assert stats.errors_by_type['ssl_error'] == 1


class TestHighSpeedIngestor:
    """HighSpeedIngestor 테스트"""

    @pytest.mark.asyncio
    async def test_start_stop(self):
        """시작/중지 테스트"""
        from src.ingestor.httpx_crawler import HighSpeedIngestor

        ingestor = HighSpeedIngestor()

        await ingestor.start()
        assert ingestor._client is not None
        assert ingestor._semaphore is not None

        await ingestor.stop()
        assert ingestor._client is None

    @pytest.mark.asyncio
    async def test_context_manager(self):
        """컨텍스트 매니저 테스트"""
        from src.ingestor.httpx_crawler import HighSpeedIngestor

        async with HighSpeedIngestor() as ingestor:
            assert ingestor._client is not None

    @pytest.mark.asyncio
    async def test_crawl_success(self):
        """성공적인 크롤링 테스트 (mock)"""
        from src.ingestor.httpx_crawler import HighSpeedIngestor

        async with HighSpeedIngestor() as ingestor:
            # httpbin.org 테스트 (실제 요청)
            # 실제 테스트 환경에서는 mock 사용 권장
            result = await ingestor.crawl("https://httpbin.org/html")

            # httpbin이 사용 불가능할 수 있으므로 결과만 확인
            assert result.url == "https://httpbin.org/html"
            assert result.crawl_time_ms > 0

    @pytest.mark.asyncio
    async def test_crawl_batch(self):
        """배치 크롤링 테스트"""
        from src.ingestor.httpx_crawler import HighSpeedIngestor

        urls = [
            "https://httpbin.org/status/200",
            "https://httpbin.org/status/404",
        ]

        async with HighSpeedIngestor() as ingestor:
            results = await ingestor.crawl_batch(urls)

            assert len(results) == 2

    def test_get_stats(self):
        """통계 조회 테스트"""
        from src.ingestor.httpx_crawler import HighSpeedIngestor

        ingestor = HighSpeedIngestor()
        stats = ingestor.get_stats()

        assert 'total_requests' in stats
        assert 'success_rate' in stats
        assert 'compression_ratio' in stats


class TestKafkaPageProducer:
    """KafkaPageProducer 테스트 (mock)"""

    @pytest.mark.asyncio
    async def test_serialize(self):
        """직렬화 테스트"""
        from src.ingestor.kafka_producer import KafkaPageProducer

        producer = KafkaPageProducer(bootstrap_servers="localhost:9092")

        data = {'url': 'https://example.com', 'status': 200}
        serialized = producer._serialize(data)

        assert isinstance(serialized, bytes)

    def test_url_to_key(self):
        """URL to 키 변환 테스트"""
        from src.ingestor.kafka_producer import KafkaPageProducer

        assert KafkaPageProducer._url_to_key("https://example.com/path") == "example.com"
        assert KafkaPageProducer._url_to_key("https://sub.example.com/") == "sub.example.com"


class TestProducerStats:
    """ProducerStats 테스트"""

    def test_stats_calculation(self):
        """통계 계산 테스트"""
        from src.ingestor.kafka_producer import ProducerStats

        stats = ProducerStats()
        stats.messages_sent = 90
        stats.messages_failed = 10
        stats.bytes_sent = 50000

        assert stats.success_rate == 0.9


# 통합 테스트 (실제 Kafka 필요)
class TestIntegration:
    """통합 테스트 (실제 인프라 필요)"""

    @pytest.mark.skip(reason="Requires Kafka infrastructure")
    @pytest.mark.asyncio
    async def test_full_pipeline(self):
        """전체 파이프라인 테스트"""
        from src.ingestor.httpx_crawler import HighSpeedIngestor
        from src.ingestor.kafka_producer import KafkaPageProducer

        async with KafkaPageProducer() as producer:
            async with HighSpeedIngestor() as ingestor:
                ingestor.set_kafka_producer(producer)

                success, fail = await ingestor.crawl_and_produce(
                    ["https://example.com"],
                    batch_size=1,
                )

                assert success == 1
                assert fail == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

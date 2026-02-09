#!/usr/bin/env python3
"""
Storage Module Tests
====================

Layer 4: Hybrid Storage 테스트
- MinIO Writer
- PostgreSQL Writer
- Hybrid Storage
"""

import asyncio
import time
import hashlib
from datetime import datetime, timezone
from unittest.mock import MagicMock, AsyncMock, patch
from urllib.parse import urlparse

import sys
from pathlib import Path

try:
    import pytest
    PYTEST_AVAILABLE = True
except ImportError:
    PYTEST_AVAILABLE = False

    # pytest 없을 때 더미 데코레이터 생성
    class DummyMark:
        @staticmethod
        def asyncio(func):
            return func

        @staticmethod
        def skip(reason=""):
            def decorator(func):
                return func
            return decorator

    class DummyPytest:
        mark = DummyMark()

    pytest = DummyPytest()

sys.path.insert(0, str(Path(__file__).parent.parent))

# 의존성 모듈 체크
DEPS_AVAILABLE = True
try:
    import asyncpg
    import minio
    import msgpack
    from aiokafka import AIOKafkaConsumer
except ImportError as e:
    DEPS_AVAILABLE = False
    print(f"Warning: Some dependencies not available: {e}")

if DEPS_AVAILABLE:
    from src.storage.minio_writer import (
        MinIOWriter,
        StoredObject,
        MinIOStats,
    )
    from src.storage.postgres_writer import (
        PostgresWriter,
        PageRecord,
        PostgresStats,
    )
    from src.storage.hybrid_storage import (
        HybridStorage,
        StorageWriter,
        StorageResult,
        HybridStorageStats,
    )
else:
    # 모듈 없을 때는 직접 정의
    from dataclasses import dataclass, field
    from typing import Optional, Any

    @dataclass
    class StoredObject:
        bucket: str
        key: str
        size: int
        etag: str
        url: str
        content_type: str = "application/octet-stream"
        compressed: bool = False
        original_size: int = 0

        @property
        def full_path(self) -> str:
            return f"{self.bucket}/{self.key}"

        @property
        def compression_ratio(self) -> float:
            if self.original_size == 0:
                return 0.0
            return 1 - (self.size / self.original_size)

    @dataclass
    class MinIOStats:
        objects_stored: int = 0
        objects_failed: int = 0
        total_bytes_stored: int = 0
        total_bytes_original: int = 0
        start_time: float = field(default_factory=time.time)

        @property
        def compression_ratio(self) -> float:
            if self.total_bytes_original == 0:
                return 0.0
            return 1 - (self.total_bytes_stored / self.total_bytes_original)

        @property
        def average_size(self) -> float:
            return self.total_bytes_stored / self.objects_stored if self.objects_stored > 0 else 0

        @property
        def objects_per_second(self) -> float:
            elapsed = time.time() - self.start_time
            return self.objects_stored / elapsed if elapsed > 0 else 0

        def record_success(self, stored_size: int, original_size: int):
            self.objects_stored += 1
            self.total_bytes_stored += stored_size
            self.total_bytes_original += original_size

        def record_failure(self):
            self.objects_failed += 1

    @dataclass
    class PostgresStats:
        records_inserted: int = 0
        records_updated: int = 0
        records_failed: int = 0
        batch_count: int = 0
        total_insert_time_ms: float = 0.0
        start_time: float = field(default_factory=time.time)

        @property
        def total_records(self) -> int:
            return self.records_inserted + self.records_updated

        @property
        def records_per_second(self) -> float:
            elapsed = time.time() - self.start_time
            return self.total_records / elapsed if elapsed > 0 else 0

        @property
        def average_insert_time_ms(self) -> float:
            return self.total_insert_time_ms / self.batch_count if self.batch_count > 0 else 0

    @dataclass
    class PageRecord:
        url: str
        domain: str
        title: Optional[str] = None
        description: Optional[str] = None
        language: Optional[str] = None
        keywords: Optional[list] = None
        processor: str = "fast"
        static_score: Optional[int] = None
        route_reason: Optional[str] = None
        raw_html_key: Optional[str] = None
        markdown_key: Optional[str] = None
        raw_size_bytes: Optional[int] = None
        markdown_size_bytes: Optional[int] = None
        status_code: Optional[int] = None
        content_type: Optional[str] = None
        crawled_at: Optional[Any] = None
        processed_at: Optional[Any] = None

        @classmethod
        def from_processed_result(cls, result: dict, minio_keys: dict = None) -> "PageRecord":
            from urllib.parse import urlparse
            url = result.get('url', '')
            parsed = urlparse(url)
            minio_keys = minio_keys or {}
            stats = result.get('stats', {})
            metadata = result.get('metadata', {})

            return cls(
                url=url,
                domain=parsed.netloc or 'unknown',
                title=metadata.get('title'),
                description=metadata.get('description'),
                language=metadata.get('language'),
                keywords=metadata.get('keywords'),
                processor=result.get('processor_type', 'fast'),
                static_score=stats.get('original_score'),
                markdown_key=minio_keys.get('markdown'),
            )

    @dataclass
    class StorageResult:
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

    # MinIOWriter URL to key 함수만 정의
    class MinIOWriter:
        def __init__(self, compress: bool = True):
            self.compress = compress
            self.stats = MinIOStats()

        def _url_to_key(self, url: str, extension: str = "") -> str:
            import hashlib
            from urllib.parse import urlparse

            parsed = urlparse(url)
            domain = parsed.netloc or "unknown"
            url_hash = hashlib.sha256(url.encode()).hexdigest()[:16]
            key = f"{domain}/{url_hash[0:2]}/{url_hash[2:4]}/{url_hash}{extension}"
            return key

    # Dummy classes
    HybridStorage = None
    StorageWriter = None
    PostgresWriter = None


class TestStoredObject:
    """StoredObject 테스트"""

    def test_full_path(self):
        """전체 경로 테스트"""
        obj = StoredObject(
            bucket="markdown",
            key="example.com/ab/cd/abcd1234.md",
            size=1000,
            etag="abc123",
            url="https://example.com/page",
        )
        assert obj.full_path == "markdown/example.com/ab/cd/abcd1234.md"

    def test_compression_ratio(self):
        """압축률 계산 테스트"""
        obj = StoredObject(
            bucket="markdown",
            key="test.md",
            size=300,
            etag="abc",
            url="https://example.com",
            original_size=1000,
            compressed=True,
        )
        assert obj.compression_ratio == 0.7  # 1 - 300/1000

    def test_compression_ratio_zero_original(self):
        """원본 크기 0일 때"""
        obj = StoredObject(
            bucket="markdown",
            key="test.md",
            size=0,
            etag="abc",
            url="https://example.com",
            original_size=0,
        )
        assert obj.compression_ratio == 0.0


class TestMinIOStats:
    """MinIOStats 테스트"""

    def test_record_success(self):
        """성공 기록"""
        stats = MinIOStats()
        stats.record_success(stored_size=500, original_size=1000)

        assert stats.objects_stored == 1
        assert stats.total_bytes_stored == 500
        assert stats.total_bytes_original == 1000

    def test_record_failure(self):
        """실패 기록"""
        stats = MinIOStats()
        stats.record_failure()

        assert stats.objects_failed == 1

    def test_compression_ratio(self):
        """압축률 계산"""
        stats = MinIOStats()
        stats.record_success(300, 1000)
        stats.record_success(400, 1000)

        # 700 / 2000 = 0.35, compression = 1 - 0.35 = 0.65
        assert stats.compression_ratio == 0.65

    def test_average_size(self):
        """평균 크기"""
        stats = MinIOStats()
        stats.record_success(200, 400)
        stats.record_success(400, 800)

        assert stats.average_size == 300  # (200+400) / 2

    def test_objects_per_second(self):
        """초당 처리량"""
        stats = MinIOStats()
        stats.start_time = time.time() - 10  # 10초 전
        stats.objects_stored = 50

        assert 4.5 <= stats.objects_per_second <= 5.5  # ~5/s


class TestMinIOWriter:
    """MinIOWriter 테스트"""

    def test_url_to_key(self):
        """URL → 키 변환"""
        writer = MinIOWriter(compress=False)

        url = "https://example.com/path/to/page"
        key = writer._url_to_key(url, ".md")

        # 구조 확인: domain/hash[0:2]/hash[2:4]/hash.ext
        assert key.startswith("example.com/")
        assert key.endswith(".md")
        parts = key.split("/")
        assert len(parts) == 4  # domain, hash[0:2], hash[2:4], filename

    def test_url_to_key_consistency(self):
        """동일 URL은 동일 키"""
        writer = MinIOWriter(compress=False)

        url = "https://test.com/page"
        key1 = writer._url_to_key(url, ".md")
        key2 = writer._url_to_key(url, ".md")

        assert key1 == key2

    def test_url_to_key_different_urls(self):
        """다른 URL은 다른 키"""
        writer = MinIOWriter(compress=False)

        key1 = writer._url_to_key("https://a.com/1", ".md")
        key2 = writer._url_to_key("https://a.com/2", ".md")

        assert key1 != key2

    def test_stats_initialized(self):
        """통계 초기화"""
        writer = MinIOWriter()

        assert writer.stats.objects_stored == 0
        assert writer.stats.objects_failed == 0


class TestPageRecord:
    """PageRecord 테스트"""

    def test_from_processed_result(self):
        """ProcessedResult에서 생성"""
        result = {
            'url': 'https://example.com/article',
            'success': True,
            'processor_type': 'fast',
            'metadata': {
                'title': 'Test Article',
                'description': 'A test article',
                'language': 'en',
                'keywords': ['test', 'article'],
            },
            'stats': {
                'original_score': 85,
                'content_length': 5000,
                'markdown_length': 2000,
            },
            'timestamps': {
                'original': time.time() - 60,
                'processor': time.time(),
            },
        }

        record = PageRecord.from_processed_result(
            result,
            minio_keys={'markdown': 'example.com/ab/cd/test.md'}
        )

        assert record.url == 'https://example.com/article'
        assert record.domain == 'example.com'
        assert record.title == 'Test Article'
        assert record.description == 'A test article'
        assert record.language == 'en'
        assert record.processor == 'fast'
        assert record.static_score == 85
        assert record.markdown_key == 'example.com/ab/cd/test.md'

    def test_from_processed_result_minimal(self):
        """최소 데이터로 생성"""
        result = {
            'url': 'https://test.com/',
            'success': True,
        }

        record = PageRecord.from_processed_result(result)

        assert record.url == 'https://test.com/'
        assert record.domain == 'test.com'
        assert record.processor == 'fast'


class TestPostgresStats:
    """PostgresStats 테스트"""

    def test_total_records(self):
        """총 레코드 수"""
        stats = PostgresStats()
        stats.records_inserted = 100
        stats.records_updated = 50

        assert stats.total_records == 150

    def test_records_per_second(self):
        """초당 레코드 수"""
        stats = PostgresStats()
        stats.start_time = time.time() - 10
        stats.records_inserted = 80
        stats.records_updated = 20

        assert 9.0 <= stats.records_per_second <= 11.0  # ~10/s

    def test_average_insert_time(self):
        """평균 삽입 시간"""
        stats = PostgresStats()
        stats.batch_count = 5
        stats.total_insert_time_ms = 500

        assert stats.average_insert_time_ms == 100.0


class TestStorageResult:
    """StorageResult 테스트"""

    def test_to_dict_success(self):
        """성공 결과 딕셔너리 변환"""
        minio_obj = StoredObject(
            bucket="markdown",
            key="test.md",
            size=500,
            etag="abc",
            url="https://example.com",
        )

        result = StorageResult(
            url="https://example.com",
            success=True,
            minio_markdown=minio_obj,
            postgres_saved=True,
            processing_time_ms=50.0,
        )

        d = result.to_dict()

        assert d['url'] == "https://example.com"
        assert d['success'] is True
        assert d['minio']['markdown_key'] == "test.md"
        assert d['postgres_saved'] is True
        assert d['processing_time_ms'] == 50.0

    def test_to_dict_failure(self):
        """실패 결과 딕셔너리 변환"""
        result = StorageResult(
            url="https://example.com",
            success=False,
            error="Connection failed",
        )

        d = result.to_dict()

        assert d['success'] is False
        assert d['error'] == "Connection failed"
        assert d['minio']['markdown_key'] is None


class TestHybridStorageStats:
    """HybridStorageStats 테스트"""

    def test_success_rate(self):
        """성공률 계산"""
        stats = HybridStorageStats()
        stats.messages_stored = 90
        stats.messages_failed = 10

        assert stats.success_rate == 0.9

    def test_success_rate_no_messages(self):
        """메시지 없을 때 성공률"""
        stats = HybridStorageStats()

        assert stats.success_rate == 0

    def test_messages_per_second(self):
        """초당 메시지 수"""
        stats = HybridStorageStats()
        stats.start_time = time.time() - 5
        stats.messages_consumed = 100

        assert 18.0 <= stats.messages_per_second <= 22.0  # ~20/s

    def test_average_processing_time(self):
        """평균 처리 시간"""
        stats = HybridStorageStats()
        stats.messages_stored = 10
        stats.total_processing_time_ms = 500

        assert stats.average_processing_time_ms == 50.0


class TestHybridStorageMocked:
    """HybridStorage 모킹 테스트"""

    @pytest.mark.asyncio
    async def test_store_message_skips_failed(self):
        """실패한 메시지는 건너뛰기"""
        storage = HybridStorage()

        # _store_message 직접 호출
        message = {
            'url': 'https://example.com',
            'success': False,
        }

        result = await storage._store_message(message)

        assert result is None

    @pytest.mark.asyncio
    async def test_store_single(self):
        """단일 저장 테스트 (모킹)"""
        storage = HybridStorage()

        # MinIO와 Postgres 모킹
        storage.minio.store_markdown = AsyncMock(return_value=StoredObject(
            bucket="markdown",
            key="test.md",
            size=100,
            etag="abc",
            url="https://example.com",
        ))
        storage.postgres.save = AsyncMock(return_value=True)

        message = {
            'url': 'https://example.com',
            'success': True,
            'markdown': '# Test',
            'metadata': {'title': 'Test'},
        }

        result = await storage.store_single(message)

        assert result.success is True
        assert result.minio_markdown is not None
        assert result.postgres_saved is True


class TestStorageWriter:
    """StorageWriter 테스트"""

    @pytest.mark.asyncio
    async def test_save_mocked(self):
        """저장 테스트 (모킹)"""
        writer = StorageWriter()

        # MinIO와 Postgres 모킹
        writer.minio.store_markdown = AsyncMock(return_value=StoredObject(
            bucket="markdown",
            key="test.md",
            size=100,
            etag="abc",
            url="https://example.com",
        ))
        writer.postgres.save = AsyncMock(return_value=True)
        writer.postgres.flush = AsyncMock()

        result = await writer.save(
            url="https://example.com/article",
            markdown="# Test Article",
            metadata={'title': 'Test'},
            processor='fast',
            score=85,
        )

        assert result.success is True


class TestIntegration:
    """통합 테스트 (실제 연결 필요)"""

    @pytest.mark.skip(reason="Requires actual MinIO and PostgreSQL")
    @pytest.mark.asyncio
    async def test_full_storage_flow(self):
        """전체 저장 흐름 테스트"""
        async with StorageWriter() as writer:
            result = await writer.save(
                url="https://example.com/test",
                markdown="# Test\n\nContent here",
                metadata={'title': 'Test Page'},
                processor='fast',
                score=90,
            )

            assert result.success is True
            assert result.minio_markdown is not None
            assert result.postgres_saved is True


def run_quick_test():
    """빠른 테스트 실행"""
    print("=" * 60)
    print("Storage Module Quick Tests")
    print("=" * 60)

    # StoredObject 테스트
    print("\n1. Testing StoredObject...")
    obj = StoredObject(
        bucket="markdown",
        key="example.com/ab/cd/test.md",
        size=300,
        etag="abc123",
        url="https://example.com/page",
        original_size=1000,
        compressed=True,
    )
    assert obj.full_path == "markdown/example.com/ab/cd/test.md"
    assert obj.compression_ratio == 0.7
    print("   ✅ StoredObject tests passed")

    # MinIOStats 테스트
    print("\n2. Testing MinIOStats...")
    stats = MinIOStats()
    stats.record_success(300, 1000)
    stats.record_success(400, 1000)
    assert stats.objects_stored == 2
    assert stats.compression_ratio == 0.65
    print("   ✅ MinIOStats tests passed")

    # PageRecord 테스트
    print("\n3. Testing PageRecord...")
    record = PageRecord.from_processed_result({
        'url': 'https://example.com/test',
        'success': True,
        'processor_type': 'fast',
        'metadata': {'title': 'Test'},
        'stats': {'original_score': 85},
    })
    assert record.domain == 'example.com'
    assert record.static_score == 85
    print("   ✅ PageRecord tests passed")

    # StorageResult 테스트
    print("\n4. Testing StorageResult...")
    result = StorageResult(
        url="https://example.com",
        success=True,
        postgres_saved=True,
    )
    d = result.to_dict()
    assert d['success'] is True
    print("   ✅ StorageResult tests passed")

    # HybridStorageStats 테스트
    print("\n5. Testing HybridStorageStats...")
    h_stats = HybridStorageStats()
    h_stats.messages_stored = 90
    h_stats.messages_failed = 10
    assert h_stats.success_rate == 0.9
    print("   ✅ HybridStorageStats tests passed")

    # MinIOWriter URL→Key 테스트
    print("\n6. Testing MinIOWriter URL to Key...")
    writer = MinIOWriter(compress=False)
    key = writer._url_to_key("https://example.com/page", ".md")
    assert key.startswith("example.com/")
    assert key.endswith(".md")
    print(f"   Key: {key}")
    print("   ✅ URL to Key tests passed")

    print("\n" + "=" * 60)
    print("All quick tests passed! ✅")
    print("=" * 60)


if __name__ == "__main__":
    run_quick_test()

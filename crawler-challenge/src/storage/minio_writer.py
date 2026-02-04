"""
MinIO Writer - Object Storage for Content
==========================================

대용량 콘텐츠(HTML, Markdown)를 MinIO/S3에 저장
- 비동기 업로드
- Zstd 압축 저장
- 버킷 자동 생성
"""

import asyncio
import logging
import hashlib
import time
from typing import Optional, Union
from dataclasses import dataclass, field
from urllib.parse import urlparse
from io import BytesIO

from minio import Minio
from minio.error import S3Error

import sys
sys.path.insert(0, '/home/user/crawler-more-more/crawler-challenge')
from config.kafka_config import get_config, MinIOConfig
from src.ingestor.compression import ZstdCompressor

logger = logging.getLogger(__name__)


@dataclass
class StoredObject:
    """저장된 객체 정보"""
    bucket: str
    key: str
    size: int
    etag: str
    url: str  # 원본 URL
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
    """MinIO 저장 통계"""
    objects_stored: int = 0
    objects_failed: int = 0
    total_bytes_stored: int = 0
    total_bytes_original: int = 0
    start_time: float = field(default_factory=time.time)

    @property
    def average_size(self) -> float:
        return self.total_bytes_stored / self.objects_stored if self.objects_stored > 0 else 0

    @property
    def compression_ratio(self) -> float:
        if self.total_bytes_original == 0:
            return 0.0
        return 1 - (self.total_bytes_stored / self.total_bytes_original)

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

    def __str__(self) -> str:
        return (
            f"MinIOStats("
            f"stored={self.objects_stored:,}, "
            f"failed={self.objects_failed:,}, "
            f"bytes={self.total_bytes_stored:,}, "
            f"compression={self.compression_ratio:.1%}, "
            f"ops={self.objects_per_second:.1f}/s)"
        )


class MinIOWriter:
    """
    MinIO 콘텐츠 저장기

    대용량 HTML/Markdown을 MinIO(S3 호환)에 저장
    - 압축 저장으로 스토리지 절약
    - URL 기반 키 생성
    - 버킷 자동 생성
    """

    def __init__(
        self,
        config: Optional[MinIOConfig] = None,
        compress: bool = True,
        compression_level: int = 3,
    ):
        """
        Args:
            config: MinIO 설정 (없으면 기본값)
            compress: 압축 저장 여부
            compression_level: Zstd 압축 레벨
        """
        self.config = config or get_config().minio

        # MinIO 클라이언트
        self._client = Minio(
            endpoint=self.config.endpoint,
            access_key=self.config.access_key,
            secret_key=self.config.secret_key,
            secure=self.config.secure,
        )

        # 압축 설정
        self.compress = compress
        self._compressor = ZstdCompressor(level=compression_level) if compress else None

        # 통계
        self.stats = MinIOStats()

        # 버킷 초기화
        self._buckets_initialized = set()

        logger.info(
            f"MinIOWriter initialized: "
            f"endpoint={self.config.endpoint}, "
            f"compress={compress}"
        )

    async def initialize_buckets(self) -> None:
        """필요한 버킷들 생성"""
        buckets = [
            self.config.bucket_raw_html,
            self.config.bucket_markdown,
            self.config.bucket_metadata,
        ]

        for bucket in buckets:
            await self._ensure_bucket(bucket)

    async def _ensure_bucket(self, bucket: str) -> bool:
        """버킷 존재 확인 및 생성"""
        if bucket in self._buckets_initialized:
            return True

        try:
            # 동기 호출을 비동기로 래핑
            loop = asyncio.get_event_loop()
            exists = await loop.run_in_executor(
                None, self._client.bucket_exists, bucket
            )

            if not exists:
                await loop.run_in_executor(
                    None, self._client.make_bucket, bucket
                )
                logger.info(f"Created bucket: {bucket}")

            self._buckets_initialized.add(bucket)
            return True

        except S3Error as e:
            logger.error(f"Failed to ensure bucket {bucket}: {e}")
            return False

    def _url_to_key(self, url: str, extension: str = "") -> str:
        """
        URL을 저장 키로 변환

        구조: domain/hash[0:2]/hash[2:4]/hash.ext
        예: example.com/ab/cd/abcdef123456.md
        """
        parsed = urlparse(url)
        domain = parsed.netloc or "unknown"

        # URL 해시
        url_hash = hashlib.sha256(url.encode()).hexdigest()[:16]

        # 계층적 구조로 저장 (많은 파일 분산)
        key = f"{domain}/{url_hash[0:2]}/{url_hash[2:4]}/{url_hash}{extension}"

        return key

    async def store_markdown(
        self,
        url: str,
        markdown: str,
        metadata: Optional[dict] = None,
    ) -> Optional[StoredObject]:
        """
        Markdown 콘텐츠 저장

        Args:
            url: 원본 URL
            markdown: Markdown 콘텐츠
            metadata: 추가 메타데이터 (S3 태그)

        Returns:
            StoredObject 또는 None (실패 시)
        """
        return await self._store_content(
            bucket=self.config.bucket_markdown,
            url=url,
            content=markdown,
            extension=".md",
            content_type="text/markdown; charset=utf-8",
            metadata=metadata,
        )

    async def store_raw_html(
        self,
        url: str,
        html: Union[str, bytes],
        metadata: Optional[dict] = None,
    ) -> Optional[StoredObject]:
        """
        Raw HTML 저장

        Args:
            url: 원본 URL
            html: HTML 콘텐츠
            metadata: 추가 메타데이터

        Returns:
            StoredObject 또는 None
        """
        return await self._store_content(
            bucket=self.config.bucket_raw_html,
            url=url,
            content=html,
            extension=".html.zst" if self.compress else ".html",
            content_type="text/html; charset=utf-8",
            metadata=metadata,
        )

    async def _store_content(
        self,
        bucket: str,
        url: str,
        content: Union[str, bytes],
        extension: str,
        content_type: str,
        metadata: Optional[dict] = None,
    ) -> Optional[StoredObject]:
        """내부 저장 메서드"""
        try:
            # 버킷 확인
            await self._ensure_bucket(bucket)

            # 키 생성
            key = self._url_to_key(url, extension)

            # 바이트로 변환
            if isinstance(content, str):
                content_bytes = content.encode('utf-8')
            else:
                content_bytes = content

            original_size = len(content_bytes)

            # 압축
            if self.compress and self._compressor:
                content_bytes = self._compressor.compress(content_bytes)
                content_type = "application/zstd"

            # BytesIO로 래핑
            data = BytesIO(content_bytes)
            size = len(content_bytes)

            # 메타데이터 준비
            minio_metadata = {
                "x-amz-meta-original-url": url[:256],  # S3 메타데이터 크기 제한
                "x-amz-meta-original-size": str(original_size),
                "x-amz-meta-compressed": str(self.compress),
            }
            if metadata:
                for k, v in metadata.items():
                    if isinstance(v, str) and len(v) < 256:
                        minio_metadata[f"x-amz-meta-{k}"] = v

            # 업로드 (동기 → 비동기)
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                None,
                lambda: self._client.put_object(
                    bucket_name=bucket,
                    object_name=key,
                    data=data,
                    length=size,
                    content_type=content_type,
                    metadata=minio_metadata,
                )
            )

            # 통계 기록
            self.stats.record_success(size, original_size)

            stored = StoredObject(
                bucket=bucket,
                key=key,
                size=size,
                etag=result.etag,
                url=url,
                content_type=content_type,
                compressed=self.compress,
                original_size=original_size,
            )

            logger.debug(
                f"Stored: {stored.full_path} "
                f"({original_size:,} -> {size:,} bytes, "
                f"{stored.compression_ratio:.1%} saved)"
            )

            return stored

        except S3Error as e:
            logger.error(f"S3 error storing {url}: {e}")
            self.stats.record_failure()
            return None
        except Exception as e:
            logger.error(f"Error storing {url}: {e}")
            self.stats.record_failure()
            return None

    async def get_markdown(self, url: str) -> Optional[str]:
        """
        저장된 Markdown 조회

        Args:
            url: 원본 URL

        Returns:
            Markdown 콘텐츠 또는 None
        """
        return await self._get_content(
            bucket=self.config.bucket_markdown,
            url=url,
            extension=".md",
        )

    async def get_raw_html(self, url: str) -> Optional[str]:
        """
        저장된 Raw HTML 조회

        Args:
            url: 원본 URL

        Returns:
            HTML 콘텐츠 또는 None
        """
        extension = ".html.zst" if self.compress else ".html"
        return await self._get_content(
            bucket=self.config.bucket_raw_html,
            url=url,
            extension=extension,
        )

    async def _get_content(
        self,
        bucket: str,
        url: str,
        extension: str,
    ) -> Optional[str]:
        """내부 조회 메서드"""
        try:
            key = self._url_to_key(url, extension)

            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                None,
                lambda: self._client.get_object(bucket, key)
            )

            content_bytes = response.read()
            response.close()
            response.release_conn()

            # 압축 해제
            if self.compress and self._compressor:
                content_bytes = self._compressor.decompress(content_bytes)

            return content_bytes.decode('utf-8')

        except S3Error as e:
            if e.code == 'NoSuchKey':
                logger.debug(f"Object not found: {url}")
            else:
                logger.error(f"S3 error getting {url}: {e}")
            return None
        except Exception as e:
            logger.error(f"Error getting {url}: {e}")
            return None

    async def delete(self, bucket: str, url: str, extension: str = "") -> bool:
        """
        객체 삭제

        Args:
            bucket: 버킷 이름
            url: 원본 URL
            extension: 파일 확장자

        Returns:
            성공 여부
        """
        try:
            key = self._url_to_key(url, extension)

            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                None,
                lambda: self._client.remove_object(bucket, key)
            )

            logger.debug(f"Deleted: {bucket}/{key}")
            return True

        except S3Error as e:
            logger.error(f"S3 error deleting {url}: {e}")
            return False

    async def exists(self, bucket: str, url: str, extension: str = "") -> bool:
        """
        객체 존재 확인

        Args:
            bucket: 버킷 이름
            url: 원본 URL
            extension: 파일 확장자

        Returns:
            존재 여부
        """
        try:
            key = self._url_to_key(url, extension)

            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                None,
                lambda: self._client.stat_object(bucket, key)
            )
            return True

        except S3Error as e:
            if e.code == 'NoSuchKey':
                return False
            logger.error(f"S3 error checking {url}: {e}")
            return False

    def get_stats(self) -> dict:
        """현재 통계 반환"""
        return {
            'objects_stored': self.stats.objects_stored,
            'objects_failed': self.stats.objects_failed,
            'total_bytes_stored': self.stats.total_bytes_stored,
            'total_bytes_original': self.stats.total_bytes_original,
            'compression_ratio': self.stats.compression_ratio,
            'average_size': self.stats.average_size,
            'objects_per_second': self.stats.objects_per_second,
        }


async def test_minio_connection(config: Optional[MinIOConfig] = None) -> dict:
    """MinIO 연결 테스트"""
    config = config or get_config().minio

    result = {
        'connected': False,
        'endpoint': config.endpoint,
        'buckets': [],
        'error': None,
    }

    try:
        client = Minio(
            endpoint=config.endpoint,
            access_key=config.access_key,
            secret_key=config.secret_key,
            secure=config.secure,
        )

        # 버킷 목록 조회
        buckets = client.list_buckets()
        result['connected'] = True
        result['buckets'] = [b.name for b in buckets]

    except Exception as e:
        result['error'] = str(e)

    return result

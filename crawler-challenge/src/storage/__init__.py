"""
Storage Module - Layer 4: Hybrid Storage
=========================================

효율적인 저장소 구성
- PostgreSQL: 메타데이터, 통계, 요약 정보 (검색용)
- MinIO/S3: 대용량 HTML/Markdown 본문 (저장용)

Components:
- postgres_writer: PostgreSQL 메타데이터 저장
- minio_writer: MinIO 콘텐츠 저장
- hybrid_storage: 통합 스토리지 인터페이스
"""

from .postgres_writer import PostgresWriter, PageRecord, test_postgres_connection
from .minio_writer import MinIOWriter, StoredObject, test_minio_connection
from .hybrid_storage import HybridStorage, StorageWriter, StorageResult, test_storage_connections

__all__ = [
    # Writers
    "PostgresWriter",
    "MinIOWriter",
    "HybridStorage",
    "StorageWriter",
    # Data classes
    "PageRecord",
    "StoredObject",
    "StorageResult",
    # Test utilities
    "test_postgres_connection",
    "test_minio_connection",
    "test_storage_connections",
]

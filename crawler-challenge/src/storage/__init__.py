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

from .postgres_writer import PostgresWriter
from .minio_writer import MinIOWriter
from .hybrid_storage import HybridStorage

__all__ = [
    "PostgresWriter",
    "MinIOWriter",
    "HybridStorage",
]

"""
Ingestor Module - Layer 1: High-Speed Ingestor
==============================================

Mac에서 실행되는 고속 HTTP 크롤러
- httpx async 기반 대량 요청
- Zstd 압축으로 네트워크 대역폭 절약
- Kafka로 직접 produce (DB 미거침)

Components:
- httpx_crawler: 비동기 HTTP 크롤러
- kafka_producer: Kafka 메시지 프로듀서
- compression: Zstd 압축 유틸리티
"""

from .httpx_crawler import HighSpeedIngestor
from .kafka_producer import KafkaPageProducer
from src.common.compression import ZstdCompressor

__all__ = [
    "HighSpeedIngestor",
    "KafkaPageProducer",
    "ZstdCompressor",
]

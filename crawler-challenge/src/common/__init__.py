"""
Common Module - Shared utilities across all layers
===================================================

Cross-layer shared code:
- compression: Zstd compression/decompression
- kafka_config: Pipeline configuration (Kafka, MinIO, PostgreSQL, etc.)
"""

from .compression import ZstdCompressor, CompressionStats, compress, decompress, decompress_to_str
from .kafka_config import get_config, reset_config, StreamPipelineConfig

__all__ = [
    "ZstdCompressor",
    "CompressionStats",
    "compress",
    "decompress",
    "decompress_to_str",
    "get_config",
    "reset_config",
    "StreamPipelineConfig",
]

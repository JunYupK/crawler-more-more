"""
Zstd Compression Utilities
==========================

HTML 콘텐츠를 Zstd로 압축/해제하는 유틸리티
- Zstd: 빠른 압축 속도와 높은 압축률을 동시에 제공
- Level 3: 속도와 압축률의 균형점 (기본값)
"""

import zstandard as zstd
from typing import Union
import logging

logger = logging.getLogger(__name__)


class ZstdCompressor:
    """
    Zstd 압축/해제 유틸리티

    특징:
    - asyncio 안전: 단일 이벤트 루프 내에서 안전하게 재사용 가능
    - 스레드 안전하지 않음: 멀티스레드 환경에서는 인스턴스를 스레드별로 생성 필요
    - 스트리밍 지원: 대용량 데이터 처리 가능
    """

    def __init__(self, level: int = 3, threads: int = -1):
        """
        Args:
            level: 압축 레벨 (1-22, 기본값 3)
                   - 1-3: 빠른 압축 (크롤링에 적합)
                   - 10-15: 균형
                   - 19-22: 최대 압축 (저장용)
            threads: 멀티스레드 압축 (-1: 자동, 0: 싱글)
        """
        self.level = level
        self.threads = threads

        # 압축기/해제기 생성 (재사용을 위해 인스턴스 유지)
        self._compressor = zstd.ZstdCompressor(
            level=level,
            threads=threads if threads != -1 else 0,
        )
        self._decompressor = zstd.ZstdDecompressor()

        logger.debug(f"ZstdCompressor initialized: level={level}, threads={threads}")

    def compress(self, data: Union[str, bytes]) -> bytes:
        """
        데이터를 Zstd로 압축

        Args:
            data: 압축할 데이터 (str 또는 bytes)

        Returns:
            압축된 bytes
        """
        if isinstance(data, str):
            data = data.encode('utf-8')

        compressed = self._compressor.compress(data)

        # 압축률 로깅 (디버그용)
        original_size = len(data)
        compressed_size = len(compressed)
        ratio = (1 - compressed_size / original_size) * 100 if original_size > 0 else 0

        logger.debug(
            f"Compressed: {original_size:,} -> {compressed_size:,} bytes "
            f"({ratio:.1f}% reduction)"
        )

        return compressed

    def decompress(self, data: bytes) -> bytes:
        """
        Zstd 압축 해제

        Args:
            data: 압축된 bytes

        Returns:
            해제된 bytes
        """
        return self._decompressor.decompress(data)

    def decompress_to_str(self, data: bytes, encoding: str = 'utf-8') -> str:
        """
        Zstd 압축 해제 후 문자열로 변환

        Args:
            data: 압축된 bytes
            encoding: 문자 인코딩 (기본값: utf-8)

        Returns:
            해제된 문자열
        """
        decompressed = self.decompress(data)
        return decompressed.decode(encoding, errors='replace')

    def compress_stream(self, data: Union[str, bytes], chunk_size: int = 65536) -> bytes:
        """
        스트리밍 방식으로 대용량 데이터 압축

        Args:
            data: 압축할 데이터
            chunk_size: 청크 크기 (기본값: 64KB)

        Returns:
            압축된 bytes
        """
        if isinstance(data, str):
            data = data.encode('utf-8')

        from io import BytesIO
        output = BytesIO()
        with self._compressor.stream_writer(
            output, closefd=False, size=len(data)
        ) as writer:
            for i in range(0, len(data), chunk_size):
                writer.write(data[i:i + chunk_size])

        return output.getvalue()

    @staticmethod
    def get_compression_ratio(original: bytes, compressed: bytes) -> float:
        """
        압축률 계산

        Returns:
            압축률 (0.0 ~ 1.0, 높을수록 좋음)
        """
        if len(original) == 0:
            return 0.0
        return 1 - (len(compressed) / len(original))

    @staticmethod
    def estimate_decompressed_size(compressed: bytes) -> int:
        """
        압축 해제 후 예상 크기 반환
        Zstd 프레임 헤더에서 원본 크기를 읽음

        Args:
            compressed: 압축된 데이터

        Returns:
            예상 원본 크기 (알 수 없으면 -1)
        """
        try:
            return zstd.frame_content_size(compressed)
        except Exception:
            return -1


class CompressionStats:
    """압축 통계 수집"""

    def __init__(self):
        self.total_original_bytes = 0
        self.total_compressed_bytes = 0
        self.compression_count = 0

    def record(self, original_size: int, compressed_size: int):
        """압축 결과 기록"""
        self.total_original_bytes += original_size
        self.total_compressed_bytes += compressed_size
        self.compression_count += 1

    @property
    def average_ratio(self) -> float:
        """평균 압축률"""
        if self.total_original_bytes == 0:
            return 0.0
        return 1 - (self.total_compressed_bytes / self.total_original_bytes)

    @property
    def total_saved_bytes(self) -> int:
        """총 절약된 바이트"""
        return self.total_original_bytes - self.total_compressed_bytes

    def __str__(self) -> str:
        return (
            f"CompressionStats("
            f"count={self.compression_count}, "
            f"original={self.total_original_bytes:,}B, "
            f"compressed={self.total_compressed_bytes:,}B, "
            f"ratio={self.average_ratio:.1%}, "
            f"saved={self.total_saved_bytes:,}B)"
        )


# 싱글톤 인스턴스 (기본 설정)
_default_compressor: ZstdCompressor | None = None


def get_compressor(level: int = 3) -> ZstdCompressor:
    """기본 압축기 인스턴스 반환"""
    global _default_compressor
    if _default_compressor is None:
        _default_compressor = ZstdCompressor(level=level)
    return _default_compressor


def compress(data: Union[str, bytes], level: int = 3) -> bytes:
    """간편 압축 함수"""
    return get_compressor(level).compress(data)


def decompress(data: bytes) -> bytes:
    """간편 해제 함수"""
    return get_compressor().decompress(data)


def decompress_to_str(data: bytes, encoding: str = 'utf-8') -> str:
    """간편 해제 + 문자열 변환 함수"""
    return get_compressor().decompress_to_str(data, encoding)

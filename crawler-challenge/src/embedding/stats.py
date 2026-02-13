"""Embedding worker statistics model."""

import time
from dataclasses import dataclass, field


@dataclass
class EmbeddingStats:
    """임베딩 워커 통계"""

    messages_consumed: int = 0
    messages_skipped: int = 0
    messages_failed: int = 0
    chunks_created: int = 0
    chunks_stored: int = 0
    embed_time_ms: float = 0.0
    store_time_ms: float = 0.0
    start_time: float = field(default_factory=time.time)

    @property
    def messages_processed(self) -> int:
        return self.messages_consumed - self.messages_skipped - self.messages_failed

    @property
    def elapsed_sec(self) -> float:
        return time.time() - self.start_time

    @property
    def mps(self) -> float:
        return self.messages_consumed / self.elapsed_sec if self.elapsed_sec > 0 else 0.0

    def __str__(self) -> str:
        return (
            f"EmbeddingStats("
            f"consumed={self.messages_consumed:,}, "
            f"processed={self.messages_processed:,}, "
            f"skipped={self.messages_skipped:,}, "
            f"failed={self.messages_failed:,}, "
            f"chunks={self.chunks_stored:,}, "
            f"mps={self.mps:.1f})"
        )

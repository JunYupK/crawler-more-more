"""
PostgreSQL Writer - Metadata Storage
=====================================

페이지 메타데이터를 PostgreSQL에 저장
- 비동기 연결 풀 (asyncpg)
- 배치 삽입 최적화
- 중복 처리 (UPSERT)
"""

import asyncio
import logging
import time
from typing import Optional, Any
from dataclasses import dataclass, field
from datetime import datetime, timezone
from urllib.parse import urlparse

import asyncpg
from asyncpg import Pool, Connection

import sys
sys.path.insert(0, '/home/user/crawler-more-more/crawler-challenge')
from config.kafka_config import get_config, PostgresConfig

logger = logging.getLogger(__name__)


@dataclass
class PageRecord:
    """페이지 레코드"""
    url: str
    domain: str
    title: Optional[str] = None
    description: Optional[str] = None
    language: Optional[str] = None
    keywords: Optional[list[str]] = None

    # 처리 정보
    processor: str = "fast"  # fast or rich
    static_score: Optional[int] = None
    route_reason: Optional[str] = None

    # MinIO 참조
    raw_html_key: Optional[str] = None
    markdown_key: Optional[str] = None

    # 크기 정보
    raw_size_bytes: Optional[int] = None
    markdown_size_bytes: Optional[int] = None

    # HTTP 정보
    status_code: Optional[int] = None
    content_type: Optional[str] = None

    # 타임스탬프
    crawled_at: Optional[datetime] = None
    processed_at: Optional[datetime] = None

    @classmethod
    def from_processed_result(cls, result: dict, minio_keys: dict = None) -> "PageRecord":
        """ProcessedResult에서 생성"""
        url = result.get('url', '')
        parsed = urlparse(url)

        minio_keys = minio_keys or {}
        timestamps = result.get('timestamps', {})
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
            raw_html_key=minio_keys.get('raw_html'),
            markdown_key=minio_keys.get('markdown'),
            raw_size_bytes=stats.get('content_length'),
            markdown_size_bytes=stats.get('markdown_length'),
            crawled_at=datetime.fromtimestamp(
                timestamps.get('original', time.time()),
                tz=timezone.utc
            ) if timestamps.get('original') else None,
            processed_at=datetime.fromtimestamp(
                timestamps.get('processor', time.time()),
                tz=timezone.utc
            ) if timestamps.get('processor') else None,
        )


@dataclass
class PostgresStats:
    """PostgreSQL 저장 통계"""
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

    def __str__(self) -> str:
        return (
            f"PostgresStats("
            f"inserted={self.records_inserted:,}, "
            f"updated={self.records_updated:,}, "
            f"failed={self.records_failed:,}, "
            f"rps={self.records_per_second:.1f})"
        )


class PostgresWriter:
    """
    PostgreSQL 메타데이터 저장기

    페이지 메타데이터를 PostgreSQL에 저장
    - asyncpg 비동기 연결 풀
    - 배치 UPSERT
    - 자동 재연결
    """

    # 배치 삽입 쿼리
    UPSERT_QUERY = """
        INSERT INTO pages (
            url, domain, title, description, language, keywords,
            processor, static_score, route_reason,
            raw_html_key, markdown_key,
            raw_size_bytes, markdown_size_bytes,
            status_code, content_type,
            crawled_at, processed_at
        ) VALUES (
            $1, $2, $3, $4, $5, $6,
            $7, $8, $9,
            $10, $11,
            $12, $13,
            $14, $15,
            $16, $17
        )
        ON CONFLICT (url) DO UPDATE SET
            title = EXCLUDED.title,
            description = EXCLUDED.description,
            language = EXCLUDED.language,
            keywords = EXCLUDED.keywords,
            processor = EXCLUDED.processor,
            static_score = EXCLUDED.static_score,
            route_reason = EXCLUDED.route_reason,
            raw_html_key = EXCLUDED.raw_html_key,
            markdown_key = EXCLUDED.markdown_key,
            raw_size_bytes = EXCLUDED.raw_size_bytes,
            markdown_size_bytes = EXCLUDED.markdown_size_bytes,
            status_code = EXCLUDED.status_code,
            content_type = EXCLUDED.content_type,
            processed_at = EXCLUDED.processed_at,
            version = pages.version + 1
        RETURNING (xmax = 0) AS inserted
    """

    def __init__(
        self,
        config: Optional[PostgresConfig] = None,
        batch_size: int = 100,
        flush_interval: float = 5.0,
    ):
        """
        Args:
            config: PostgreSQL 설정
            batch_size: 배치 크기
            flush_interval: 자동 플러시 간격 (초)
        """
        self.config = config or get_config().postgres
        self.batch_size = batch_size
        self.flush_interval = flush_interval

        # 연결 풀
        self._pool: Optional[Pool] = None

        # 배치 버퍼
        self._buffer: list[PageRecord] = []
        self._buffer_lock = asyncio.Lock()
        self._last_flush = time.time()

        # 통계
        self.stats = PostgresStats()

        logger.info(
            f"PostgresWriter initialized: "
            f"host={self.config.host}, "
            f"db={self.config.database}, "
            f"batch_size={batch_size}"
        )

    async def start(self) -> None:
        """연결 풀 시작"""
        try:
            self._pool = await asyncpg.create_pool(
                dsn=self.config.dsn,
                min_size=self.config.min_connections,
                max_size=self.config.max_connections,
                command_timeout=30,
            )
            self.stats = PostgresStats()
            logger.info("PostgreSQL connection pool created")

        except Exception as e:
            logger.error(f"Failed to create PostgreSQL pool: {e}")
            raise

    async def stop(self) -> None:
        """연결 풀 종료"""
        # 남은 버퍼 플러시
        await self.flush()

        if self._pool:
            await self._pool.close()
            self._pool = None

        logger.info(f"PostgresWriter stopped. {self.stats}")

    async def __aenter__(self) -> "PostgresWriter":
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.stop()

    async def save(self, record: PageRecord) -> bool:
        """
        레코드 저장 (버퍼에 추가)

        Args:
            record: 저장할 레코드

        Returns:
            성공 여부 (버퍼 추가 기준)
        """
        async with self._buffer_lock:
            self._buffer.append(record)

            # 배치 크기 도달 시 플러시
            if len(self._buffer) >= self.batch_size:
                await self._flush_buffer()

            # 타임아웃 플러시
            elif time.time() - self._last_flush > self.flush_interval:
                await self._flush_buffer()

        return True

    async def save_batch(self, records: list[PageRecord]) -> tuple[int, int]:
        """
        배치 저장

        Args:
            records: 저장할 레코드 리스트

        Returns:
            (성공 수, 실패 수) 튜플
        """
        if not records:
            return 0, 0

        if not self._pool:
            logger.error("PostgreSQL pool not initialized")
            return 0, len(records)

        start_time = time.time()
        inserted = 0
        updated = 0
        failed = 0

        try:
            async with self._pool.acquire() as conn:
                # 배치 UPSERT
                for record in records:
                    try:
                        result = await conn.fetchrow(
                            self.UPSERT_QUERY,
                            record.url,
                            record.domain,
                            record.title,
                            record.description,
                            record.language,
                            record.keywords,
                            record.processor,
                            record.static_score,
                            record.route_reason,
                            record.raw_html_key,
                            record.markdown_key,
                            record.raw_size_bytes,
                            record.markdown_size_bytes,
                            record.status_code,
                            record.content_type,
                            record.crawled_at,
                            record.processed_at or datetime.now(timezone.utc),
                        )

                        if result and result['inserted']:
                            inserted += 1
                        else:
                            updated += 1

                    except Exception as e:
                        logger.error(f"Error saving record {record.url}: {e}")
                        failed += 1

            # 통계 업데이트
            elapsed_ms = (time.time() - start_time) * 1000
            self.stats.records_inserted += inserted
            self.stats.records_updated += updated
            self.stats.records_failed += failed
            self.stats.batch_count += 1
            self.stats.total_insert_time_ms += elapsed_ms

            logger.debug(
                f"Batch saved: inserted={inserted}, updated={updated}, "
                f"failed={failed}, time={elapsed_ms:.1f}ms"
            )

        except Exception as e:
            logger.error(f"Batch save error: {e}")
            failed = len(records)
            self.stats.records_failed += failed

        return inserted + updated, failed

    async def flush(self) -> None:
        """버퍼 강제 플러시"""
        async with self._buffer_lock:
            await self._flush_buffer()

    async def _flush_buffer(self) -> None:
        """내부 버퍼 플러시"""
        if not self._buffer:
            return

        records = self._buffer.copy()
        self._buffer.clear()
        self._last_flush = time.time()

        await self.save_batch(records)

    async def get_page(self, url: str) -> Optional[dict]:
        """
        페이지 조회

        Args:
            url: 조회할 URL

        Returns:
            페이지 데이터 또는 None
        """
        if not self._pool:
            return None

        try:
            async with self._pool.acquire() as conn:
                row = await conn.fetchrow(
                    "SELECT * FROM pages WHERE url = $1",
                    url
                )
                return dict(row) if row else None

        except Exception as e:
            logger.error(f"Error getting page {url}: {e}")
            return None

    async def get_stats_summary(self) -> dict:
        """
        통계 요약 조회

        Returns:
            데이터베이스 통계
        """
        if not self._pool:
            return {}

        try:
            async with self._pool.acquire() as conn:
                # 총 페이지 수
                total = await conn.fetchval("SELECT COUNT(*) FROM pages")

                # 프로세서별 통계
                processor_stats = await conn.fetch("""
                    SELECT processor, COUNT(*) as count
                    FROM pages
                    GROUP BY processor
                """)

                # 최근 크롤링
                recent = await conn.fetchval("""
                    SELECT COUNT(*) FROM pages
                    WHERE processed_at > NOW() - INTERVAL '1 hour'
                """)

                # 평균 점수
                avg_score = await conn.fetchval("""
                    SELECT AVG(static_score) FROM pages
                    WHERE static_score IS NOT NULL
                """)

                return {
                    'total_pages': total,
                    'processor_stats': {
                        row['processor']: row['count']
                        for row in processor_stats
                    },
                    'recent_hour': recent,
                    'average_score': float(avg_score) if avg_score else None,
                }

        except Exception as e:
            logger.error(f"Error getting stats: {e}")
            return {}

    async def update_crawl_stats(self) -> None:
        """
        시간별 통계 업데이트

        crawl_stats 테이블 갱신
        """
        if not self._pool:
            return

        try:
            async with self._pool.acquire() as conn:
                await conn.execute("""
                    INSERT INTO crawl_stats (
                        stat_hour,
                        total_crawled,
                        total_processed,
                        fast_processed,
                        rich_processed
                    )
                    SELECT
                        date_trunc('hour', NOW()),
                        COUNT(*),
                        COUNT(*),
                        SUM(CASE WHEN processor = 'fast' THEN 1 ELSE 0 END),
                        SUM(CASE WHEN processor = 'rich' THEN 1 ELSE 0 END)
                    FROM pages
                    WHERE processed_at > date_trunc('hour', NOW())
                    ON CONFLICT (stat_hour) DO UPDATE SET
                        total_processed = EXCLUDED.total_processed,
                        fast_processed = EXCLUDED.fast_processed,
                        rich_processed = EXCLUDED.rich_processed
                """)

        except Exception as e:
            logger.error(f"Error updating crawl stats: {e}")

    def get_stats(self) -> dict:
        """현재 통계 반환"""
        return {
            'records_inserted': self.stats.records_inserted,
            'records_updated': self.stats.records_updated,
            'records_failed': self.stats.records_failed,
            'total_records': self.stats.total_records,
            'batch_count': self.stats.batch_count,
            'records_per_second': self.stats.records_per_second,
            'average_insert_time_ms': self.stats.average_insert_time_ms,
            'buffer_size': len(self._buffer),
        }


async def test_postgres_connection(config: Optional[PostgresConfig] = None) -> dict:
    """PostgreSQL 연결 테스트"""
    config = config or get_config().postgres

    result = {
        'connected': False,
        'host': config.host,
        'database': config.database,
        'tables': [],
        'error': None,
    }

    try:
        conn = await asyncpg.connect(dsn=config.dsn)

        # 테이블 목록 조회
        tables = await conn.fetch("""
            SELECT tablename FROM pg_tables
            WHERE schemaname = 'public'
        """)
        result['tables'] = [t['tablename'] for t in tables]
        result['connected'] = True

        await conn.close()

    except Exception as e:
        result['error'] = str(e)

    return result

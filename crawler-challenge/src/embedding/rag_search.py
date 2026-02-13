"""
RAG Search - 벡터 유사도 검색 인터페이스
=========================================

pgvector를 활용한 의미 기반 문서 검색.
검색 결과를 LLM 프롬프트용 컨텍스트 문자열로 변환합니다.

사용 예시:
    async with RAGSearcher() as searcher:
        results = await searcher.search("크롤러 성능 최적화 방법")
        context = searcher.format_context(results)
        # LLM에 context 전달

환경변수:
  EMBED_BACKEND      : "local" | "openai" (기본: local)
  EMBED_MODEL_NAME   : 모델명
  POSTGRES_HOST/PORT/DB/USER/PASSWORD : PostgreSQL 접속 정보
"""

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Optional

import asyncpg

from src.common.kafka_config import get_config
from src.embedding.embedder import BaseEmbedder, create_embedder
from src.embedding.vector_dimension import (
    ensure_dimension_match,
    get_page_chunks_embedding_dimension,
)

logger = logging.getLogger(__name__)


@dataclass
class SearchResult:
    """벡터 검색 결과 단건"""
    url: str
    title: Optional[str]
    domain: str
    chunk_text: str
    heading_ctx: Optional[str]
    chunk_index: int
    similarity: float          # 1 - cosine_distance (0~1, 높을수록 유사)
    language: Optional[str] = None
    crawled_at: Optional[str] = None


# 검색 쿼리 (pgvector <=> 는 cosine distance, 낮을수록 유사)
SEARCH_QUERY = """
    SELECT
        c.url,
        c.chunk_text,
        c.heading_ctx,
        c.chunk_index,
        1 - (c.embedding <=> $1::vector) AS similarity,
        p.title,
        p.domain,
        p.language,
        p.crawled_at::TEXT AS crawled_at
    FROM page_chunks c
    LEFT JOIN pages p ON c.url = p.url
    WHERE c.embedding IS NOT NULL
      AND 1 - (c.embedding <=> $1::vector) >= $2
    ORDER BY c.embedding <=> $1::vector
    LIMIT $3
"""

SEARCH_QUERY_WITH_DOMAIN = """
    SELECT
        c.url,
        c.chunk_text,
        c.heading_ctx,
        c.chunk_index,
        1 - (c.embedding <=> $1::vector) AS similarity,
        p.title,
        p.domain,
        p.language,
        p.crawled_at::TEXT AS crawled_at
    FROM page_chunks c
    LEFT JOIN pages p ON c.url = p.url
    WHERE c.embedding IS NOT NULL
      AND 1 - (c.embedding <=> $1::vector) >= $2
      AND p.domain = $4
    ORDER BY c.embedding <=> $1::vector
    LIMIT $3
"""


class RAGSearcher:
    """
    pgvector 기반 의미 유사도 검색기

    동기식 embed + 비동기 DB 검색 혼합 구조.
    임베딩 모델은 초기화 시 한 번만 로드됩니다.
    """

    DEFAULT_TOP_K = 5
    DEFAULT_MIN_SIMILARITY = 0.3  # cosine similarity 최소값 (0~1)
    IVF_PROBES = 10               # IVFFlat 인덱스 탐색 범위 (정확도↑ vs 속도↓)

    def __init__(
        self,
        embedder: Optional[BaseEmbedder] = None,
        pg_dsn: Optional[str] = None,
    ):
        """
        Args:
            embedder: 임베딩 모델 (None이면 환경변수로 자동 선택)
            pg_dsn: PostgreSQL DSN (None이면 환경변수로 자동 설정)
        """
        config = get_config()
        self.pg_dsn = pg_dsn or config.postgres.dsn

        self._embedder: Optional[BaseEmbedder] = embedder
        self._pg_pool: Optional[asyncpg.Pool] = None

    async def start(self) -> None:
        """검색기 초기화 (모델 로드 + DB 연결)"""
        if self._embedder is None:
            logger.info("Loading embedding model for search...")
            self._embedder = create_embedder()
            logger.info(f"Embedder ready: {self._embedder.model_name}")

        self._pg_pool = await asyncpg.create_pool(
            dsn=self.pg_dsn,
            min_size=1,
            max_size=5,
            command_timeout=15,
        )

        async with self._pg_pool.acquire() as conn:
            db_dim = await get_page_chunks_embedding_dimension(conn)
        ensure_dimension_match(
            component="RAGSearcher",
            model_name=self._embedder.model_name,
            model_dimension=self._embedder.dimension,
            db_dimension=db_dim,
        )

        logger.info("RAGSearcher ready")

    async def stop(self) -> None:
        """리소스 해제"""
        if self._pg_pool:
            await self._pg_pool.close()
            self._pg_pool = None

    async def __aenter__(self) -> "RAGSearcher":
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.stop()

    async def search(
        self,
        query: str,
        top_k: int = DEFAULT_TOP_K,
        min_similarity: float = DEFAULT_MIN_SIMILARITY,
        domain_filter: Optional[str] = None,
    ) -> list[SearchResult]:
        """
        자연어 쿼리로 유사 청크 검색

        Args:
            query: 검색할 자연어 쿼리
            top_k: 반환할 최대 결과 수
            min_similarity: 최소 코사인 유사도 (0.0~1.0)
            domain_filter: 특정 도메인으로 검색 범위 제한 (예: "example.com")

        Returns:
            SearchResult 리스트 (유사도 내림차순 정렬)
        """
        if not self._pg_pool or not self._embedder:
            raise RuntimeError("RAGSearcher not started. Call start() first.")

        if not query.strip():
            return []

        # 1. 쿼리 임베딩 생성 (동기 → executor)
        loop = asyncio.get_running_loop()
        embeddings = await loop.run_in_executor(
            None,
            self._embedder.embed_batch,
            [query],
        )
        query_vector = embeddings[0]
        # pgvector text input: '[f1,f2,...]' 형식으로 명시적 변환
        vector_str = f"[{','.join(str(x) for x in query_vector)}]"

        # 2. pgvector 검색
        try:
            async with self._pg_pool.acquire() as conn:
                # IVFFlat 검색 정확도 설정
                await conn.execute(f"SET ivfflat.probes = {self.IVF_PROBES}")

                if domain_filter:
                    rows = await conn.fetch(
                        SEARCH_QUERY_WITH_DOMAIN,
                        vector_str,
                        min_similarity,
                        top_k,
                        domain_filter,
                    )
                else:
                    rows = await conn.fetch(
                        SEARCH_QUERY,
                        vector_str,
                        min_similarity,
                        top_k,
                    )

            return [
                SearchResult(
                    url=row['url'],
                    title=row['title'],
                    domain=row['domain'] or '',
                    chunk_text=row['chunk_text'],
                    heading_ctx=row['heading_ctx'],
                    chunk_index=row['chunk_index'],
                    similarity=float(row['similarity']),
                    language=row['language'],
                    crawled_at=row['crawled_at'],
                )
                for row in rows
            ]

        except Exception as e:
            logger.error(f"Search error for query '{query[:50]}': {e}", exc_info=True)
            return []

    def format_context(
        self,
        results: list[SearchResult],
        max_total_chars: int = 4000,
    ) -> str:
        """
        LLM 프롬프트용 컨텍스트 문자열 생성

        Args:
            results: search() 결과 목록
            max_total_chars: 전체 컨텍스트 최대 문자 수 (LLM 컨텍스트 윈도우 고려)

        Returns:
            포맷된 컨텍스트 문자열
        """
        if not results:
            return "관련 문서를 찾을 수 없습니다."

        parts: list[str] = []
        total_chars = 0

        for i, result in enumerate(results, 1):
            # 헤딩 컨텍스트 포함
            location = f"{result.heading_ctx} " if result.heading_ctx else ""
            header = (
                f"[{i}] {result.title or '제목 없음'} "
                f"({result.domain})\n"
                f"URL: {result.url}\n"
                f"위치: {location}(유사도: {result.similarity:.2f})"
            )
            section = f"{header}\n\n{result.chunk_text}"

            if total_chars + len(section) > max_total_chars:
                # 남은 공간에 맞게 잘라서 추가
                remaining = max_total_chars - total_chars - len(header) - 10
                if remaining > 100:
                    section = f"{header}\n\n{result.chunk_text[:remaining]}..."
                    parts.append(section)
                break

            parts.append(section)
            total_chars += len(section)

        return "\n\n---\n\n".join(parts)

    async def get_embedding_stats(self) -> dict:
        """
        임베딩 현황 통계 조회

        Returns:
            임베딩된 URL 수, 총 청크 수 등
        """
        if not self._pg_pool:
            return {}

        try:
            async with self._pg_pool.acquire() as conn:
                row = await conn.fetchrow("SELECT * FROM v_embedding_stats")
                if row:
                    return dict(row)
                return {}
        except Exception as e:
            logger.error(f"Error getting embedding stats: {e}")
            return {}

    async def search_by_url(self, url: str) -> list[SearchResult]:
        """
        특정 URL의 모든 청크를 순서대로 조회 (문서 내용 확인용)

        Args:
            url: 조회할 페이지 URL

        Returns:
            해당 URL의 SearchResult 리스트 (chunk_index 순)
        """
        if not self._pg_pool:
            return []

        try:
            async with self._pg_pool.acquire() as conn:
                rows = await conn.fetch(
                    """
                    SELECT c.*, p.title, p.domain, p.language, p.crawled_at::TEXT
                    FROM page_chunks c
                    LEFT JOIN pages p ON c.url = p.url
                    WHERE c.url = $1
                    ORDER BY c.chunk_index
                    """,
                    url,
                )
            return [
                SearchResult(
                    url=row['url'],
                    title=row['title'],
                    domain=row['domain'] or '',
                    chunk_text=row['chunk_text'],
                    heading_ctx=row['heading_ctx'],
                    chunk_index=row['chunk_index'],
                    similarity=1.0,  # 직접 조회이므로 유사도 1.0
                    language=row['language'],
                    crawled_at=row['crawled_at'],
                )
                for row in rows
            ]
        except Exception as e:
            logger.error(f"Error in search_by_url({url}): {e}")
            return []

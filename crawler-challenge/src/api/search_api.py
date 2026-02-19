"""
RAG Search API
==============

크롤링된 문서를 pgvector 코사인 유사도로 검색하는 HTTP API.

Endpoints:
  GET /health          - 서비스 상태 확인
  GET /search          - 자연어 벡터 검색
  GET /stats           - 임베딩 현황 통계
  GET /chunk           - 특정 URL의 모든 청크 조회

Docs: http://localhost:8600/docs (Swagger UI 자동 생성)

환경변수: EMBED_BACKEND, EMBED_MODEL_NAME, POSTGRES_*, (OPENAI_API_KEY)
"""

import logging
import time
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from src.embedding.rag_search import RAGSearcher

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────
# 싱글턴 검색기 (앱 수명 동안 공유)
# ──────────────────────────────────────────────
_searcher: Optional[RAGSearcher] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """앱 시작 시 RAGSearcher 초기화, 종료 시 리소스 해제"""
    global _searcher
    logger.info("Starting RAG Search API — loading embedder and DB pool...")
    _searcher = RAGSearcher()
    await _searcher.start()
    logger.info("RAG Search API ready")
    yield
    logger.info("Shutting down RAG Search API...")
    await _searcher.stop()
    _searcher = None


# ──────────────────────────────────────────────
# FastAPI App
# ──────────────────────────────────────────────
app = FastAPI(
    title="Crawler RAG Search API",
    description=(
        "크롤링된 웹 문서를 pgvector 코사인 유사도로 검색합니다.\n\n"
        "임베딩 백엔드: `EMBED_BACKEND=local` (sentence-transformers) "
        "또는 `EMBED_BACKEND=openai` (text-embedding-3-small)"
    ),
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


# ──────────────────────────────────────────────
# Response Models
# ──────────────────────────────────────────────
class ChunkResult(BaseModel):
    url: str
    title: Optional[str]
    domain: str
    chunk_text: str
    heading_ctx: Optional[str]
    chunk_index: int
    similarity: float
    language: Optional[str]
    crawled_at: Optional[str]


class SearchResponse(BaseModel):
    query: str
    top_k: int
    total: int
    elapsed_ms: float
    results: list[ChunkResult]


class StatsResponse(BaseModel):
    total_urls: Optional[int] = None
    total_chunks: Optional[int] = None
    avg_chunks_per_url: Optional[float] = None
    embed_backend: Optional[str] = None
    embed_model: Optional[str] = None


class HealthResponse(BaseModel):
    status: str          # "ok" | "degraded"
    db: str              # "connected" | "error"
    embedder: str        # 모델명 또는 "not loaded"


# ──────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────
def _require_searcher() -> RAGSearcher:
    if _searcher is None:
        raise HTTPException(status_code=503, detail="Search service not ready")
    return _searcher


# ──────────────────────────────────────────────
# Endpoints
# ──────────────────────────────────────────────
@app.get("/health", response_model=HealthResponse, tags=["System"])
async def health():
    """서비스 상태 및 DB 연결 확인"""
    searcher = _require_searcher()

    # DB 연결 확인: 간단한 stats 쿼리로 검증
    try:
        await searcher.get_embedding_stats()
        db_status = "connected"
        overall = "ok"
    except Exception as e:
        logger.warning(f"Health check DB error: {e}")
        db_status = "error"
        overall = "degraded"

    embedder_name = (
        searcher._embedder.model_name if searcher._embedder else "not loaded"
    )

    return HealthResponse(status=overall, db=db_status, embedder=embedder_name)


@app.get("/search", response_model=SearchResponse, tags=["Search"])
async def search(
    q: str = Query(..., description="검색할 자연어 쿼리"),
    top_k: int = Query(5, ge=1, le=50, description="반환할 최대 결과 수"),
    domain: Optional[str] = Query(None, description="도메인 필터 (예: example.com)"),
    min_similarity: float = Query(0.3, ge=0.0, le=1.0, description="최소 코사인 유사도"),
):
    """
    자연어 쿼리로 크롤링된 문서에서 의미적으로 유사한 청크를 검색합니다.

    - **q**: 검색어 (자연어)
    - **top_k**: 반환 결과 수 (1~50)
    - **domain**: 특정 도메인으로 검색 범위 제한
    - **min_similarity**: 0.0(유사도 무관)~1.0(완전 일치)
    """
    if not q.strip():
        raise HTTPException(status_code=400, detail="Query cannot be empty")

    searcher = _require_searcher()

    t0 = time.monotonic()
    results = await searcher.search(
        query=q,
        top_k=top_k,
        min_similarity=min_similarity,
        domain_filter=domain,
    )
    elapsed_ms = (time.monotonic() - t0) * 1000

    return SearchResponse(
        query=q,
        top_k=top_k,
        total=len(results),
        elapsed_ms=round(elapsed_ms, 2),
        results=[
            ChunkResult(
                url=r.url,
                title=r.title,
                domain=r.domain,
                chunk_text=r.chunk_text,
                heading_ctx=r.heading_ctx,
                chunk_index=r.chunk_index,
                similarity=round(r.similarity, 4),
                language=r.language,
                crawled_at=r.crawled_at,
            )
            for r in results
        ],
    )


@app.get("/stats", response_model=StatsResponse, tags=["Search"])
async def stats():
    """임베딩 현황 통계: 임베딩된 URL 수, 총 청크 수, 평균 청크 수"""
    searcher = _require_searcher()

    try:
        raw = await searcher.get_embedding_stats()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Stats query failed: {e}")

    import os
    return StatsResponse(
        total_urls=raw.get("total_urls"),
        total_chunks=raw.get("total_chunks"),
        avg_chunks_per_url=raw.get("avg_chunks_per_url"),
        embed_backend=os.getenv("EMBED_BACKEND", "local"),
        embed_model=searcher._embedder.model_name if searcher._embedder else None,
    )


@app.get("/chunk", response_model=list[ChunkResult], tags=["Search"])
async def chunk_by_url(
    url: str = Query(..., description="조회할 페이지 URL"),
):
    """
    특정 URL의 모든 청크를 chunk_index 순서대로 반환합니다.

    임베딩된 문서의 원문 내용 확인 용도.
    """
    searcher = _require_searcher()

    results = await searcher.search_by_url(url)
    if not results:
        raise HTTPException(
            status_code=404,
            detail=f"No chunks found for URL: {url}",
        )

    return [
        ChunkResult(
            url=r.url,
            title=r.title,
            domain=r.domain,
            chunk_text=r.chunk_text,
            heading_ctx=r.heading_ctx,
            chunk_index=r.chunk_index,
            similarity=r.similarity,
            language=r.language,
            crawled_at=r.crawled_at,
        )
        for r in results
    ]

-- Migration 002: pgvector 확장 및 page_chunks 테이블 추가
-- RAG(Retrieval-Augmented Generation)를 위한 벡터 임베딩 저장소
--
-- 실행 방법:
--   psql -U crawler -d crawler_stream -f 002_add_pgvector.sql
--
-- 필요 조건:
--   PostgreSQL 14+ with pgvector extension
--   Docker: ankane/pgvector 이미지 또는 pgvector 설치 완료

-- ============================================
-- pgvector 확장 활성화
-- ============================================
CREATE EXTENSION IF NOT EXISTS vector;

-- ============================================
-- page_chunks: 청크별 임베딩 저장 테이블
-- ============================================
CREATE TABLE IF NOT EXISTS page_chunks (
    id          BIGSERIAL PRIMARY KEY,

    -- pages 테이블 참조 (URL이 아직 pages에 없을 수 있으므로 NULL 허용)
    page_id     BIGINT REFERENCES pages(id) ON DELETE CASCADE,

    -- 식별자
    url         TEXT NOT NULL,
    chunk_index INT  NOT NULL,  -- 문서 내 청크 순서 (0-based)

    -- 청크 내용
    chunk_text  TEXT NOT NULL,  -- 임베딩 대상 원본 텍스트
    heading_ctx TEXT,           -- 청크 위치 힌트 ("H1 제목 > H2 소제목")

    -- 임베딩 벡터
    -- all-MiniLM-L6-v2: 384차원 (기본값)
    -- all-mpnet-base-v2: 768차원
    -- text-embedding-3-small: 1536차원
    embedding   vector(384),

    -- 타임스탬프
    created_at  TIMESTAMPTZ DEFAULT NOW(),

    -- 동일 URL의 같은 인덱스 청크는 UPSERT (재처리 안전)
    UNIQUE (url, chunk_index)
);

-- ============================================
-- 인덱스
-- ============================================

-- URL 기반 조회 (청크 전체 삭제/조회 시)
CREATE INDEX IF NOT EXISTS idx_page_chunks_url
    ON page_chunks(url);

-- page_id 기반 조회 (pages JOIN 시)
CREATE INDEX IF NOT EXISTS idx_page_chunks_page_id
    ON page_chunks(page_id);

-- IVFFlat 벡터 인덱스 (ANN: Approximate Nearest Neighbor)
-- lists = 100: 약 100만 행까지 권장 (행수 증가 시 sqrt(N)으로 조정)
-- probes 설정은 검색 시 SET ivfflat.probes = 10; 으로 제어
CREATE INDEX IF NOT EXISTS idx_page_chunks_embedding
    ON page_chunks USING ivfflat (embedding vector_cosine_ops)
    WITH (lists = 100);

-- ============================================
-- 편의 뷰: 청크 + 페이지 메타데이터 조인
-- ============================================
CREATE OR REPLACE VIEW v_chunk_search AS
    SELECT
        c.id          AS chunk_id,
        c.url,
        c.chunk_index,
        c.chunk_text,
        c.heading_ctx,
        c.embedding,
        p.title,
        p.domain,
        p.language,
        p.description,
        p.markdown_key,
        p.crawled_at
    FROM page_chunks c
    LEFT JOIN pages p ON c.url = p.url;

-- ============================================
-- 통계 뷰: 임베딩 현황
-- ============================================
CREATE OR REPLACE VIEW v_embedding_stats AS
    SELECT
        COUNT(DISTINCT url)                   AS urls_embedded,
        COUNT(*)                              AS total_chunks,
        AVG(array_length(embedding::float4[], 1))  AS avg_dimension,
        MIN(created_at)                       AS first_embedded_at,
        MAX(created_at)                       AS last_embedded_at
    FROM page_chunks
    WHERE embedding IS NOT NULL;

-- ============================================
-- 마이그레이션 완료 메모
-- ============================================
DO $$
BEGIN
    RAISE NOTICE 'Migration 002: pgvector + page_chunks table created successfully.';
    RAISE NOTICE 'Vector dimension: 384 (all-MiniLM-L6-v2 default).';
    RAISE NOTICE 'To change dimension, recreate embedding column with ALTER TABLE.';
END $$;

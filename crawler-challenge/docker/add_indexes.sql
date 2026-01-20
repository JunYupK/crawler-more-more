-- PostgreSQL 인덱스 추가 스크립트
-- 크롤링 데이터가 100만 건 이상일 때 쿼리 성능 향상을 위한 인덱스

-- 기존 인덱스 확인
SELECT
    schemaname,
    tablename,
    indexname,
    indexdef
FROM pg_indexes
WHERE tablename IN ('crawled_pages', 'crawler_dlq')
ORDER BY tablename, indexname;

-- 1. crawled_pages.created_at 인덱스 (이미 있을 수 있음)
-- 최근 데이터 조회 쿼리 최적화 (WHERE created_at > NOW() - INTERVAL ...)
CREATE INDEX IF NOT EXISTS idx_crawled_pages_created_at
ON crawled_pages(created_at DESC);

-- 2. crawled_pages.domain 인덱스
-- 도메인별 집계 쿼리 최적화 (GROUP BY domain)
CREATE INDEX IF NOT EXISTS idx_crawled_pages_domain
ON crawled_pages(domain);

-- 3. crawled_pages 복합 인덱스 (created_at + domain)
-- 최근 데이터의 도메인별 집계 최적화
CREATE INDEX IF NOT EXISTS idx_crawled_pages_created_at_domain
ON crawled_pages(created_at DESC, domain);

-- 4. crawler_dlq.created_at 인덱스 (DLQ가 있는 경우)
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'crawler_dlq') THEN
        CREATE INDEX IF NOT EXISTS idx_crawler_dlq_created_at
        ON crawler_dlq(created_at DESC);
    END IF;
END $$;

-- 5. crawler_dlq.error_type 인덱스 (DLQ가 있는 경우)
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'crawler_dlq') THEN
        CREATE INDEX IF NOT EXISTS idx_crawler_dlq_error_type
        ON crawler_dlq(error_type);
    END IF;
END $$;

-- 인덱스 생성 후 통계 업데이트
ANALYZE crawled_pages;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'crawler_dlq') THEN
        EXECUTE 'ANALYZE crawler_dlq';
    END IF;
END $$;

-- 생성된 인덱스 확인
SELECT
    schemaname,
    tablename,
    indexname,
    pg_size_pretty(pg_relation_size(indexrelid)) as index_size
FROM pg_stat_user_indexes
WHERE schemaname = 'public' AND tablename IN ('crawled_pages', 'crawler_dlq')
ORDER BY tablename, indexname;

-- 성능 향상 예상치 출력
SELECT
    'Index creation completed!' as status,
    (SELECT COUNT(*) FROM crawled_pages) as total_pages,
    (SELECT COUNT(DISTINCT domain) FROM crawled_pages) as total_domains;

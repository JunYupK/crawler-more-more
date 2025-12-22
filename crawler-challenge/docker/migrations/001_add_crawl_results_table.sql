-- 001_add_crawl_results_table.sql
-- 크롤링 결과 및 에러 모니터링을 위한 스키마 확장
-- 실행 방법: psql -U postgres -d crawler_db -f 001_add_crawl_results_table.sql

-- ============================================================
-- 1. crawl_results 테이블 생성 (크롤링 시도 기록)
-- ============================================================
CREATE TABLE IF NOT EXISTS crawl_results (
    id BIGSERIAL PRIMARY KEY,
    url TEXT NOT NULL,
    domain TEXT NOT NULL,
    success BOOLEAN NOT NULL DEFAULT FALSE,

    -- 에러 정보
    error_type VARCHAR(50),          -- timeout, ssl_error, connection_error, http_403, etc.
    error_message TEXT,
    status_code INTEGER,

    -- 성능 정보
    response_time FLOAT,             -- 응답 시간 (초)
    content_length INTEGER,

    -- 메타 정보
    worker_id INTEGER,
    shard_id INTEGER,
    attempt INTEGER DEFAULT 1,       -- 재시도 횟수

    -- 타임스탬프
    crawl_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- 추가 정보 (JSON)
    extra_info JSONB
);

-- ============================================================
-- 2. 인덱스 추가 (쿼리 성능 최적화)
-- ============================================================

-- 에러 분석용 인덱스
CREATE INDEX IF NOT EXISTS idx_crawl_results_error_type
ON crawl_results(error_type);

CREATE INDEX IF NOT EXISTS idx_crawl_results_success
ON crawl_results(success);

CREATE INDEX IF NOT EXISTS idx_crawl_results_domain
ON crawl_results(domain);

-- 시간 기반 분석용 인덱스
CREATE INDEX IF NOT EXISTS idx_crawl_results_crawl_time
ON crawl_results(crawl_time);

-- 복합 인덱스 (에러 분석)
CREATE INDEX IF NOT EXISTS idx_crawl_results_error_analysis
ON crawl_results(success, error_type, crawl_time);

-- 도메인별 분석 복합 인덱스
CREATE INDEX IF NOT EXISTS idx_crawl_results_domain_analysis
ON crawl_results(domain, success, crawl_time);

-- ============================================================
-- 3. 에러 통계 뷰 생성
-- ============================================================

-- 에러 타입별 통계 뷰
CREATE OR REPLACE VIEW crawl_error_stats AS
SELECT
    error_type,
    COUNT(*) as count,
    ROUND(
        COUNT(*) * 100.0 / NULLIF((SELECT COUNT(*) FROM crawl_results WHERE success = false), 0),
        2
    ) as percentage,
    ROUND(AVG(response_time)::numeric, 2) as avg_response_time,
    ROUND(MAX(response_time)::numeric, 2) as max_response_time,
    MIN(crawl_time) as first_seen,
    MAX(crawl_time) as last_seen
FROM crawl_results
WHERE success = false AND error_type IS NOT NULL
GROUP BY error_type
ORDER BY count DESC;

-- 도메인별 실패율 뷰
CREATE OR REPLACE VIEW domain_failure_stats AS
SELECT
    domain,
    COUNT(*) as total_attempts,
    SUM(CASE WHEN success THEN 1 ELSE 0 END) as successes,
    SUM(CASE WHEN success THEN 0 ELSE 1 END) as failures,
    ROUND(
        SUM(CASE WHEN success THEN 0 ELSE 1 END) * 100.0 / NULLIF(COUNT(*), 0),
        2
    ) as failure_rate,
    ROUND(AVG(response_time)::numeric, 2) as avg_response_time,
    array_agg(DISTINCT error_type) FILTER (WHERE error_type IS NOT NULL) as error_types
FROM crawl_results
GROUP BY domain
HAVING COUNT(*) >= 3
ORDER BY failure_rate DESC;

-- 시간대별 성공률 뷰
CREATE OR REPLACE VIEW hourly_success_stats AS
SELECT
    DATE_TRUNC('hour', crawl_time) as hour,
    COUNT(*) as total,
    SUM(CASE WHEN success THEN 1 ELSE 0 END) as successes,
    SUM(CASE WHEN success THEN 0 ELSE 1 END) as failures,
    ROUND(
        SUM(CASE WHEN success THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0),
        2
    ) as success_rate,
    ROUND(AVG(response_time)::numeric, 2) as avg_response_time
FROM crawl_results
GROUP BY DATE_TRUNC('hour', crawl_time)
ORDER BY hour DESC;

-- 워커별 성능 통계 뷰
CREATE OR REPLACE VIEW worker_performance_stats AS
SELECT
    worker_id,
    COUNT(*) as total_requests,
    SUM(CASE WHEN success THEN 1 ELSE 0 END) as successes,
    SUM(CASE WHEN success THEN 0 ELSE 1 END) as failures,
    ROUND(
        SUM(CASE WHEN success THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0),
        2
    ) as success_rate,
    ROUND(AVG(response_time)::numeric, 2) as avg_response_time,
    ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY response_time)::numeric, 2) as p95_response_time
FROM crawl_results
WHERE worker_id IS NOT NULL
GROUP BY worker_id
ORDER BY worker_id;

-- ============================================================
-- 4. 유용한 함수 생성
-- ============================================================

-- 최근 N분간 에러 통계 함수
CREATE OR REPLACE FUNCTION get_recent_error_stats(minutes INTEGER DEFAULT 60)
RETURNS TABLE (
    error_type VARCHAR,
    count BIGINT,
    percentage NUMERIC,
    avg_response_time NUMERIC
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        cr.error_type,
        COUNT(*)::BIGINT as count,
        ROUND(COUNT(*) * 100.0 / NULLIF(SUM(COUNT(*)) OVER (), 0), 2) as percentage,
        ROUND(AVG(cr.response_time)::numeric, 2) as avg_response_time
    FROM crawl_results cr
    WHERE cr.success = false
      AND cr.crawl_time > NOW() - (minutes || ' minutes')::INTERVAL
      AND cr.error_type IS NOT NULL
    GROUP BY cr.error_type
    ORDER BY count DESC;
END;
$$ LANGUAGE plpgsql;

-- 도메인 상태 요약 함수
CREATE OR REPLACE FUNCTION get_domain_status(domain_name TEXT)
RETURNS TABLE (
    total_attempts BIGINT,
    successes BIGINT,
    failures BIGINT,
    failure_rate NUMERIC,
    last_success TIMESTAMP,
    last_failure TIMESTAMP,
    common_errors TEXT[]
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        COUNT(*)::BIGINT as total_attempts,
        SUM(CASE WHEN success THEN 1 ELSE 0 END)::BIGINT as successes,
        SUM(CASE WHEN success THEN 0 ELSE 1 END)::BIGINT as failures,
        ROUND(SUM(CASE WHEN success THEN 0 ELSE 1 END) * 100.0 / NULLIF(COUNT(*), 0), 2) as failure_rate,
        MAX(CASE WHEN success THEN crawl_time END) as last_success,
        MAX(CASE WHEN NOT success THEN crawl_time END) as last_failure,
        array_agg(DISTINCT error_type) FILTER (WHERE error_type IS NOT NULL) as common_errors
    FROM crawl_results
    WHERE domain = domain_name;
END;
$$ LANGUAGE plpgsql;

-- ============================================================
-- 5. 코멘트 추가
-- ============================================================
COMMENT ON TABLE crawl_results IS '크롤링 시도 결과 기록 (성공/실패 모두 포함)';
COMMENT ON COLUMN crawl_results.error_type IS '에러 타입: timeout, ssl_error, connection_error, http_403, http_404, http_429, http_5xx, dns_error, robots_blocked, unknown';
COMMENT ON COLUMN crawl_results.response_time IS '요청부터 응답까지 소요 시간 (초)';
COMMENT ON COLUMN crawl_results.attempt IS '해당 URL에 대한 시도 횟수';

COMMENT ON VIEW crawl_error_stats IS '에러 타입별 통계 집계';
COMMENT ON VIEW domain_failure_stats IS '도메인별 실패율 통계 (3회 이상 시도된 도메인)';
COMMENT ON VIEW hourly_success_stats IS '시간대별 성공률 추이';
COMMENT ON VIEW worker_performance_stats IS '워커별 성능 통계';

-- analyze_failures.sql
-- 크롤링 실패 원인 분석을 위한 쿼리 모음
-- 사용법: psql -U postgres -d crawler_db -f analyze_failures.sql

\echo '============================================================'
\echo '         CRAWL FAILURE ANALYSIS REPORT'
\echo '============================================================'
\echo ''

-- 1. 전체 통계 요약
\echo '1. Overall Statistics'
\echo '------------------------------------------------------------'
SELECT
    COUNT(*) as total_requests,
    SUM(CASE WHEN success THEN 1 ELSE 0 END) as successes,
    SUM(CASE WHEN success THEN 0 ELSE 1 END) as failures,
    ROUND(SUM(CASE WHEN success THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) as success_rate_pct,
    ROUND(AVG(response_time)::numeric, 2) as avg_response_time_sec
FROM crawl_results;

\echo ''

-- 2. 에러 타입별 분포
\echo '2. Error Type Distribution'
\echo '------------------------------------------------------------'
SELECT
    error_type,
    COUNT(*) as count,
    ROUND(COUNT(*) * 100.0 / NULLIF((SELECT COUNT(*) FROM crawl_results WHERE success = false), 0), 2) as percentage
FROM crawl_results
WHERE success = false
GROUP BY error_type
ORDER BY count DESC;

\echo ''

-- 3. 실패율 높은 도메인 Top 20
\echo '3. Top 20 Failing Domains'
\echo '------------------------------------------------------------'
SELECT
    domain,
    COUNT(*) as attempts,
    SUM(CASE WHEN success THEN 0 ELSE 1 END) as failures,
    ROUND(SUM(CASE WHEN success THEN 0 ELSE 1 END) * 100.0 / NULLIF(COUNT(*), 0), 2) as failure_rate_pct
FROM crawl_results
GROUP BY domain
HAVING COUNT(*) >= 3
ORDER BY failure_rate_pct DESC
LIMIT 20;

\echo ''

-- 4. 시간대별 성공률 (최근 24시간)
\echo '4. Hourly Success Rate (Last 24 Hours)'
\echo '------------------------------------------------------------'
SELECT
    DATE_TRUNC('hour', crawl_time) as hour,
    COUNT(*) as total,
    SUM(CASE WHEN success THEN 1 ELSE 0 END) as successes,
    ROUND(SUM(CASE WHEN success THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) as success_rate_pct
FROM crawl_results
WHERE crawl_time > NOW() - INTERVAL '24 hours'
GROUP BY DATE_TRUNC('hour', crawl_time)
ORDER BY hour DESC;

\echo ''

-- 5. 응답 시간 통계 (성공 vs 실패)
\echo '5. Response Time Statistics (Success vs Failure)'
\echo '------------------------------------------------------------'
SELECT
    CASE WHEN success THEN 'Success' ELSE 'Failure' END as status,
    COUNT(*) as count,
    ROUND(AVG(response_time)::numeric, 2) as avg_time_sec,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY response_time)::numeric, 2) as median_sec,
    ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY response_time)::numeric, 2) as p95_sec,
    ROUND(MAX(response_time)::numeric, 2) as max_sec
FROM crawl_results
WHERE response_time IS NOT NULL
GROUP BY success
ORDER BY success DESC;

\echo ''

-- 6. Timeout 발생 도메인 Top 20
\echo '6. Top 20 Timeout Domains'
\echo '------------------------------------------------------------'
SELECT
    domain,
    COUNT(*) as timeout_count,
    ROUND(AVG(response_time)::numeric, 2) as avg_time_before_timeout
FROM crawl_results
WHERE error_type = 'timeout'
GROUP BY domain
ORDER BY timeout_count DESC
LIMIT 20;

\echo ''

-- 7. HTTP 상태 코드 분포
\echo '7. HTTP Status Code Distribution'
\echo '------------------------------------------------------------'
SELECT
    status_code,
    COUNT(*) as count,
    ROUND(COUNT(*) * 100.0 / NULLIF((SELECT COUNT(*) FROM crawl_results WHERE status_code IS NOT NULL), 0), 2) as percentage
FROM crawl_results
WHERE status_code IS NOT NULL
GROUP BY status_code
ORDER BY count DESC;

\echo ''

-- 8. 워커별 성능 비교
\echo '8. Worker Performance Comparison'
\echo '------------------------------------------------------------'
SELECT
    worker_id,
    COUNT(*) as total,
    SUM(CASE WHEN success THEN 1 ELSE 0 END) as successes,
    ROUND(SUM(CASE WHEN success THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) as success_rate_pct,
    ROUND(AVG(response_time)::numeric, 2) as avg_response_time
FROM crawl_results
WHERE worker_id IS NOT NULL
GROUP BY worker_id
ORDER BY worker_id;

\echo ''

-- 9. 최근 1시간 에러 추이
\echo '9. Recent Error Trend (Last Hour, 5-min buckets)'
\echo '------------------------------------------------------------'
SELECT
    DATE_TRUNC('minute', crawl_time) - (EXTRACT(MINUTE FROM crawl_time)::INT % 5) * INTERVAL '1 minute' as time_bucket,
    COUNT(*) as total,
    SUM(CASE WHEN success THEN 0 ELSE 1 END) as failures,
    ROUND(SUM(CASE WHEN success THEN 0 ELSE 1 END) * 100.0 / NULLIF(COUNT(*), 0), 2) as failure_rate_pct
FROM crawl_results
WHERE crawl_time > NOW() - INTERVAL '1 hour'
GROUP BY time_bucket
ORDER BY time_bucket DESC;

\echo ''

-- 10. 연속 실패 도메인 (재시도 대상)
\echo '10. Domains with Consecutive Failures (Retry Candidates)'
\echo '------------------------------------------------------------'
WITH recent_failures AS (
    SELECT
        domain,
        COUNT(*) as failure_count,
        MAX(crawl_time) as last_failure,
        array_agg(DISTINCT error_type) as error_types
    FROM crawl_results
    WHERE success = false
      AND crawl_time > NOW() - INTERVAL '6 hours'
    GROUP BY domain
    HAVING COUNT(*) >= 3
)
SELECT * FROM recent_failures
ORDER BY failure_count DESC
LIMIT 20;

\echo ''
\echo '============================================================'
\echo '                    END OF REPORT'
\echo '============================================================'

-- Stream Pipeline Database Schema
-- PostgreSQL 15+

-- ============================================
-- Extension
-- ============================================
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";  -- For text search

-- ============================================
-- Main Table: pages_v2 (Metadata Only)
-- ============================================
CREATE TABLE IF NOT EXISTS pages (
    id BIGSERIAL PRIMARY KEY,
    url TEXT UNIQUE NOT NULL,
    domain TEXT NOT NULL,

    -- Metadata
    title TEXT,
    description TEXT,
    language VARCHAR(10),
    keywords TEXT[],

    -- Processing Info
    processor VARCHAR(10) NOT NULL CHECK (processor IN ('fast', 'rich')),
    static_score INT CHECK (static_score >= 0 AND static_score <= 100),
    route_reason TEXT,

    -- Content References (MinIO)
    raw_html_key TEXT,           -- MinIO key for raw HTML
    markdown_key TEXT NOT NULL,  -- MinIO key for processed markdown

    -- Size Info
    raw_size_bytes BIGINT,
    markdown_size_bytes BIGINT,

    -- HTTP Info
    status_code INT,
    content_type TEXT,

    -- Timestamps
    crawled_at TIMESTAMP WITH TIME ZONE NOT NULL,
    processed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    -- Versioning
    version INT DEFAULT 1
);

-- ============================================
-- Indexes
-- ============================================
CREATE INDEX IF NOT EXISTS idx_pages_domain ON pages(domain);
CREATE INDEX IF NOT EXISTS idx_pages_processor ON pages(processor);
CREATE INDEX IF NOT EXISTS idx_pages_crawled_at ON pages(crawled_at);
CREATE INDEX IF NOT EXISTS idx_pages_static_score ON pages(static_score);
CREATE INDEX IF NOT EXISTS idx_pages_title_trgm ON pages USING gin(title gin_trgm_ops);

-- ============================================
-- Statistics Table
-- ============================================
CREATE TABLE IF NOT EXISTS crawl_stats (
    id SERIAL PRIMARY KEY,
    stat_hour TIMESTAMP WITH TIME ZONE NOT NULL,

    -- Counts
    total_crawled BIGINT DEFAULT 0,
    total_processed BIGINT DEFAULT 0,
    fast_processed BIGINT DEFAULT 0,
    rich_processed BIGINT DEFAULT 0,

    -- Failures
    crawl_failures BIGINT DEFAULT 0,
    process_failures BIGINT DEFAULT 0,

    -- Scores
    avg_static_score DECIMAL(5,2),
    min_static_score INT,
    max_static_score INT,

    -- Performance
    avg_crawl_time_ms DECIMAL(10,2),
    avg_process_time_ms DECIMAL(10,2),

    -- Storage
    total_raw_bytes BIGINT DEFAULT 0,
    total_markdown_bytes BIGINT DEFAULT 0,

    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    UNIQUE(stat_hour)
);

CREATE INDEX IF NOT EXISTS idx_crawl_stats_hour ON crawl_stats(stat_hour);

-- ============================================
-- DLQ Tables (Dead Letter Queue)
-- ============================================
CREATE TABLE IF NOT EXISTS crawl_dlq (
    id BIGSERIAL PRIMARY KEY,
    url TEXT NOT NULL,
    error_type VARCHAR(50) NOT NULL,
    error_message TEXT,
    retry_count INT DEFAULT 0,
    original_data JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    last_retry_at TIMESTAMP WITH TIME ZONE
);

CREATE TABLE IF NOT EXISTS process_dlq (
    id BIGSERIAL PRIMARY KEY,
    url TEXT NOT NULL,
    processor VARCHAR(10),
    error_type VARCHAR(50) NOT NULL,
    error_message TEXT,
    retry_count INT DEFAULT 0,
    original_data JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    last_retry_at TIMESTAMP WITH TIME ZONE
);

CREATE INDEX IF NOT EXISTS idx_crawl_dlq_created ON crawl_dlq(created_at);
CREATE INDEX IF NOT EXISTS idx_process_dlq_created ON process_dlq(created_at);

-- ============================================
-- Domain Statistics
-- ============================================
CREATE TABLE IF NOT EXISTS domain_stats (
    domain TEXT PRIMARY KEY,
    page_count INT DEFAULT 0,
    avg_score DECIMAL(5,2),
    dominant_processor VARCHAR(10),
    last_crawled_at TIMESTAMP WITH TIME ZONE,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- ============================================
-- Views
-- ============================================
CREATE OR REPLACE VIEW v_hourly_stats AS
SELECT
    stat_hour,
    total_crawled,
    total_processed,
    fast_processed,
    rich_processed,
    ROUND(fast_processed::numeric / NULLIF(total_processed, 0) * 100, 2) as fast_ratio,
    avg_static_score,
    total_raw_bytes / 1024 / 1024 as total_raw_mb,
    total_markdown_bytes / 1024 / 1024 as total_markdown_mb
FROM crawl_stats
ORDER BY stat_hour DESC;

CREATE OR REPLACE VIEW v_top_domains AS
SELECT
    domain,
    COUNT(*) as page_count,
    ROUND(AVG(static_score), 2) as avg_score,
    SUM(CASE WHEN processor = 'fast' THEN 1 ELSE 0 END) as fast_count,
    SUM(CASE WHEN processor = 'rich' THEN 1 ELSE 0 END) as rich_count
FROM pages
GROUP BY domain
ORDER BY page_count DESC
LIMIT 100;

-- ============================================
-- Functions
-- ============================================

-- Function to update hourly stats
CREATE OR REPLACE FUNCTION update_hourly_stats()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO crawl_stats (stat_hour, total_processed, fast_processed, rich_processed)
    VALUES (
        date_trunc('hour', NEW.processed_at),
        1,
        CASE WHEN NEW.processor = 'fast' THEN 1 ELSE 0 END,
        CASE WHEN NEW.processor = 'rich' THEN 1 ELSE 0 END
    )
    ON CONFLICT (stat_hour) DO UPDATE SET
        total_processed = crawl_stats.total_processed + 1,
        fast_processed = crawl_stats.fast_processed + CASE WHEN NEW.processor = 'fast' THEN 1 ELSE 0 END,
        rich_processed = crawl_stats.rich_processed + CASE WHEN NEW.processor = 'rich' THEN 1 ELSE 0 END;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger for auto stats update
DROP TRIGGER IF EXISTS trg_update_stats ON pages;
CREATE TRIGGER trg_update_stats
    AFTER INSERT ON pages
    FOR EACH ROW
    EXECUTE FUNCTION update_hourly_stats();

-- ============================================
-- Initial Data
-- ============================================
INSERT INTO crawl_stats (stat_hour, total_crawled, total_processed)
VALUES (date_trunc('hour', NOW()), 0, 0)
ON CONFLICT DO NOTHING;

-- Grant permissions
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO crawler;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO crawler;

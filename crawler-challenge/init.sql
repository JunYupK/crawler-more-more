-- 크롤링 페이지 데이터를 저장할 테이블
CREATE TABLE IF NOT EXISTS crawled_pages (
    id SERIAL PRIMARY KEY,
    url TEXT NOT NULL UNIQUE,
    domain VARCHAR(255) NOT NULL,
    title TEXT,
    content_text TEXT,
    metadata JSONB DEFAULT '{}',
    crawled_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 도메인별 검색을 위한 인덱스
CREATE INDEX IF NOT EXISTS idx_crawled_pages_domain ON crawled_pages(domain);

-- 크롤링 시간별 검색을 위한 인덱스
CREATE INDEX IF NOT EXISTS idx_crawled_pages_crawled_at ON crawled_pages(crawled_at);

-- 도메인과 크롤링 시간 조합 인덱스
CREATE INDEX IF NOT EXISTS idx_crawled_pages_domain_crawled_at ON crawled_pages(domain, crawled_at);

-- JSONB 메타데이터 검색을 위한 GIN 인덱스
CREATE INDEX IF NOT EXISTS idx_crawled_pages_metadata ON crawled_pages USING GIN(metadata);
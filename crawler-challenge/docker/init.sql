-- docker/init.sql

-- 기존 테이블이 있다면 삭제 (테이블 구조가 완전히 바뀌므로)
DROP TABLE IF EXISTS pages;
DROP TABLE IF EXISTS crawled_pages;

CREATE TABLE IF NOT EXISTS crawled_pages (
    id SERIAL PRIMARY KEY,
    url TEXT UNIQUE NOT NULL,
    domain TEXT,
    title TEXT,
    content_text TEXT,
    metadata JSONB,  -- 파이썬에서 json.dumps()로 넣으므로 JSONB 타입 사용
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 인덱스 추가 (조회 및 통계 속도 향상)
CREATE INDEX IF NOT EXISTS idx_url ON crawled_pages(url);
CREATE INDEX IF NOT EXISTS idx_domain ON crawled_pages(domain);
CREATE INDEX IF NOT EXISTS idx_created_at ON crawled_pages(created_at);
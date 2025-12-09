-- docker/init.sql

CREATE TABLE IF NOT EXISTS pages (
    id SERIAL PRIMARY KEY,
    url TEXT UNIQUE NOT NULL,
    status VARCHAR(50) DEFAULT 'ready', -- ready, processing, done, failed
    title TEXT,
    content TEXT,
    crawled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 인덱스 추가 (조회 속도 향상)
CREATE INDEX IF NOT EXISTS idx_status ON pages(status);
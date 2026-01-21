"""
Kafka Configuration for Stream Pipeline
========================================

환경변수로 설정 가능한 Kafka 관련 설정들
"""

import os
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class KafkaConfig:
    """Kafka 연결 및 토픽 설정"""

    # Connection
    bootstrap_servers: str = field(
        default_factory=lambda: os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    )

    # Security (optional)
    security_protocol: str = field(
        default_factory=lambda: os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
    )
    sasl_mechanism: Optional[str] = field(
        default_factory=lambda: os.getenv("KAFKA_SASL_MECHANISM")
    )
    sasl_username: Optional[str] = field(
        default_factory=lambda: os.getenv("KAFKA_SASL_USERNAME")
    )
    sasl_password: Optional[str] = field(
        default_factory=lambda: os.getenv("KAFKA_SASL_PASSWORD")
    )


@dataclass
class TopicConfig:
    """Kafka 토픽 이름 설정"""

    # Layer 1: Raw Pages
    raw_page: str = "raw.page"
    raw_page_dlq: str = "raw.page.dlq"

    # Layer 2: Routed Pages
    process_fast: str = "process.fast"
    process_rich: str = "process.rich"

    # Layer 3: Processed Results
    processed_final: str = "processed.final"
    processed_dlq: str = "processed.dlq"

    # Layer 4: Storage Events
    storage_saved: str = "storage.saved"
    storage_dlq: str = "storage.dlq"

    # Monitoring
    metrics_throughput: str = "metrics.throughput"
    metrics_errors: str = "metrics.errors"


@dataclass
class ProducerConfig:
    """Kafka Producer 설정"""

    # Batching
    linger_ms: int = field(
        default_factory=lambda: int(os.getenv("KAFKA_PRODUCER_LINGER_MS", "50"))
    )
    batch_size: int = field(
        default_factory=lambda: int(os.getenv("KAFKA_PRODUCER_BATCH_SIZE", "65536"))
    )

    # Compression
    compression_type: str = field(
        default_factory=lambda: os.getenv("KAFKA_PRODUCER_COMPRESSION", "lz4")
    )

    # Reliability
    acks: str = field(
        default_factory=lambda: os.getenv("KAFKA_PRODUCER_ACKS", "1")
    )
    retries: int = field(
        default_factory=lambda: int(os.getenv("KAFKA_PRODUCER_RETRIES", "3"))
    )

    # Timeout
    request_timeout_ms: int = field(
        default_factory=lambda: int(os.getenv("KAFKA_PRODUCER_TIMEOUT_MS", "30000"))
    )


@dataclass
class ConsumerConfig:
    """Kafka Consumer 설정"""

    # Group
    group_id_prefix: str = field(
        default_factory=lambda: os.getenv("KAFKA_CONSUMER_GROUP_PREFIX", "crawler-stream")
    )

    # Fetch settings
    fetch_min_bytes: int = field(
        default_factory=lambda: int(os.getenv("KAFKA_CONSUMER_FETCH_MIN", "1"))
    )
    fetch_max_bytes: int = field(
        default_factory=lambda: int(os.getenv("KAFKA_CONSUMER_FETCH_MAX", "52428800"))
    )
    max_poll_records: int = field(
        default_factory=lambda: int(os.getenv("KAFKA_CONSUMER_MAX_POLL", "500"))
    )

    # Offset
    auto_offset_reset: str = field(
        default_factory=lambda: os.getenv("KAFKA_CONSUMER_OFFSET_RESET", "earliest")
    )
    enable_auto_commit: bool = field(
        default_factory=lambda: os.getenv("KAFKA_CONSUMER_AUTO_COMMIT", "false").lower() == "true"
    )

    # Session
    session_timeout_ms: int = field(
        default_factory=lambda: int(os.getenv("KAFKA_CONSUMER_SESSION_TIMEOUT", "30000"))
    )
    heartbeat_interval_ms: int = field(
        default_factory=lambda: int(os.getenv("KAFKA_CONSUMER_HEARTBEAT", "10000"))
    )


@dataclass
class MinIOConfig:
    """MinIO/S3 설정"""

    endpoint: str = field(
        default_factory=lambda: os.getenv("MINIO_ENDPOINT", "localhost:9000")
    )
    access_key: str = field(
        default_factory=lambda: os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    )
    secret_key: str = field(
        default_factory=lambda: os.getenv("MINIO_SECRET_KEY", "minioadmin123")
    )
    secure: bool = field(
        default_factory=lambda: os.getenv("MINIO_SECURE", "false").lower() == "true"
    )

    # Buckets
    bucket_raw_html: str = "raw-html"
    bucket_markdown: str = "processed-markdown"
    bucket_metadata: str = "crawl-metadata"


@dataclass
class PostgresConfig:
    """PostgreSQL 설정"""

    host: str = field(
        default_factory=lambda: os.getenv("POSTGRES_HOST", "localhost")
    )
    port: int = field(
        default_factory=lambda: int(os.getenv("POSTGRES_PORT", "5432"))
    )
    database: str = field(
        default_factory=lambda: os.getenv("POSTGRES_DB", "crawler_stream")
    )
    user: str = field(
        default_factory=lambda: os.getenv("POSTGRES_USER", "crawler")
    )
    password: str = field(
        default_factory=lambda: os.getenv("POSTGRES_PASSWORD", "crawler123")
    )

    # Connection Pool
    min_connections: int = field(
        default_factory=lambda: int(os.getenv("POSTGRES_MIN_CONN", "5"))
    )
    max_connections: int = field(
        default_factory=lambda: int(os.getenv("POSTGRES_MAX_CONN", "20"))
    )

    @property
    def dsn(self) -> str:
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"


@dataclass
class IngestorConfig:
    """Ingestor (Mac) 설정"""

    # HTTP Client
    max_concurrent_requests: int = field(
        default_factory=lambda: int(os.getenv("INGESTOR_MAX_CONCURRENT", "500"))
    )
    request_timeout: float = field(
        default_factory=lambda: float(os.getenv("INGESTOR_TIMEOUT", "15.0"))
    )
    max_keepalive_connections: int = field(
        default_factory=lambda: int(os.getenv("INGESTOR_KEEPALIVE", "100"))
    )

    # Compression
    compression_level: int = field(
        default_factory=lambda: int(os.getenv("INGESTOR_COMPRESSION_LEVEL", "3"))
    )

    # Batching
    batch_size: int = field(
        default_factory=lambda: int(os.getenv("INGESTOR_BATCH_SIZE", "1000"))
    )

    # HTTP/2
    use_http2: bool = field(
        default_factory=lambda: os.getenv("INGESTOR_HTTP2", "true").lower() == "true"
    )


@dataclass
class RouterConfig:
    """Smart Router 설정"""

    # Scoring
    static_threshold: int = field(
        default_factory=lambda: int(os.getenv("ROUTER_STATIC_THRESHOLD", "80"))
    )

    # Content Length
    min_content_length: int = field(
        default_factory=lambda: int(os.getenv("ROUTER_MIN_CONTENT", "500"))
    )
    rich_content_threshold: int = field(
        default_factory=lambda: int(os.getenv("ROUTER_RICH_THRESHOLD", "3000"))
    )


@dataclass
class ProcessorConfig:
    """Processor Worker 설정"""

    # Fast Worker (BeautifulSoup)
    fast_worker_count: int = field(
        default_factory=lambda: int(os.getenv("PROCESSOR_FAST_WORKERS", "6"))
    )

    # Rich Worker (Crawl4AI)
    rich_worker_count: int = field(
        default_factory=lambda: int(os.getenv("PROCESSOR_RICH_WORKERS", "2"))
    )
    browser_pool_size: int = field(
        default_factory=lambda: int(os.getenv("PROCESSOR_BROWSER_POOL", "5"))
    )
    browser_headless: bool = field(
        default_factory=lambda: os.getenv("PROCESSOR_BROWSER_HEADLESS", "true").lower() == "true"
    )


@dataclass
class StreamPipelineConfig:
    """전체 파이프라인 통합 설정"""

    kafka: KafkaConfig = field(default_factory=KafkaConfig)
    topics: TopicConfig = field(default_factory=TopicConfig)
    producer: ProducerConfig = field(default_factory=ProducerConfig)
    consumer: ConsumerConfig = field(default_factory=ConsumerConfig)
    minio: MinIOConfig = field(default_factory=MinIOConfig)
    postgres: PostgresConfig = field(default_factory=PostgresConfig)
    ingestor: IngestorConfig = field(default_factory=IngestorConfig)
    router: RouterConfig = field(default_factory=RouterConfig)
    processor: ProcessorConfig = field(default_factory=ProcessorConfig)


# Singleton instance
config = StreamPipelineConfig()


def get_config() -> StreamPipelineConfig:
    """설정 인스턴스 반환"""
    return config

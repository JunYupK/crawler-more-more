"""Kafka URL 처리 및 임베딩 관련 회귀 테스트."""

from unittest.mock import patch


def test_topic_config_has_expected_pipeline_topics():
    """스트림 파이프라인 토픽 이름이 의도대로 노출되는지 확인."""
    from src.common.kafka_config import TopicConfig

    topics = TopicConfig()

    assert topics.discovered_urls == "discovered.urls"
    assert topics.discovered_urls_dlq == "discovered.urls.dlq"
    assert topics.processed_final == "processed.final"


def test_url_normalize_removes_tracking_params_and_fragment():
    """URL 정규화 시 트래킹 파라미터/fragment 제거 및 도메인 소문자화 검증."""
    from src.common.url_extractor import URLExtractor

    normalized = URLExtractor.normalize(
        "https://Example.COM/path?a=1&utm_source=x&fbclid=abc#section"
    )

    assert normalized == "https://example.com/path?a=1"


def test_url_filter_and_normalize_deduplicates_after_normalization():
    """정규화 이후 동일 URL이 중복 제거되는지 확인."""
    from src.common.url_extractor import URLExtractor

    urls = [
        "https://example.com/page?utm_source=google",
        "https://example.com/page",
        "javascript:void(0)",
    ]

    assert URLExtractor.filter_and_normalize(urls) == ["https://example.com/page"]


def test_create_embedder_falls_back_to_local_for_unknown_backend(monkeypatch):
    """잘못된 EMBED_BACKEND 설정 시 local 백엔드로 폴백되는지 확인."""
    from src.embedding import embedder as embedder_module

    class DummyEmbedder:
        pass

    monkeypatch.setenv("EMBED_BACKEND", "unexpected-backend")

    with patch.object(embedder_module, "SentenceTransformerEmbedder", return_value=DummyEmbedder()):
        created = embedder_module.create_embedder()

    assert isinstance(created, DummyEmbedder)


def test_embedding_stats_processed_count_excludes_skipped_and_failed():
    """임베딩 처리 수 계산이 consumed/skipped/failed 관계를 유지하는지 확인."""
    from src.embedding.embedding_worker import EmbeddingStats

    stats = EmbeddingStats(messages_consumed=20, messages_skipped=3, messages_failed=2)

    assert stats.messages_processed == 15

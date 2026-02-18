"""
URL Queue Consumer - discovered.urls Kafka 토픽 → Redis Queue 적재기
====================================================================

FastWorker/RichWorker가 발행한 discovered.urls 메시지를 소비하여
ShardedRedisQueueManager의 priority_low 큐에 적재합니다.

안전장치:
1. 총 큐 사이즈 제한 (TOTAL_QUEUE_LIMIT): Redis 전체 pending URL 수가
   임계치를 초과하면 신규 URL 추가를 차단합니다.
2. 도메인별 URL 수 제한 (MAX_DOMAIN_URLS): 같은 도메인에서 발견된
   URL이 MAX_DOMAIN_URLS 개를 초과하면 추가를 차단합니다.
3. 중복 제거: 이미 completed/failed 된 URL은 추가하지 않습니다.
"""

import os
import json
import time
import asyncio
import logging
from typing import Optional
from urllib.parse import urlparse

import msgpack
from aiokafka import AIOKafkaConsumer

from src.common.kafka_config import get_config
from src.common.url_extractor import URLExtractor
from src.managers.sharded_queue_manager import ShardedRedisQueueManager

logger = logging.getLogger(__name__)


class URLQueueConsumer:
    """
    Kafka discovered.urls 소비 → Redis 큐 적재기

    환경변수 설정:
      URL_QUEUE_TOTAL_LIMIT   총 pending URL 상한 (기본: 5,000,000)
      URL_QUEUE_DOMAIN_LIMIT  도메인별 최대 추가 수 (기본: 100)
    """

    # 안전장치 임계값
    TOTAL_QUEUE_LIMIT = int(os.getenv("URL_QUEUE_TOTAL_LIMIT", "5000000"))
    MAX_DOMAIN_URLS = int(os.getenv("URL_QUEUE_DOMAIN_LIMIT", "100"))

    # 발견된 URL의 우선순위 (Tranco 원본 700~1000보다 낮게)
    DISCOVERED_PRIORITY = 500
    CUSTOM_DISCOVERED_PRIORITY = 850

    # 도메인별 카운터를 저장하는 Redis 키 (샤드 0, DB 1에 저장)
    DOMAIN_COUNT_KEY = "crawler:discovered:domain_counts"
    SCOPE_PAGE_COUNT_KEY = "crawler:scope:pages_crawled"

    def __init__(
        self,
        bootstrap_servers: Optional[str] = None,
        shard_configs: Optional[list] = None,
    ):
        """
        Args:
            bootstrap_servers: Kafka 브로커 주소
            shard_configs: Redis 샤드 설정 (None이면 기본값 사용)
        """
        config = get_config()
        self.bootstrap_servers = bootstrap_servers or config.kafka.bootstrap_servers
        self.topics = config.topics

        self._consumer: Optional[AIOKafkaConsumer] = None
        effective_shard_configs = shard_configs or self._build_default_shard_configs()
        self._queue_manager = ShardedRedisQueueManager(effective_shard_configs)
        self._running = False

        # 통계
        self._stats = {
            'messages_consumed': 0,
            'urls_received': 0,
            'urls_added': 0,
            'urls_skipped_dedup': 0,
            'urls_skipped_dedup_completed': 0,
            'urls_skipped_dedup_pending': 0,
            'urls_skipped_domain_limit': 0,
            'urls_skipped_queue_limit': 0,
            'urls_skipped_invalid': 0,
            'urls_skipped_scope': 0,
        }

        logger.info(
            f"URLQueueConsumer initialized: "
            f"total_limit={self.TOTAL_QUEUE_LIMIT:,}, "
            f"domain_limit={self.MAX_DOMAIN_URLS}, "
            f"redis_target={effective_shard_configs[0]['host']}:{effective_shard_configs[0]['port']}"
        )

    def _build_default_shard_configs(self) -> list[dict]:
        """
        URL 재적재용 기본 Redis 샤드 설정 생성.

        우선순위:
        1) URL_QUEUE_REDIS_HOST/PORT/DB_START
        2) REDIS_HOST
        3) localhost
        """
        redis_host = os.getenv("URL_QUEUE_REDIS_HOST", os.getenv("REDIS_HOST", "localhost"))
        redis_port = int(os.getenv("URL_QUEUE_REDIS_PORT", "6379"))
        db_start = int(os.getenv("URL_QUEUE_REDIS_DB_START", "1"))
        return [
            {'host': redis_host, 'port': redis_port, 'db': db_start},
            {'host': redis_host, 'port': redis_port, 'db': db_start + 1},
            {'host': redis_host, 'port': redis_port, 'db': db_start + 2},
        ]

    async def start(self) -> None:
        """Consumer 시작"""
        config = get_config()

        self._consumer = AIOKafkaConsumer(
            self.topics.discovered_urls,
            bootstrap_servers=self.bootstrap_servers,
            group_id=f"{config.consumer.group_id_prefix}-url-queue",
            value_deserializer=lambda m: msgpack.unpackb(m, raw=False),
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            max_poll_records=100,
            session_timeout_ms=30000,
            heartbeat_interval_ms=10000,
        )

        await self._consumer.start()
        self._running = True
        logger.info("URLQueueConsumer started")

    async def stop(self) -> None:
        """Consumer 중지"""
        self._running = False
        if self._consumer:
            await self._consumer.stop()
            self._consumer = None
        logger.info(f"URLQueueConsumer stopped. Stats: {self._stats}")

    async def __aenter__(self) -> "URLQueueConsumer":
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.stop()

    async def run(self) -> None:
        """
        메인 소비 루프

        discovered.urls 메시지를 지속적으로 소비하여 Redis 큐에 적재합니다.
        """
        if not self._running:
            raise RuntimeError("Consumer not started. Call start() first.")

        uncommitted = 0
        last_commit_time = time.time()
        last_log_time = time.time()
        COMMIT_BATCH = 50
        COMMIT_INTERVAL = 5.0
        LOG_INTERVAL = 60.0

        logger.info("URLQueueConsumer run loop started")

        try:
            async for message in self._consumer:
                if not self._running:
                    break

                try:
                    await self._process_message(message.value)
                    uncommitted += 1

                    now = time.time()

                    # 오프셋 커밋
                    if uncommitted >= COMMIT_BATCH or (now - last_commit_time) >= COMMIT_INTERVAL:
                        await self._consumer.commit()
                        uncommitted = 0
                        last_commit_time = now

                    # 주기적 통계 로깅
                    if (now - last_log_time) >= LOG_INTERVAL:
                        logger.info(f"URLQueueConsumer stats: {self._stats}")
                        last_log_time = now

                except Exception as e:
                    logger.error(f"Error processing message: {e}", exc_info=True)

        except asyncio.CancelledError:
            logger.info("URLQueueConsumer loop cancelled")
        except Exception as e:
            logger.error(f"Error in consumer loop: {e}", exc_info=True)
        finally:
            if uncommitted > 0 and self._consumer:
                try:
                    await self._consumer.commit()
                except Exception:
                    pass
            logger.info("URLQueueConsumer run loop ended")

    async def _process_message(self, message: dict) -> None:
        """
        discovered.urls 메시지 단건 처리

        메시지 형식:
          {
            'source_url': str,   # URL을 발견한 원본 페이지
            'urls': list[str],   # 발견된 URL 목록 (절대 URL)
            'timestamp': float
          }
        """
        source_url = message.get('source_url', '')
        raw_urls: list = message.get('urls', [])
        scope = message.get('scope')
        current_depth = int(message.get('current_depth', 0))

        self._stats['messages_consumed'] += 1
        self._stats['urls_received'] += len(raw_urls)

        if not raw_urls:
            return

        # 1. URL 정규화 및 필터링 (트래킹 파라미터 제거, http/https만)
        normalized_urls = URLExtractor.filter_and_normalize(raw_urls)
        skipped_invalid = len(raw_urls) - len(normalized_urls)
        self._stats['urls_skipped_invalid'] += skipped_invalid

        if not normalized_urls:
            return

        # 2. 총 큐 사이즈 상한 확인 (배치 단위로 한 번만 조회)
        stats = self._queue_manager.get_queue_stats()
        total_pending = stats.get('total_pending', 0)
        if total_pending >= self.TOTAL_QUEUE_LIMIT:
            self._stats['urls_skipped_queue_limit'] += len(normalized_urls)
            logger.warning(
                f"Queue limit reached ({total_pending:,}/{self.TOTAL_QUEUE_LIMIT:,}). "
                f"Skipping {len(normalized_urls)} URLs from {source_url}"
            )
            return

        # 3. 각 URL을 동기 Redis 작업으로 처리 (executor 사용)
        loop = asyncio.get_running_loop()
        for url in normalized_urls:
            if scope:
                result = await loop.run_in_executor(
                    None,
                    self._try_add_scoped_url,
                    url,
                    source_url,
                    scope,
                    current_depth,
                )
            else:
                result = await loop.run_in_executor(
                    None,
                    self._try_add_url, url, source_url,
                )
            # stats 업데이트는 async 컨텍스트에서만 수행 (race condition 방지)
            if result == 'added':
                self._stats['urls_added'] += 1
            elif result == 'dedup_completed':
                self._stats['urls_skipped_dedup'] += 1
                self._stats['urls_skipped_dedup_completed'] += 1
            elif result == 'dedup_pending':
                self._stats['urls_skipped_dedup'] += 1
                self._stats['urls_skipped_dedup_pending'] += 1
            elif result == 'domain_limit':
                self._stats['urls_skipped_domain_limit'] += 1
            elif result in ('scope_filtered', 'scope_depth_limit', 'scope_page_limit'):
                self._stats['urls_skipped_scope'] += 1

    def _check_duplicate(self, url_hash: str) -> str | None:
        """중복 상태 확인"""
        # 1. completed 중복 체크
        for shard_id in range(self._queue_manager.num_shards):
            client = self._queue_manager.redis_clients[shard_id]
            completed_key = self._queue_manager.completed_template.format(shard=shard_id)
            if client.sismember(completed_key, url_hash):
                return 'dedup_completed'

        # 2. pending 상태 중복 체크
        if self._queue_manager.is_url_hash_pending(url_hash):
            return 'dedup_pending'

        return None

    def _try_add_url(self, url: str, source_url: str) -> str:
        """
        URL 단건 Redis 큐 추가 시도 (동기, executor 스레드에서 실행)

        처리 순서:
          1. completed set 중복 체크 (모든 샤드)
          2. pending 상태 중복 체크 (모든 샤드 큐/processing/retry)
          3. 도메인별 URL 수 상한 체크
          4. priority_low 큐에 추가
          5. 도메인 카운터 증가

        Returns:
            'added'        : 큐에 추가 성공
            'dedup_completed': 이미 완료된 URL (중복)
            'dedup_pending'  : 이미 pending 상태의 URL (중복)
            'domain_limit' : 도메인 상한 초과
        """
        url_hash = self._queue_manager._get_url_hash(url)
        domain = urlparse(url).netloc

        duplicate = self._check_duplicate(url_hash)
        if duplicate:
            return duplicate

        # 3. 도메인별 상한 체크 (샤드 0의 Redis에 카운터 저장)
        domain_client = self._queue_manager.redis_clients[0]
        domain_count = int(domain_client.hget(self.DOMAIN_COUNT_KEY, domain) or 0)
        if domain_count >= self.MAX_DOMAIN_URLS:
            return 'domain_limit'

        # 4. priority_low 큐에 추가
        shard_id = self._queue_manager.get_shard_for_url(url)
        client = self._queue_manager.redis_clients[shard_id]
        queue_key = self._queue_manager.queue_templates['priority_low'].format(shard=shard_id)

        url_data = json.dumps({
            'url': url,
            'domain': domain,
            'priority': self.DISCOVERED_PRIORITY,
            'url_type': 'discovered',
            'source_url': source_url,
        })
        client.zadd(queue_key, {url_data: self.DISCOVERED_PRIORITY})

        # 5. 도메인 카운터 증가
        domain_client.hincrby(self.DOMAIN_COUNT_KEY, domain, 1)

        logger.debug(f"Added discovered URL: {url} (domain_count={domain_count + 1})")
        return 'added'

    def _try_add_scoped_url(
        self,
        url: str,
        source_url: str,
        scope: dict,
        current_depth: int,
    ) -> str:
        """
        scope가 포함된 URL 단건 추가 시도

        처리 순서:
          1. 중복 체크
          2. scope.allowed_domain / path_prefix 매칭
          3. max_depth 확인
          4. max_pages 확인 (Redis atomic counter)
          5. priority_custom 큐에 추가 (도메인 제한 미적용)
        """
        url_hash = self._queue_manager._get_url_hash(url)
        parsed = urlparse(url)
        domain = parsed.netloc.lower()
        path = parsed.path or "/"

        duplicate = self._check_duplicate(url_hash)
        if duplicate:
            return duplicate

        allowed_domain = str(scope.get('allowed_domain', '')).lower()
        path_prefix = str(scope.get('path_prefix', '/') or '/')
        if not path_prefix.startswith('/'):
            path_prefix = f"/{path_prefix}"
        if not allowed_domain:
            return 'scope_filtered'

        if domain != allowed_domain:
            return 'scope_filtered'
        if not path.startswith(path_prefix):
            return 'scope_filtered'

        max_depth = int(scope.get('max_depth', 3))
        if current_depth > max_depth:
            return 'scope_depth_limit'

        max_pages = int(scope.get('max_pages', 10000))
        seed_url = str(scope.get('seed_url', ''))
        if not seed_url:
            seed_url = f"https://{allowed_domain}{path_prefix}"

        counter_client = self._queue_manager.redis_clients[0]
        pages_crawled = int(counter_client.hget(self.SCOPE_PAGE_COUNT_KEY, seed_url) or 0)
        if pages_crawled >= max_pages:
            return 'scope_page_limit'

        shard_id = self._queue_manager.get_shard_for_url(url)
        client = self._queue_manager.redis_clients[shard_id]
        queue_key = self._queue_manager.queue_templates['priority_custom'].format(shard=shard_id)

        next_scope = dict(scope)
        next_scope['current_depth'] = current_depth
        next_scope['pages_crawled'] = pages_crawled + 1

        url_data = json.dumps({
            'url': url,
            'domain': domain,
            'priority': self.CUSTOM_DISCOVERED_PRIORITY,
            'url_type': 'custom',
            'source_url': source_url,
            'scope': next_scope,
        })
        client.zadd(queue_key, {url_data: self.CUSTOM_DISCOVERED_PRIORITY})
        counter_client.hincrby(self.SCOPE_PAGE_COUNT_KEY, seed_url, 1)

        logger.debug(
            "Added scoped URL: %s (seed=%s, depth=%s/%s, pages=%s/%s)",
            url, seed_url, current_depth, max_depth, pages_crawled + 1, max_pages
        )
        return 'added'

    def get_stats(self) -> dict:
        """현재 통계 반환"""
        return dict(self._stats)

    def reset_domain_counts(self) -> None:
        """
        도메인별 카운터 초기화 (운영 중 도메인 제한 리셋용)

        주의: 이 작업 후에는 이미 제한에 걸렸던 도메인도 다시 추가 가능해집니다.
        """
        try:
            domain_client = self._queue_manager.redis_clients[0]
            domain_client.delete(self.DOMAIN_COUNT_KEY)
            logger.info("Domain counts reset")
        except Exception as e:
            logger.error(f"Failed to reset domain counts: {e}")

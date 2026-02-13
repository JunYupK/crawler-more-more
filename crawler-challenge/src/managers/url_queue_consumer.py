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

    # 도메인별 카운터를 저장하는 Redis 키 (샤드 0, DB 1에 저장)
    DOMAIN_COUNT_KEY = "crawler:discovered:domain_counts"

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
        self._queue_manager = ShardedRedisQueueManager(shard_configs)
        self._running = False

        # 통계
        self._stats = {
            'messages_consumed': 0,
            'urls_received': 0,
            'urls_added': 0,
            'urls_skipped_dedup': 0,
            'urls_skipped_domain_limit': 0,
            'urls_skipped_queue_limit': 0,
            'urls_skipped_invalid': 0,
        }

        logger.info(
            f"URLQueueConsumer initialized: "
            f"total_limit={self.TOTAL_QUEUE_LIMIT:,}, "
            f"domain_limit={self.MAX_DOMAIN_URLS}"
        )

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
            result = await loop.run_in_executor(
                None,
                self._try_add_url, url, source_url,
            )
            # stats 업데이트는 async 컨텍스트에서만 수행 (race condition 방지)
            if result == 'added':
                self._stats['urls_added'] += 1
            elif result == 'dedup':
                self._stats['urls_skipped_dedup'] += 1
            elif result == 'domain_limit':
                self._stats['urls_skipped_domain_limit'] += 1

    def _try_add_url(self, url: str, source_url: str) -> str:
        """
        URL 단건 Redis 큐 추가 시도 (동기, executor 스레드에서 실행)

        처리 순서:
          1. completed set 중복 체크 (모든 샤드)
          2. 도메인별 URL 수 상한 체크
          3. priority_low 큐에 추가
          4. 도메인 카운터 증가

        Returns:
            'added'        : 큐에 추가 성공
            'dedup'        : 이미 완료된 URL (중복)
            'domain_limit' : 도메인 상한 초과
        """
        url_hash = self._queue_manager._get_url_hash(url)
        domain = urlparse(url).netloc

        # 1. 중복 체크: 모든 샤드의 completed set 확인
        for shard_id in range(self._queue_manager.num_shards):
            client = self._queue_manager.redis_clients[shard_id]
            completed_key = self._queue_manager.completed_template.format(shard=shard_id)
            if client.sismember(completed_key, url_hash):
                return 'dedup'

        # 2. 도메인별 상한 체크 (샤드 0의 Redis에 카운터 저장)
        domain_client = self._queue_manager.redis_clients[0]
        domain_count = int(domain_client.hget(self.DOMAIN_COUNT_KEY, domain) or 0)
        if domain_count >= self.MAX_DOMAIN_URLS:
            return 'domain_limit'

        # 3. priority_low 큐에 추가
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

        # 4. 도메인 카운터 증가
        domain_client.hincrby(self.DOMAIN_COUNT_KEY, domain, 1)

        logger.debug(f"Added discovered URL: {url} (domain_count={domain_count + 1})")
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

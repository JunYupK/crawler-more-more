"""
Seed Manager - Custom URL 시드 관리
====================================

사용자가 지정한 seed URL을 priority_custom 큐에 삽입하고
scope 메타데이터를 함께 저장한다.
"""

import json
import logging
from pathlib import Path
from urllib.parse import urlparse, urlunparse

from src.common.url_extractor import URLExtractor
from src.managers.sharded_queue_manager import ShardedRedisQueueManager

logger = logging.getLogger(__name__)


class SeedManager:
    """Custom seed URL 삽입 매니저"""

    CUSTOM_PRIORITY = 850
    SCOPE_PAGE_COUNTER_KEY = "crawler:scope:pages_crawled"

    def __init__(self, queue_manager: ShardedRedisQueueManager):
        self.queue_manager = queue_manager

    def add_seed(self, url: str, max_depth: int = 3, max_pages: int = 10000) -> bool:
        """
        커스텀 seed URL 1개를 priority_custom 큐에 추가한다.

        Returns:
            True: 추가 성공
            False: 유효하지 않거나 중복
        """
        normalized = self._normalize_seed_url(url)
        if not normalized:
            logger.warning(f"Invalid custom seed URL skipped: {url}")
            return False

        if self._is_duplicate(normalized):
            logger.info(f"Duplicate custom seed skipped: {normalized}")
            return False

        parsed = urlparse(normalized)
        domain = parsed.netloc.lower()
        path_prefix = parsed.path or "/"

        scope = {
            "seed_url": normalized,
            "allowed_domain": domain,
            "path_prefix": path_prefix,
            "max_depth": max_depth,
            "max_pages": max_pages,
            "current_depth": 0,
            "pages_crawled": 0,
        }

        seed_data = {
            "url": normalized,
            "domain": domain,
            "priority": self.CUSTOM_PRIORITY,
            "url_type": "custom",
            "scope": scope,
        }

        shard_id = self.queue_manager.get_shard_for_url(normalized)
        client = self.queue_manager.redis_clients[shard_id]
        queue_key = self.queue_manager.queue_templates["priority_custom"].format(shard=shard_id)
        client.zadd(queue_key, {json.dumps(seed_data): self.CUSTOM_PRIORITY})

        # scope 페이지 카운터는 별도 Redis hash(샤드 0)에 저장
        counter_client = self.queue_manager.redis_clients[0]
        try:
            counter_client.hsetnx(self.SCOPE_PAGE_COUNTER_KEY, normalized, 0)
        except Exception:
            # 일부 테스트용 fake client는 hsetnx가 없을 수 있음
            if counter_client.hget(self.SCOPE_PAGE_COUNTER_KEY, normalized) is None:
                counter_client.hset(self.SCOPE_PAGE_COUNTER_KEY, normalized, 0)

        logger.info(
            "Custom seed added: %s (domain=%s, depth=%s, max_pages=%s, shard=%s)",
            normalized, domain, max_depth, max_pages, shard_id,
        )
        return True

    def add_seeds_from_file(self, file_path: str, max_depth: int = 3, max_pages: int = 10000) -> int:
        """
        파일에서 seed URL 목록을 읽어 추가한다.
        - 한 줄 하나
        - 빈 줄/`#` 주석 무시

        Returns:
            실제 추가된 seed 수
        """
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"Seed file not found: {file_path}")

        added = 0
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if self.add_seed(line, max_depth=max_depth, max_pages=max_pages):
                added += 1

        logger.info("Loaded custom seeds from file: %s (added=%s)", file_path, added)
        return added

    def _normalize_seed_url(self, url: str) -> str | None:
        """Seed URL 정규화 (스킴 보정 + tracking 제거 + path 보존)"""
        text = (url or "").strip()
        if not text:
            return None

        if not text.startswith(("http://", "https://")):
            text = f"https://{text}"

        normalized = URLExtractor.normalize(text)
        if not normalized:
            return None

        parsed = urlparse(normalized)
        path = parsed.path or "/"
        return urlunparse((parsed.scheme, parsed.netloc.lower(), path, "", "", ""))

    def _is_duplicate(self, url: str) -> bool:
        """이미 완료/대기/처리중인 URL인지 확인"""
        url_hash = self.queue_manager._get_url_hash(url)

        # completed 중복 검사
        for shard_id in range(self.queue_manager.num_shards):
            client = self.queue_manager.redis_clients[shard_id]
            completed_key = self.queue_manager.completed_template.format(shard=shard_id)
            if client.sismember(completed_key, url_hash):
                return True

        # pending(큐/processing/retry) 중복 검사
        return self.queue_manager.is_url_hash_pending(url_hash)

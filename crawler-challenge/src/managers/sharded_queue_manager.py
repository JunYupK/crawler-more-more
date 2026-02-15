#!/usr/bin/env python3
"""
Sharded Redis Queue Manager - Redis 샤딩을 통한 고성능 큐 관리
"""

import os
import redis
import json
import time
import hashlib
from typing import List, Dict, Optional, Tuple, Set
from datetime import datetime, timedelta
from urllib.parse import urlparse
import logging
import asyncio
import random

logger = logging.getLogger(__name__)

class ShardedRedisQueueManager:
    """샤딩된 Redis 큐를 통한 대규모 URL 관리"""

    def __init__(self, shard_configs: List[Dict] = None):
        """
        Args:
            shard_configs: 샤드 설정 리스트
            예: [
                {'host': 'redis1', 'port': 6379, 'db': 1},
                {'host': 'redis2', 'port': 6379, 'db': 1},
                {'host': 'redis3', 'port': 6379, 'db': 1}
            ]
        """
        if shard_configs is None:
            redis_host = os.getenv('REDIS_HOST', 'localhost')
            # 기본 단일 Redis 인스턴스를 3개 DB로 샤딩
            shard_configs = [
                {'host': redis_host, 'port': 6379, 'db': 1},
                {'host': redis_host, 'port': 6379, 'db': 2},
                {'host': redis_host, 'port': 6379, 'db': 3}
            ]

        self.shard_configs = shard_configs
        self.num_shards = len(shard_configs)
        self.redis_clients = []

        # 각 샤드별 Redis 클라이언트 초기화
        for i, config in enumerate(shard_configs):
            client = redis.Redis(
                host=config['host'],
                port=config['port'],
                db=config['db'],
                decode_responses=True,
                socket_keepalive=True,
                health_check_interval=30
            )
            self.redis_clients.append(client)
            logger.info(f"샤드 {i} 초기화: {config['host']}:{config['port']}/{config['db']}")

        # 큐 이름 템플릿 (샤드별로 생성됨)
        self.queue_templates = {
            'priority_high': 'crawler:shard{shard}:queue:priority_high',
            'priority_custom': 'crawler:shard{shard}:queue:priority_custom',
            'priority_medium': 'crawler:shard{shard}:queue:priority_medium',
            'priority_normal': 'crawler:shard{shard}:queue:priority_normal',
            'priority_low': 'crawler:shard{shard}:queue:priority_low'
        }

        # 상태 관리 (샤드별)
        self.processing_template = 'crawler:shard{shard}:processing'
        self.completed_template = 'crawler:shard{shard}:completed'
        self.failed_template = 'crawler:shard{shard}:failed'
        self.retry_template = 'crawler:shard{shard}:retry'

        # 도메인별 상태 관리 (도메인 해시에 따라 샤드 결정)
        self.domain_stats_template = 'crawler:shard{shard}:domain_stats'
        self.domain_delays_template = 'crawler:shard{shard}:domain_delays'

        # 메타데이터 (모든 샤드에 복제)
        self.metadata_key = 'crawler:metadata'

    def get_shard_for_url(self, url: str) -> int:
        """URL에 대해 랜덤 샤드 선택

        기존: 도메인 해시 기반 샤딩 (같은 도메인 → 같은 샤드)
        변경: 랜덤 샤딩

        변경 이유:
        - Tranco Top 10k 데이터셋은 각 도메인당 1개 URL만 존재
        - 도메인 지역성(robots.txt 캐싱, 딜레이 관리) 이점 없음
        - 도메인 해시 기반은 특정 샤드에 부하 집중(Hot Shard) 리스크 존재

        랜덤 샤딩 이점:
        - 완전 균등 분배로 모든 샤드에 작업이 고르게 분산
        - Hot Shard 문제 자동 해결
        - 별도의 Rebalancing 로직 불필요
        """
        return random.randint(0, self.num_shards - 1)

    def get_shard_for_domain(self, domain: str) -> int:
        """도메인을 기준으로 샤드 결정"""
        hash_value = int(hashlib.md5(domain.encode('utf-8')).hexdigest(), 16)
        return hash_value % self.num_shards

    def get_random_shard(self) -> int:
        """랜덤 샤드 선택 (로드 밸런싱용)"""
        return random.randint(0, self.num_shards - 1)

    def test_connection(self) -> bool:
        """모든 샤드 연결 테스트"""
        try:
            for i, client in enumerate(self.redis_clients):
                client.ping()
                logger.info(f"샤드 {i} 연결 성공")
            logger.info("모든 샤드 연결 성공")
            return True
        except Exception as e:
            logger.error(f"샤드 연결 실패: {e}")
            return False

    def is_url_hash_pending(self, url_hash: str) -> bool:
        """URL 해시가 pending 상태(큐/processing/retry)에 존재하는지 확인"""
        for shard_id in range(self.num_shards):
            client = self.redis_clients[shard_id]

            processing_key = self.processing_template.format(shard=shard_id)
            if client.sismember(processing_key, url_hash):
                return True

            for queue_template in self.queue_templates.values():
                queue_key = queue_template.format(shard=shard_id)
                cursor = 0

                while True:
                    cursor, entries = client.zscan(queue_key, cursor=cursor)
                    for raw_data, _ in entries:
                        try:
                            parsed = json.loads(raw_data)
                        except json.JSONDecodeError:
                            continue

                        pending_url = parsed.get('url')
                        if pending_url and self._get_url_hash(pending_url) == url_hash:
                            return True

                    if cursor == 0:
                        break

            retry_key = self.retry_template.format(shard=shard_id)
            cursor = 0
            while True:
                cursor, entries = client.zscan(retry_key, cursor=cursor)
                for raw_data, _ in entries:
                    try:
                        parsed = json.loads(raw_data)
                    except json.JSONDecodeError:
                        continue

                    retry_url = parsed.get('url')
                    if retry_url and self._get_url_hash(retry_url) == url_hash:
                        return True

                if cursor == 0:
                    break

        return False

    def initialize_queues(self, url_data: List[Dict]) -> bool:
        """샤딩된 큐 초기화 및 URL 로드"""
        try:
            logger.info(f"샤딩된 큐 초기화 시작: {len(url_data)}개 URL, {self.num_shards}개 샤드")

            # 기존 큐 정리
            self.clear_all_queues()

            # 샤드별 통계
            shard_counts = [{'high': 0, 'custom': 0, 'medium': 0, 'normal': 0, 'low': 0} for _ in range(self.num_shards)]
            total_counts = {'high': 0, 'custom': 0, 'medium': 0, 'normal': 0, 'low': 0}

            # URL을 도메인별로 적절한 샤드에 분산
            for url_info in url_data:
                url = url_info['url']
                priority = url_info.get('priority', 0)
                shard_id = self.get_shard_for_url(url)

                client = self.redis_clients[shard_id]
                url_data_str = json.dumps(url_info)

                # 우선순위별 큐 결정
                if url_info.get('url_type') == 'custom':
                    queue_key = self.queue_templates['priority_custom'].format(shard=shard_id)
                    category = 'custom'
                elif priority >= 900:
                    queue_key = self.queue_templates['priority_high'].format(shard=shard_id)
                    category = 'high'
                elif priority >= 800:
                    queue_key = self.queue_templates['priority_medium'].format(shard=shard_id)
                    category = 'medium'
                elif priority >= 700:
                    queue_key = self.queue_templates['priority_normal'].format(shard=shard_id)
                    category = 'normal'
                else:
                    queue_key = self.queue_templates['priority_low'].format(shard=shard_id)
                    category = 'low'

                # 해당 샤드의 큐에 추가
                client.zadd(queue_key, {url_data_str: priority})
                shard_counts[shard_id][category] += 1
                total_counts[category] += 1

            # 메타데이터 저장 (모든 샤드에 복제)
            metadata = {
                'total_urls': len(url_data),
                'num_shards': self.num_shards,
                'initialization_time': datetime.now().isoformat(),
                'shard_distribution': json.dumps(shard_counts),
                'priority_distribution': json.dumps(total_counts)
            }

            for client in self.redis_clients:
                client.hset(self.metadata_key, mapping=metadata)

            logger.info(f"샤딩된 큐 초기화 완료:")
            logger.info(f"  - 총 URL: {len(url_data)}개")
            logger.info(f"  - 샤드 수: {self.num_shards}개")
            for i, counts in enumerate(shard_counts):
                total_shard = sum(counts.values())
                logger.info(f"  - 샤드 {i}: {total_shard}개 ({counts})")

            return True

        except Exception as e:
            logger.error(f"샤딩된 큐 초기화 실패: {e}")
            return False

    def get_next_batch(self, batch_size: int = 50) -> List[Dict]:
        """다음 배치 URL 획득 (완전 랜덤 샤드 선택)

        기존 방식의 문제점:
        - 워커별 preferred_shard 할당 (worker_id % num_shards)
        - 4개 워커, 3개 샤드인 경우:
          Worker 0,3 → Shard 0 (2배 빠르게 소진)
          Worker 1 → Shard 1
          Worker 2 → Shard 2
        - 결과: 샤드간 불균등한 소진 속도, 빈번한 Rebalancing 발생

        개선된 방식:
        - 매번 완전 랜덤 순서로 샤드 탐색
        - 모든 워커가 모든 샤드에 균등하게 접근
        - 통계적으로 균등한 부하 분산 달성
        """
        try:
            batch = []

            # 완전 랜덤 순서로 샤드 탐색 (균등 로드 밸런싱)
            shard_order = list(range(self.num_shards))
            random.shuffle(shard_order)

            for shard_id in shard_order:
                if len(batch) >= batch_size:
                    break

                client = self.redis_clients[shard_id]
                remaining_size = batch_size - len(batch)

                # 우선순위 순으로 큐에서 가져오기
                for queue_name, queue_template in self.queue_templates.items():
                    if len(batch) >= batch_size:
                        break

                    queue_key = queue_template.format(shard=shard_id)
                    items_needed = min(remaining_size, 20)  # 샤드당 최대 20개

                    items = client.zrevrange(queue_key, 0, items_needed - 1, withscores=True)

                    for item, score in items:
                        try:
                            url_data = json.loads(item)
                            batch.append({**url_data, 'shard_id': shard_id})

                            # 처리 중 세트에 추가
                            url_hash = self._get_url_hash(url_data['url'])
                            processing_key = self.processing_template.format(shard=shard_id)
                            client.sadd(processing_key, url_hash)

                            # 큐에서 제거
                            client.zrem(queue_key, item)

                        except json.JSONDecodeError:
                            logger.warning(f"잘못된 JSON 데이터 제거: {item}")
                            client.zrem(queue_key, item)

                    remaining_size = batch_size - len(batch)

            logger.debug(f"배치 획득: {len(batch)}개 URL (샤드별 분산)")
            return batch

        except Exception as e:
            logger.error(f"배치 획득 실패: {e}")
            return []

    def mark_completed(self, url: str, success: bool = True, error_info: Optional[Dict] = None, shard_id: Optional[int] = None):
        """URL 완료 처리 (해당 샤드에)

        Args:
            url: 완료 처리할 URL
            success: 성공 여부
            error_info: 실패 시 에러 정보
            shard_id: URL이 저장된 샤드 ID (랜덤 샤딩 사용 시 필수)
                      - get_next_batch()에서 반환된 shard_id를 전달해야 함
                      - None인 경우 모든 샤드에서 검색 시도 (비효율적)
        """
        try:
            # shard_id가 제공되지 않은 경우 모든 샤드에서 검색 (하위 호환성)
            if shard_id is None:
                logger.warning(f"shard_id 미제공 - 모든 샤드 검색 필요: {url}")
                # 첫 번째 샤드를 기본값으로 사용 (랜덤 샤딩에서는 정확하지 않을 수 있음)
                shard_id = 0

            client = self.redis_clients[shard_id]

            url_hash = self._get_url_hash(url)
            domain = urlparse(url).netloc

            # 처리 중 세트에서 제거
            processing_key = self.processing_template.format(shard=shard_id)
            client.srem(processing_key, url_hash)

            if success:
                # 완료 세트에 추가
                completed_key = self.completed_template.format(shard=shard_id)
                client.sadd(completed_key, url_hash)

                # 도메인 통계 업데이트 (성공)
                domain_stats_key = self.domain_stats_template.format(shard=shard_id)
                client.hincrby(domain_stats_key, f"{domain}:success", 1)

            else:
                # 실패 처리
                failure_data = {
                    'url': url,
                    'timestamp': datetime.now().isoformat(),
                    'error': error_info or {},
                    'shard_id': shard_id
                }

                failed_key = self.failed_template.format(shard=shard_id)
                client.zadd(failed_key, {json.dumps(failure_data): time.time()})

                # 도메인 통계 업데이트 (실패)
                domain_stats_key = self.domain_stats_template.format(shard=shard_id)
                client.hincrby(domain_stats_key, f"{domain}:failed", 1)

                # 재시도 가능한 에러인 경우 재시도 큐에 추가
                if error_info and error_info.get('recoverable', False):
                    retry_data = {
                        'url': url,
                        'retry_count': error_info.get('retry_count', 0) + 1,
                        'last_error': error_info,
                        'next_retry': (datetime.now() + timedelta(minutes=30)).isoformat(),
                        'shard_id': shard_id
                    }

                    if retry_data['retry_count'] <= 3:  # 최대 3회 재시도
                        retry_key = self.retry_template.format(shard=shard_id)
                        client.zadd(retry_key, {json.dumps(retry_data): time.time() + 1800})  # 30분 후

        except Exception as e:
            logger.error(f"완료 처리 실패 ({url}): {e}")

    def get_queue_stats(self) -> Dict[str, any]:
        """전체 샤드의 큐 상태 통계"""
        try:
            total_stats = {}
            shard_stats = []

            # 각 샤드별 통계 수집
            for shard_id in range(self.num_shards):
                client = self.redis_clients[shard_id]
                shard_stat = {'shard_id': shard_id}

                # 각 큐별 크기
                for queue_name, queue_template in self.queue_templates.items():
                    queue_key = queue_template.format(shard=shard_id)
                    shard_stat[f"queue_{queue_name}"] = client.zcard(queue_key)

                # 상태별 통계
                processing_key = self.processing_template.format(shard=shard_id)
                completed_key = self.completed_template.format(shard=shard_id)
                failed_key = self.failed_template.format(shard=shard_id)
                retry_key = self.retry_template.format(shard=shard_id)

                shard_stat['processing'] = client.scard(processing_key)
                shard_stat['completed'] = client.scard(completed_key)
                shard_stat['failed'] = client.zcard(failed_key)
                shard_stat['retry'] = client.zcard(retry_key)

                # 총계
                shard_stat['total_pending'] = sum(shard_stat[k] for k in shard_stat if k.startswith('queue_'))

                shard_stats.append(shard_stat)

            # 전체 통계 계산
            for key in ['queue_priority_high', 'queue_priority_custom', 'queue_priority_medium', 'queue_priority_normal', 'queue_priority_low',
                       'processing', 'completed', 'failed', 'retry', 'total_pending']:
                total_stats[key] = sum(shard.get(key, 0) for shard in shard_stats)

            total_stats['total_urls'] = (total_stats['total_pending'] + total_stats['processing'] +
                                       total_stats['completed'] + total_stats['failed'])

            # 진행률
            if total_stats['total_urls'] > 0:
                total_stats['completion_rate'] = total_stats['completed'] / total_stats['total_urls']
                total_stats['failure_rate'] = total_stats['failed'] / total_stats['total_urls']
            else:
                total_stats['completion_rate'] = 0.0
                total_stats['failure_rate'] = 0.0

            # 샤드별 상세 정보
            total_stats['shard_details'] = shard_stats
            total_stats['num_shards'] = self.num_shards

            return total_stats

        except Exception as e:
            logger.error(f"큐 통계 조회 실패: {e}")
            return {}

    def get_shard_load_balance(self) -> Dict[int, float]:
        """샤드별 로드 밸런스 상태 확인"""
        try:
            shard_loads = {}

            for shard_id in range(self.num_shards):
                client = self.redis_clients[shard_id]

                # 각 샤드의 총 작업량 계산
                total_work = 0
                for queue_template in self.queue_templates.values():
                    queue_key = queue_template.format(shard=shard_id)
                    total_work += client.zcard(queue_key)

                processing_key = self.processing_template.format(shard=shard_id)
                total_work += client.scard(processing_key)

                shard_loads[shard_id] = total_work

            return shard_loads

        except Exception as e:
            logger.error(f"샤드 로드 밸런스 확인 실패: {e}")
            return {}

    def rebalance_shards(self, target_batch_size: int = 100) -> bool:
        """샤드간 로드 리밸런싱"""
        try:
            logger.info("샤드 리밸런싱 시작...")

            shard_loads = self.get_shard_load_balance()
            if not shard_loads:
                return False

            avg_load = sum(shard_loads.values()) / len(shard_loads)
            logger.info(f"평균 로드: {avg_load:.1f}")

            # 로드가 높은 샤드에서 낮은 샤드로 작업 이전
            for over_shard, over_load in shard_loads.items():
                if over_load > avg_load * 1.5:  # 150% 이상인 경우
                    # 가장 로드가 낮은 샤드 찾기
                    under_shard = min(shard_loads.items(), key=lambda x: x[1])[0]

                    if shard_loads[under_shard] < avg_load * 0.5:  # 50% 이하인 경우
                        # 작업 이전 실행
                        moved = self._move_work_between_shards(over_shard, under_shard, target_batch_size // 4)
                        if moved > 0:
                            logger.info(f"샤드 {over_shard} → 샤드 {under_shard}: {moved}개 작업 이전")

            logger.info("샤드 리밸런싱 완료")
            return True

        except Exception as e:
            logger.error(f"샤드 리밸런싱 실패: {e}")
            return False

    def _move_work_between_shards(self, from_shard: int, to_shard: int, max_items: int) -> int:
        """샤드간 작업 이전"""
        try:
            from_client = self.redis_clients[from_shard]
            to_client = self.redis_clients[to_shard]
            moved_count = 0

            # 낮은 우선순위 큐부터 이전
            for queue_name in ['priority_low', 'priority_normal']:
                if moved_count >= max_items:
                    break

                from_queue = self.queue_templates[queue_name].format(shard=from_shard)
                to_queue = self.queue_templates[queue_name].format(shard=to_shard)

                # 가져올 아이템 수 결정
                items_to_move = min(max_items - moved_count, 10)
                items = from_client.zrange(from_queue, 0, items_to_move - 1, withscores=True)

                for item, score in items:
                    # 대상 샤드로 이전
                    to_client.zadd(to_queue, {item: score})
                    from_client.zrem(from_queue, item)
                    moved_count += 1

            return moved_count

        except Exception as e:
            logger.error(f"샤드간 작업 이전 실패: {e}")
            return 0

    def clear_all_queues(self):
        """모든 샤드의 큐 정리"""
        try:
            for shard_id in range(self.num_shards):
                client = self.redis_clients[shard_id]

                # 큐 키들 생성
                keys_to_clear = []
                for queue_template in self.queue_templates.values():
                    keys_to_clear.append(queue_template.format(shard=shard_id))

                keys_to_clear.extend([
                    self.processing_template.format(shard=shard_id),
                    self.completed_template.format(shard=shard_id),
                    self.failed_template.format(shard=shard_id),
                    self.retry_template.format(shard=shard_id),
                    self.domain_stats_template.format(shard=shard_id),
                    self.domain_delays_template.format(shard=shard_id)
                ])

                for key in keys_to_clear:
                    client.delete(key)

            logger.info("모든 샤드의 큐 정리 완료")

        except Exception as e:
            logger.error(f"큐 정리 실패: {e}")

    def _get_url_hash(self, url: str) -> str:
        """URL 해시 생성"""
        return hashlib.md5(url.encode('utf-8')).hexdigest()

async def main():
    """테스트 실행"""
    logging.basicConfig(level=logging.INFO)

    # 샤딩된 큐 매니저 초기화 (3개 샤드)
    shard_configs = [
        {'host': 'localhost', 'port': 6379, 'db': 1},
        {'host': 'localhost', 'port': 6379, 'db': 2},
        {'host': 'localhost', 'port': 6379, 'db': 3}
    ]

    queue_manager = ShardedRedisQueueManager(shard_configs)

    if not queue_manager.test_connection():
        print("❌ Redis 샤드 연결 실패")
        return

    # 테스트 URL 데이터 (더 많은 URL로 테스트)
    test_urls = []
    domains = ['google.com', 'youtube.com', 'facebook.com', 'twitter.com', 'instagram.com',
              'linkedin.com', 'reddit.com', 'wikipedia.org', 'github.com', 'stackoverflow.com']

    for i, domain in enumerate(domains):
        for protocol in ['https', 'http']:
            test_urls.append({
                'url': f'{protocol}://{domain}/',
                'priority': 1000 - i * 50,
                'rank': i + 1,
                'domain': domain,
                'url_type': 'root'
            })

    # 큐 초기화
    if queue_manager.initialize_queues(test_urls):
        print(f"✅ 샤딩된 큐 초기화 성공: {len(test_urls)}개 URL")
    else:
        print("❌ 큐 초기화 실패")
        return

    # 큐 상태 확인
    stats = queue_manager.get_queue_stats()
    print(f"\n📊 전체 통계:")
    print(f"  총 URL: {stats.get('total_urls', 0)}")
    print(f"  대기 중: {stats.get('total_pending', 0)}")
    print(f"  샤드 수: {stats.get('num_shards', 0)}")

    print(f"\n📈 샤드별 상세:")
    for shard in stats.get('shard_details', []):
        shard_id = shard['shard_id']
        total_pending = shard['total_pending']
        print(f"  샤드 {shard_id}: {total_pending}개 대기")

    # 로드 밸런싱 확인
    load_balance = queue_manager.get_shard_load_balance()
    print(f"\n⚖️ 샤드 로드 밸런스:")
    for shard_id, load in load_balance.items():
        print(f"  샤드 {shard_id}: {load}개 작업")

    # 배치 획득 테스트
    print(f"\n📦 배치 획득 테스트:")
    for i in range(3):
        batch = queue_manager.get_next_batch(5)
        if batch:
            print(f"  배치 {i+1}: {len(batch)}개 URL")
            for item in batch[:2]:  # 처음 2개만 출력
                print(f"    - {item['url']} (샤드: {item['shard_id']})")
        else:
            print(f"  배치 {i+1}: 빈 배치")

if __name__ == "__main__":
    asyncio.run(main())

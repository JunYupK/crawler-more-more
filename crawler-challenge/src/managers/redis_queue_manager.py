import redis
import json
import time
import hashlib
from typing import List, Dict, Optional, Tuple, Set
from datetime import datetime, timedelta
from urllib.parse import urlparse
import logging
import asyncio

logger = logging.getLogger(__name__)

class RedisQueueManager:
    """Redis 기반 대규모 URL 큐 관리자"""

    def __init__(self, redis_host="localhost", redis_port=6379, redis_db=1):
        self.redis_client = redis.Redis(
            host=redis_host,
            port=redis_port,
            db=redis_db,
            decode_responses=True,
            socket_keepalive=True,
            health_check_interval=30
        )

        # 큐 이름 정의
        self.queues = {
            'priority_high': 'crawler:queue:priority_high',    # 우선순위 1000-900
            'priority_medium': 'crawler:queue:priority_medium', # 우선순위 899-800
            'priority_normal': 'crawler:queue:priority_normal', # 우선순위 799-700
            'priority_low': 'crawler:queue:priority_low'       # 우선순위 699 이하
        }

        # 상태 관리
        self.processing_set = 'crawler:processing'  # 현재 처리 중인 URL
        self.completed_set = 'crawler:completed'    # 완료된 URL
        self.failed_set = 'crawler:failed'          # 실패한 URL
        self.retry_queue = 'crawler:retry'          # 재시도 큐

        # 도메인별 상태 관리
        self.domain_stats = 'crawler:domain_stats'  # 도메인별 통계
        self.domain_delays = 'crawler:domain_delays' # 도메인별 딜레이

        # 메타데이터
        self.metadata_key = 'crawler:metadata'

    def test_connection(self) -> bool:
        """Redis 연결 테스트"""
        try:
            self.redis_client.ping()
            logger.info("Redis 큐 매니저 연결 성공")
            return True
        except Exception as e:
            logger.error(f"Redis 연결 실패: {e}")
            return False

    def initialize_queues(self, url_data: List[Dict]) -> bool:
        """큐 초기화 및 URL 로드"""
        try:
            logger.info(f"큐 초기화 시작: {len(url_data)}개 URL")

            # 기존 큐 정리
            self.clear_all_queues()

            # 우선순위별로 URL 분류 및 적재
            priority_counts = {'high': 0, 'medium': 0, 'normal': 0, 'low': 0}

            pipe = self.redis_client.pipeline()

            for url_info in url_data:
                priority = url_info.get('priority', 0)
                url_data_str = json.dumps(url_info)

                # 우선순위별 큐 결정
                if priority >= 900:
                    queue_key = self.queues['priority_high']
                    priority_counts['high'] += 1
                elif priority >= 800:
                    queue_key = self.queues['priority_medium']
                    priority_counts['medium'] += 1
                elif priority >= 700:
                    queue_key = self.queues['priority_normal']
                    priority_counts['normal'] += 1
                else:
                    queue_key = self.queues['priority_low']
                    priority_counts['low'] += 1

                # 큐에 추가 (우선순위 점수와 함께)
                pipe.zadd(queue_key, {url_data_str: priority})

            # 메타데이터 저장
            metadata = {
                'total_urls': len(url_data),
                'initialization_time': datetime.now().isoformat(),
                'priority_distribution': json.dumps(priority_counts)
            }
            pipe.hset(self.metadata_key, mapping=metadata)

            # 배치 실행
            pipe.execute()

            logger.info(f"큐 초기화 완료: {priority_counts}")
            return True

        except Exception as e:
            logger.error(f"큐 초기화 실패: {e}")
            return False

    def get_next_batch(self, batch_size: int = 50) -> List[Dict]:
        """다음 배치 URL 획득 (우선순위 순)"""
        try:
            batch = []

            # 우선순위 순으로 큐에서 가져오기
            for queue_name, queue_key in self.queues.items():
                if len(batch) >= batch_size:
                    break

                remaining_size = batch_size - len(batch)

                # 높은 우선순위부터 가져오기
                items = self.redis_client.zrevrange(queue_key, 0, remaining_size - 1, withscores=True)

                for item, score in items:
                    try:
                        url_data = json.loads(item)
                        batch.append(url_data)

                        # 처리 중 세트에 추가
                        url_hash = self._get_url_hash(url_data['url'])
                        self.redis_client.sadd(self.processing_set, url_hash)

                        # 큐에서 제거
                        self.redis_client.zrem(queue_key, item)

                    except json.JSONDecodeError:
                        logger.warning(f"잘못된 JSON 데이터 제거: {item}")
                        self.redis_client.zrem(queue_key, item)

            logger.debug(f"배치 획득: {len(batch)}개 URL")
            return batch

        except Exception as e:
            logger.error(f"배치 획득 실패: {e}")
            return []

    def mark_completed(self, url: str, success: bool = True, error_info: Optional[Dict] = None):
        """URL 완료 처리"""
        try:
            url_hash = self._get_url_hash(url)
            domain = urlparse(url).netloc

            # 처리 중 세트에서 제거
            self.redis_client.srem(self.processing_set, url_hash)

            if success:
                # 완료 세트에 추가
                self.redis_client.sadd(self.completed_set, url_hash)

                # 도메인 통계 업데이트 (성공)
                self.redis_client.hincrby(self.domain_stats, f"{domain}:success", 1)

            else:
                # 실패 처리
                failure_data = {
                    'url': url,
                    'timestamp': datetime.now().isoformat(),
                    'error': error_info or {}
                }

                self.redis_client.zadd(self.failed_set, {json.dumps(failure_data): time.time()})

                # 도메인 통계 업데이트 (실패)
                self.redis_client.hincrby(self.domain_stats, f"{domain}:failed", 1)

                # 재시도 가능한 에러인 경우 재시도 큐에 추가
                if error_info and error_info.get('recoverable', False):
                    retry_data = {
                        'url': url,
                        'retry_count': error_info.get('retry_count', 0) + 1,
                        'last_error': error_info,
                        'next_retry': (datetime.now() + timedelta(minutes=30)).isoformat()
                    }

                    if retry_data['retry_count'] <= 3:  # 최대 3회 재시도
                        self.redis_client.zadd(self.retry_queue,
                                             {json.dumps(retry_data): time.time() + 1800})  # 30분 후

        except Exception as e:
            logger.error(f"완료 처리 실패 ({url}): {e}")

    def get_retry_batch(self, batch_size: int = 20) -> List[Dict]:
        """재시도할 URL 배치 획득"""
        try:
            current_time = time.time()
            items = self.redis_client.zrangebyscore(self.retry_queue, 0, current_time,
                                                   start=0, num=batch_size, withscores=True)

            retry_batch = []
            for item, score in items:
                try:
                    retry_data = json.loads(item)
                    retry_batch.append({
                        'url': retry_data['url'],
                        'domain': urlparse(retry_data['url']).netloc,
                        'retry_count': retry_data['retry_count'],
                        'priority': 500,  # 재시도는 중간 우선순위
                        'url_type': 'retry'
                    })

                    # 재시도 큐에서 제거
                    self.redis_client.zrem(self.retry_queue, item)

                except json.JSONDecodeError:
                    logger.warning(f"잘못된 재시도 데이터 제거: {item}")
                    self.redis_client.zrem(self.retry_queue, item)

            if retry_batch:
                logger.info(f"재시도 배치 획득: {len(retry_batch)}개")

            return retry_batch

        except Exception as e:
            logger.error(f"재시도 배치 획득 실패: {e}")
            return []

    def get_queue_stats(self) -> Dict[str, any]:
        """큐 상태 통계"""
        try:
            stats = {}

            # 각 큐별 크기
            for queue_name, queue_key in self.queues.items():
                stats[f"queue_{queue_name}"] = self.redis_client.zcard(queue_key)

            # 상태별 통계
            stats['processing'] = self.redis_client.scard(self.processing_set)
            stats['completed'] = self.redis_client.scard(self.completed_set)
            stats['failed'] = self.redis_client.zcard(self.failed_set)
            stats['retry'] = self.redis_client.zcard(self.retry_queue)

            # 총계
            stats['total_pending'] = sum(stats[k] for k in stats if k.startswith('queue_'))
            stats['total_urls'] = (stats['total_pending'] + stats['processing'] +
                                 stats['completed'] + stats['failed'])

            # 진행률
            if stats['total_urls'] > 0:
                stats['completion_rate'] = stats['completed'] / stats['total_urls']
                stats['failure_rate'] = stats['failed'] / stats['total_urls']
            else:
                stats['completion_rate'] = 0.0
                stats['failure_rate'] = 0.0

            # 메타데이터
            metadata = self.redis_client.hgetall(self.metadata_key)
            if metadata:
                stats['metadata'] = metadata

            return stats

        except Exception as e:
            logger.error(f"큐 통계 조회 실패: {e}")
            return {}

    def get_domain_stats(self, limit: int = 20) -> List[Dict]:
        """도메인별 통계 (상위 N개)"""
        try:
            all_stats = self.redis_client.hgetall(self.domain_stats)

            # 도메인별 집계
            domain_summary = {}
            for key, value in all_stats.items():
                if ':' in key:
                    domain, stat_type = key.rsplit(':', 1)
                    if domain not in domain_summary:
                        domain_summary[domain] = {'success': 0, 'failed': 0}
                    domain_summary[domain][stat_type] = int(value)

            # 총 처리량 기준 정렬
            sorted_domains = sorted(
                domain_summary.items(),
                key=lambda x: x[1]['success'] + x[1]['failed'],
                reverse=True
            )

            result = []
            for domain, stats in sorted_domains[:limit]:
                total = stats['success'] + stats['failed']
                result.append({
                    'domain': domain,
                    'success': stats['success'],
                    'failed': stats['failed'],
                    'total': total,
                    'success_rate': stats['success'] / total if total > 0 else 0.0
                })

            return result

        except Exception as e:
            logger.error(f"도메인 통계 조회 실패: {e}")
            return []

    def add_urls_to_queue(self, urls: List[Dict]) -> bool:
        """기존 큐에 URL 추가"""
        try:
            logger.info(f"큐에 URL 추가: {len(urls)}개")

            pipe = self.redis_client.pipeline()

            for url_info in urls:
                # 이미 처리된 URL인지 확인
                url_hash = self._get_url_hash(url_info['url'])
                if (self.redis_client.sismember(self.completed_set, url_hash) or
                    self.redis_client.sismember(self.processing_set, url_hash)):
                    continue

                priority = url_info.get('priority', 0)
                url_data_str = json.dumps(url_info)

                # 우선순위별 큐 결정
                if priority >= 900:
                    queue_key = self.queues['priority_high']
                elif priority >= 800:
                    queue_key = self.queues['priority_medium']
                elif priority >= 700:
                    queue_key = self.queues['priority_normal']
                else:
                    queue_key = self.queues['priority_low']

                pipe.zadd(queue_key, {url_data_str: priority})

            pipe.execute()
            return True

        except Exception as e:
            logger.error(f"URL 추가 실패: {e}")
            return False

    def clear_all_queues(self):
        """모든 큐 정리"""
        try:
            all_keys = (
                list(self.queues.values()) +
                [self.processing_set, self.completed_set, self.failed_set,
                 self.retry_queue, self.domain_stats, self.domain_delays]
            )

            for key in all_keys:
                self.redis_client.delete(key)

            logger.info("모든 큐 정리 완료")

        except Exception as e:
            logger.error(f"큐 정리 실패: {e}")

    def _get_url_hash(self, url: str) -> str:
        """URL 해시 생성"""
        return hashlib.md5(url.encode('utf-8')).hexdigest()

    def set_domain_delay(self, domain: str, delay_seconds: int):
        """도메인별 딜레이 설정"""
        try:
            self.redis_client.hset(self.domain_delays, domain, delay_seconds)
        except Exception as e:
            logger.error(f"도메인 딜레이 설정 실패 ({domain}): {e}")

    def get_domain_delay(self, domain: str) -> int:
        """도메인별 딜레이 조회"""
        try:
            delay = self.redis_client.hget(self.domain_delays, domain)
            return int(delay) if delay else 1  # 기본 1초
        except Exception as e:
            logger.error(f"도메인 딜레이 조회 실패 ({domain}): {e}")
            return 1

    def cleanup_old_data(self, hours_old: int = 24):
        """오래된 데이터 정리"""
        try:
            cutoff_time = time.time() - (hours_old * 3600)

            # 오래된 실패 기록 제거
            removed_failed = self.redis_client.zremrangebyscore(self.failed_set, 0, cutoff_time)

            # 오래된 재시도 기록 제거 (시간이 지난 것들)
            removed_retry = self.redis_client.zremrangebyscore(self.retry_queue, 0, cutoff_time - 3600)

            logger.info(f"오래된 데이터 정리: 실패 {removed_failed}개, 재시도 {removed_retry}개")

        except Exception as e:
            logger.error(f"데이터 정리 실패: {e}")

async def main():
    """테스트 실행"""
    logging.basicConfig(level=logging.INFO)

    # 큐 매니저 초기화
    queue_manager = RedisQueueManager()

    if not queue_manager.test_connection():
        print("❌ Redis 연결 실패")
        return

    # 테스트 URL 데이터
    test_urls = [
        {'url': 'https://google.com/', 'priority': 1000, 'rank': 1, 'domain': 'google.com', 'url_type': 'root'},
        {'url': 'https://youtube.com/', 'priority': 950, 'rank': 2, 'domain': 'youtube.com', 'url_type': 'root'},
        {'url': 'https://facebook.com/', 'priority': 900, 'rank': 3, 'domain': 'facebook.com', 'url_type': 'root'},
        {'url': 'https://twitter.com/', 'priority': 850, 'rank': 4, 'domain': 'twitter.com', 'url_type': 'root'},
        {'url': 'https://instagram.com/', 'priority': 800, 'rank': 5, 'domain': 'instagram.com', 'url_type': 'root'},
    ]

    # 큐 초기화
    if queue_manager.initialize_queues(test_urls):
        print(f"✅ 큐 초기화 성공: {len(test_urls)}개 URL")
    else:
        print("❌ 큐 초기화 실패")
        return

    # 큐 상태 확인
    stats = queue_manager.get_queue_stats()
    print(f"\n📊 큐 통계:")
    for key, value in stats.items():
        if not key.startswith('metadata'):
            print(f"  {key}: {value}")

    # 배치 획득 테스트
    batch = queue_manager.get_next_batch(3)
    print(f"\n📦 배치 획득: {len(batch)}개")
    for i, url_info in enumerate(batch, 1):
        print(f"  {i}. {url_info['url']} (우선순위: {url_info['priority']})")

    # 완료 처리 테스트
    if batch:
        queue_manager.mark_completed(batch[0]['url'], success=True)
        print(f"✅ 완료 처리: {batch[0]['url']}")

    # 최종 통계
    final_stats = queue_manager.get_queue_stats()
    print(f"\n📊 최종 통계:")
    print(f"  대기 중: {final_stats['total_pending']}")
    print(f"  처리 중: {final_stats['processing']}")
    print(f"  완료: {final_stats['completed']}")
    print(f"  완료율: {final_stats['completion_rate']:.1%}")

if __name__ == "__main__":
    asyncio.run(main())
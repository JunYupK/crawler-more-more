#!/usr/bin/env python3
"""
Redis 큐 확장 시스템 - 대규모 URL 관리
- 10,000+ URL 처리
- 우선순위별 큐 관리
- 진행 상황 추적
- 결과 저장 및 복구
"""

import redis
import json
import time
from typing import List, Dict, Optional, Set
from dataclasses import dataclass
from datetime import datetime, timedelta
import logging
from work_logger import WorkLogger

logger = logging.getLogger(__name__)


@dataclass
class CrawlJob:
    """크롤링 작업 정보"""
    url: str
    priority: int
    rank: int
    domain: str
    url_type: str
    created_at: float
    attempts: int = 0
    last_attempt: Optional[float] = None
    result: Optional[Dict] = None
    status: str = 'pending'  # pending, processing, completed, failed


class RedisQueueExtended:
    """확장된 Redis 큐 관리자"""

    def __init__(self, redis_host='localhost', redis_port=6379, redis_db=0):
        self.redis_client = redis.Redis(
            host=redis_host,
            port=redis_port,
            db=redis_db,
            decode_responses=True
        )

        # 큐 이름 정의
        self.queues = {
            'high': 'crawler:queue:high',      # 우선순위 1000-900
            'medium': 'crawler:queue:medium',  # 우선순위 900-800
            'low': 'crawler:queue:low',        # 우선순위 800-600
            'failed': 'crawler:queue:failed'   # 실패한 작업
        }

        # 상태 추적
        self.status_keys = {
            'processing': 'crawler:processing',  # 현재 처리 중인 작업
            'completed': 'crawler:completed',    # 완료된 작업
            'stats': 'crawler:stats',           # 통계 정보
            'results': 'crawler:results'        # 크롤링 결과
        }

        self.work_logger = WorkLogger()

        # Redis 연결 테스트
        self._test_connection()

    def _test_connection(self):
        """Redis 연결 테스트"""
        try:
            self.redis_client.ping()
            print("[OK] Redis 연결 성공")
        except Exception as e:
            print(f"[ERROR] Redis 연결 실패: {e}")
            raise

    def clear_all_queues(self):
        """모든 큐 초기화"""
        for queue_name in self.queues.values():
            self.redis_client.delete(queue_name)

        for status_key in self.status_keys.values():
            self.redis_client.delete(status_key)

        print("[OK] 모든 큐 초기화 완료")

    def add_urls_batch(self, urls: List[Dict], batch_size: int = 1000):
        """URL을 배치로 큐에 추가"""
        added_count = 0
        priority_stats = {'high': 0, 'medium': 0, 'low': 0}

        print(f"[START] {len(urls)}개 URL을 Redis 큐에 추가...")

        # 배치 단위로 처리
        for i in range(0, len(urls), batch_size):
            batch = urls[i:i + batch_size]
            pipe = self.redis_client.pipeline()

            for url_info in batch:
                job = CrawlJob(
                    url=url_info['url'],
                    priority=url_info['priority'],
                    rank=url_info.get('rank', 999999),
                    domain=url_info.get('domain', ''),
                    url_type=url_info.get('url_type', 'unknown'),
                    created_at=time.time()
                )

                # 우선순위에 따른 큐 선택
                queue_type = self._get_queue_type(job.priority)
                queue_name = self.queues[queue_type]

                # JSON 직렬화
                job_data = {
                    'url': job.url,
                    'priority': job.priority,
                    'rank': job.rank,
                    'domain': job.domain,
                    'url_type': job.url_type,
                    'created_at': job.created_at,
                    'attempts': job.attempts,
                    'status': job.status
                }

                # 우선순위 큐에 추가 (높은 우선순위부터 처리되도록 정렬)
                pipe.zadd(queue_name, {json.dumps(job_data): job.priority})

                priority_stats[queue_type] += 1
                added_count += 1

            # 배치 실행
            pipe.execute()

            if (i + batch_size) % 5000 == 0 or i + batch_size >= len(urls):
                print(f"[PROGRESS] {min(i + batch_size, len(urls)):,}/{len(urls):,} URL 처리 완료")

        # 통계 업데이트
        stats = {
            'total_urls': added_count,
            'high_priority': priority_stats['high'],
            'medium_priority': priority_stats['medium'],
            'low_priority': priority_stats['low'],
            'added_at': datetime.now().isoformat()
        }

        self.redis_client.hmset(self.status_keys['stats'], stats)

        print(f"[OK] Redis 큐 추가 완료: {added_count:,}개 URL")
        print(f"  High: {priority_stats['high']:,}, Medium: {priority_stats['medium']:,}, Low: {priority_stats['low']:,}")

        return added_count

    def _get_queue_type(self, priority: int) -> str:
        """우선순위에 따른 큐 타입 결정"""
        if priority >= 900:
            return 'high'
        elif priority >= 800:
            return 'medium'
        else:
            return 'low'

    def get_next_job(self, queue_types: List[str] = None) -> Optional[CrawlJob]:
        """다음 작업 가져오기 (우선순위 순)"""
        if queue_types is None:
            queue_types = ['high', 'medium', 'low']

        for queue_type in queue_types:
            queue_name = self.queues[queue_type]

            # 가장 높은 우선순위 작업 가져오기
            result = self.redis_client.zpopmax(queue_name, 1)

            if result:
                job_data_str, priority = result[0]
                job_data = json.loads(job_data_str)

                job = CrawlJob(
                    url=job_data['url'],
                    priority=job_data['priority'],
                    rank=job_data['rank'],
                    domain=job_data['domain'],
                    url_type=job_data['url_type'],
                    created_at=job_data['created_at'],
                    attempts=job_data.get('attempts', 0),
                    status='processing'
                )

                # 처리 중인 작업으로 등록
                processing_data = {
                    'url': job.url,
                    'started_at': time.time(),
                    'queue_type': queue_type
                }

                self.redis_client.hset(
                    self.status_keys['processing'],
                    job.url,
                    json.dumps(processing_data)
                )

                return job

        return None  # 큐가 비어있음

    def complete_job(self, job: CrawlJob, result: Dict):
        """작업 완료 처리"""
        job.result = result
        job.status = 'completed'

        # 처리 중에서 제거
        self.redis_client.hdel(self.status_keys['processing'], job.url)

        # 완료된 작업으로 등록
        completed_data = {
            'url': job.url,
            'domain': job.domain,
            'completed_at': time.time(),
            'attempts': job.attempts + 1,
            'success': result.get('success', False)
        }

        self.redis_client.hset(
            self.status_keys['completed'],
            job.url,
            json.dumps(completed_data)
        )

        # 결과 저장
        if result.get('success'):
            self.redis_client.hset(
                self.status_keys['results'],
                job.url,
                json.dumps(result)
            )

    def fail_job(self, job: CrawlJob, error: str, max_retries: int = 3):
        """작업 실패 처리"""
        job.attempts += 1
        job.last_attempt = time.time()

        # 처리 중에서 제거
        self.redis_client.hdel(self.status_keys['processing'], job.url)

        # 재시도 가능한지 확인
        if job.attempts < max_retries:
            # 재시도 큐에 추가 (우선순위 낮춤)
            job.priority = max(job.priority - 50, 100)
            job.status = 'pending'

            job_data = {
                'url': job.url,
                'priority': job.priority,
                'rank': job.rank,
                'domain': job.domain,
                'url_type': job.url_type,
                'created_at': job.created_at,
                'attempts': job.attempts,
                'last_attempt': job.last_attempt,
                'status': job.status
            }

            queue_type = self._get_queue_type(job.priority)
            queue_name = self.queues[queue_type]

            self.redis_client.zadd(queue_name, {json.dumps(job_data): job.priority})

            print(f"[RETRY] {job.url} - 재시도 {job.attempts}/{max_retries}")
        else:
            # 최대 재시도 초과, 실패 큐로 이동
            job.status = 'failed'

            failed_data = {
                'url': job.url,
                'domain': job.domain,
                'failed_at': time.time(),
                'attempts': job.attempts,
                'error': error
            }

            self.redis_client.zadd(
                self.queues['failed'],
                {json.dumps(failed_data): time.time()}
            )

            print(f"[FAILED] {job.url} - 최대 재시도 초과")

    def get_queue_stats(self) -> Dict[str, int]:
        """큐 통계 정보"""
        stats = {}

        # 큐별 대기 작업 수
        for queue_type, queue_name in self.queues.items():
            stats[f'{queue_type}_pending'] = self.redis_client.zcard(queue_name)

        # 상태별 작업 수
        stats['processing'] = self.redis_client.hlen(self.status_keys['processing'])
        stats['completed'] = self.redis_client.hlen(self.status_keys['completed'])
        stats['results'] = self.redis_client.hlen(self.status_keys['results'])

        # 총합 계산
        stats['total_pending'] = sum(stats[k] for k in stats if k.endswith('_pending'))
        stats['total_jobs'] = stats['total_pending'] + stats['processing'] + stats['completed']

        return stats

    def get_progress_report(self) -> Dict:
        """진행 상황 보고서"""
        stats = self.get_queue_stats()

        # Redis에서 전체 통계 가져오기
        saved_stats = self.redis_client.hgetall(self.status_keys['stats'])

        report = {
            'timestamp': datetime.now().isoformat(),
            'queue_status': stats,
            'total_urls_added': int(saved_stats.get('total_urls', 0)),
            'completion_rate': 0,
            'processing_speed': 0
        }

        # 완료율 계산
        if stats['total_jobs'] > 0:
            report['completion_rate'] = (stats['completed'] / stats['total_jobs']) * 100

        return report

    def cleanup_stale_jobs(self, timeout_minutes: int = 30):
        """오래된 처리 중인 작업 정리"""
        processing_jobs = self.redis_client.hgetall(self.status_keys['processing'])
        current_time = time.time()
        timeout_seconds = timeout_minutes * 60

        cleaned_count = 0

        for url, job_data_str in processing_jobs.items():
            job_data = json.loads(job_data_str)
            started_at = job_data.get('started_at', current_time)

            if current_time - started_at > timeout_seconds:
                # 타임아웃된 작업을 큐로 되돌리기
                self.redis_client.hdel(self.status_keys['processing'], url)

                # 해당 큐에 다시 추가 (우선순위 낮춤)
                retry_data = {
                    'url': url,
                    'priority': 500,  # 낮은 우선순위
                    'rank': 999999,
                    'domain': job_data.get('domain', ''),
                    'url_type': 'retry',
                    'created_at': current_time,
                    'attempts': 1,
                    'status': 'pending'
                }

                self.redis_client.zadd(
                    self.queues['low'],
                    {json.dumps(retry_data): 500}
                )

                cleaned_count += 1

        if cleaned_count > 0:
            print(f"[CLEANUP] {cleaned_count}개 오래된 작업 정리 완료")

        return cleaned_count


def main():
    """테스트 실행"""
    print("[START] Redis 큐 확장 시스템 테스트")

    # 1. Redis 큐 관리자 초기화
    try:
        queue_manager = RedisQueueExtended()
    except Exception as e:
        print(f"[ERROR] Redis 연결 실패. Redis 서버가 실행 중인지 확인하세요: {e}")
        return False

    # 2. 기존 큐 초기화
    queue_manager.clear_all_queues()

    # 3. 테스트용 URL 데이터 생성
    test_urls = []
    for i in range(1, 1001):  # 1,000개 URL
        priority = 1000 - (i // 100) * 100  # 우선순위 분산
        test_urls.append({
            'url': f'https://example{i}.com/',
            'priority': priority,
            'rank': i,
            'domain': f'example{i}.com',
            'url_type': 'test'
        })

    # 4. URL 배치 추가 테스트
    added_count = queue_manager.add_urls_batch(test_urls)

    # 5. 큐 통계 확인
    stats = queue_manager.get_queue_stats()
    print(f"\n[STATS] 큐 현황:")
    for key, value in stats.items():
        print(f"  {key}: {value:,}")

    # 6. 몇 개 작업 처리 시뮬레이션
    print(f"\n[TEST] 작업 처리 시뮬레이션...")
    for i in range(5):
        job = queue_manager.get_next_job()
        if job:
            print(f"  처리 중: {job.url} (우선순위: {job.priority})")

            # 성공 시뮬레이션
            result = {
                'success': True,
                'status_code': 200,
                'content_length': 1024,
                'processed_at': time.time()
            }
            queue_manager.complete_job(job, result)

    # 7. 진행 상황 보고
    report = queue_manager.get_progress_report()
    print(f"\n[REPORT] 진행 상황:")
    for key, value in report.items():
        if isinstance(value, dict):
            print(f"  {key}:")
            for sub_key, sub_value in value.items():
                print(f"    {sub_key}: {sub_value}")
        else:
            print(f"  {key}: {value}")

    # 8. 작업 완료 로깅
    queue_manager.work_logger.log_and_commit(
        title="Redis 큐 확장 시스템 구축 완료",
        description=f"{added_count:,}개 URL을 우선순위별 Redis 큐에 추가하고 배치 처리 시스템을 구현했습니다.",
        details={
            "총 URL 수": f"{added_count:,}",
            "큐 유형": "High/Medium/Low 우선순위",
            "배치 처리": "1,000개 단위",
            "상태": "정상 작동"
        }
    )

    print(f"\n[READY] Redis 큐 시스템 준비 완료!")
    return True


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)

    success = main()
    if not success:
        print("\n[FAILED] Redis 큐 테스트 실패")
        print("Redis 서버를 시작하려면: redis-server")
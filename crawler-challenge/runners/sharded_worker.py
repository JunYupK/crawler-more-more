#!/usr/bin/env python3
"""
Sharded Distributed Crawler - Worker Node
"""
import sys
import os
import asyncio
import logging
import signal
import argparse
import time
from datetime import datetime
from typing import List, Dict, Optional
from enum import Enum
from urllib.parse import urlparse

# Add src to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.core.polite_crawler import PoliteCrawler
from src.core.database import DatabaseManager
from src.managers.sharded_queue_manager import ShardedRedisQueueManager
from src.monitoring.metrics import MetricsManager


class CrawlErrorType(Enum):
    """크롤링 에러 타입 분류

    에러 모니터링 및 분석을 위한 세분화된 에러 분류:
    - 네트워크 에러: TIMEOUT, CONNECTION_ERROR, DNS_ERROR, SSL_ERROR
    - HTTP 에러: HTTP_403, HTTP_404, HTTP_429, HTTP_5XX
    - 크롤링 정책: ROBOTS_BLOCKED
    - 기타: UNKNOWN
    """
    TIMEOUT = "timeout"
    SSL_ERROR = "ssl_error"
    CONNECTION_ERROR = "connection_error"
    HTTP_403 = "http_403_forbidden"
    HTTP_404 = "http_404_not_found"
    HTTP_429 = "http_429_rate_limit"
    HTTP_5XX = "http_5xx_server_error"
    DNS_ERROR = "dns_error"
    ROBOTS_BLOCKED = "robots_blocked"
    CONTENT_ERROR = "content_error"
    UNKNOWN = "unknown"


def classify_crawl_error(error_message: str, status_code: int = None) -> str:
    """에러 메시지와 상태 코드를 기반으로 에러 타입 분류

    Args:
        error_message: 에러 메시지 문자열
        status_code: HTTP 상태 코드 (있는 경우)

    Returns:
        CrawlErrorType의 value 문자열
    """
    error_lower = error_message.lower() if error_message else ""

    # HTTP 상태 코드 기반 분류
    if status_code:
        if status_code == 403:
            return CrawlErrorType.HTTP_403.value
        elif status_code == 404:
            return CrawlErrorType.HTTP_404.value
        elif status_code == 429:
            return CrawlErrorType.HTTP_429.value
        elif 500 <= status_code < 600:
            return CrawlErrorType.HTTP_5XX.value

    # 에러 메시지 기반 분류
    if "timeout" in error_lower:
        return CrawlErrorType.TIMEOUT.value
    elif "ssl" in error_lower or "certificate" in error_lower:
        return CrawlErrorType.SSL_ERROR.value
    elif any(x in error_lower for x in ["connection", "connect", "refused", "reset"]):
        return CrawlErrorType.CONNECTION_ERROR.value
    elif any(x in error_lower for x in ["dns", "name resolution", "getaddrinfo", "nodename"]):
        return CrawlErrorType.DNS_ERROR.value
    elif "robots" in error_lower or "blocked" in error_lower or "불허" in error_lower:
        return CrawlErrorType.ROBOTS_BLOCKED.value
    elif any(x in error_lower for x in ["content", "decode", "encoding", "charset"]):
        return CrawlErrorType.CONTENT_ERROR.value

    return CrawlErrorType.UNKNOWN.value

# 로깅 설정
def setup_logging(worker_id: int):
    log_dir = os.path.join(os.path.dirname(__file__), '../logs')
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, f'sharded_crawler_worker_{worker_id}.log')
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )

class ShardedCrawlerWorker:
    """샤딩된 분산 크롤러 워커 노드"""

    def __init__(self, worker_id: int, batch_size: int = 50):
        self.worker_id = worker_id
        self.batch_size = batch_size
        self.should_stop = False

        # 샤딩된 Redis 연결 설정
        redis_host = os.getenv('REDIS_HOST', 'localhost')
        postgres_host = os.getenv('POSTGRES_HOST', 'localhost')

        shard_configs = [
            {'host': redis_host, 'port': 6379, 'db': 1},
            {'host': redis_host, 'port': 6379, 'db': 2},
            {'host': redis_host, 'port': 6379, 'db': 3}
        ]

        # 컴포넌트들
        self.queue_manager = ShardedRedisQueueManager(shard_configs)
        self.db_manager = DatabaseManager(host=postgres_host)

        # Prometheus 메트릭 (워커별 포트: 8001 + worker_id)
        metrics_port = 8001 + worker_id
        self.metrics = MetricsManager(port=metrics_port)

        # 통계
        self.total_processed = 0
        self.total_successful = 0
        self.total_failed = 0
        self.start_time = datetime.now()
        self.shard_distribution = {}  # 샤드별 처리 통계
        self.error_type_stats = {}  # 에러 타입별 통계

        # 신호 핸들러
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """종료 신호 처리"""
        logging.info(f"샤딩 워커 {self.worker_id}: 종료 신호 수신: {signum}")
        self.should_stop = True

    async def initialize(self) -> bool:
        """워커 초기화"""
        try:
            logging.info(f"=== 샤딩 워커 {self.worker_id} 초기화 시작 ===")

            # Redis 샤드 연결 확인
            if not self.queue_manager.test_connection():
                logging.error("Redis 샤드 연결 실패")
                return False

            # Prometheus 메트릭 서버 시작
            try:
                self.metrics.start_server()
                logging.info(f"[OK] Prometheus 메트릭 서버 시작 (port: {self.metrics.port})")
            except Exception as e:
                logging.warning(f"Prometheus 서버 시작 실패 (계속 진행): {e}")

            logging.info(f"[OK] 샤딩 워커 {self.worker_id} 초기화 완료 (랜덤 샤드 선택)")
            return True

        except Exception as e:
            logging.error(f"샤딩 워커 {self.worker_id} 초기화 실패: {e}")
            return False

    async def process_batch(self, batch: List[Dict]) -> bool:
        """배치 처리 (샤드 정보 포함)"""
        try:
            if not batch:
                return False

            batch_urls = [item['url'] for item in batch]
            logging.info(f"샤딩 워커 {self.worker_id}: 배치 처리 시작 ({len(batch_urls)}개 URL)")

            # 샤드별 분포 추적
            shard_counts = {}
            for item in batch:
                shard_id = item.get('shard_id', -1)
                shard_counts[shard_id] = shard_counts.get(shard_id, 0) + 1

            logging.debug(f"배치 샤드 분포: {shard_counts}")

            # 임시 크롤러 생성 (배치별로)
            
            async with PoliteCrawler(respect_robots_txt=False) as crawler:
                results = await crawler.crawl_batch_politely(batch_urls)

                # 결과 처리
                successful_count = 0
                failed_count = 0

                for i, result in enumerate(results):
                    original_item = batch[i] if i < len(batch) else {}
                    shard_id = original_item.get('shard_id', -1)

                    try:
                        if result['success'] and result.get('content'):
                            # 성공 - 데이터베이스에 저장
                            self.db_manager.add_to_batch(result['url'], result['content'])
                            self.queue_manager.mark_completed(result['url'], success=True, shard_id=shard_id)
                            successful_count += 1

                            # 샤드별 성공 통계
                            if shard_id not in self.shard_distribution:
                                self.shard_distribution[shard_id] = {'success': 0, 'failed': 0}
                            self.shard_distribution[shard_id]['success'] += 1

                            # Prometheus 메트릭 기록 (성공)
                            self.metrics.record_crawl_result(self.worker_id, {
                                'success': True,
                                'url': result['url'],
                                'domain': result.get('domain', urlparse(result['url']).netloc),
                                'status_code': result.get('status'),
                                'response_time': result.get('response_time')
                            })

                        else:
                            # 실패 처리 - 에러 타입 분류
                            error_message = result.get('error', 'Unknown error')
                            status_code = result.get('status')
                            error_type = classify_crawl_error(error_message, status_code)
                            domain = result.get('domain', urlparse(result['url']).netloc)

                            error_info = {
                                'type': error_type,
                                'message': error_message,
                                'status_code': status_code,
                                'domain': domain,
                                'recoverable': error_type in [
                                    CrawlErrorType.TIMEOUT.value,
                                    CrawlErrorType.HTTP_429.value,
                                    CrawlErrorType.HTTP_5XX.value,
                                    CrawlErrorType.CONNECTION_ERROR.value
                                ],
                                'worker_id': self.worker_id,
                                'shard_id': shard_id,
                                'timestamp': datetime.now().isoformat()
                            }

                            # 상세 에러 로깅
                            logging.warning(
                                f"\n{'='*60}\n"
                                f"CRAWL FAILED\n"
                                f"{'='*60}\n"
                                f"URL:           {result['url']}\n"
                                f"Domain:        {domain}\n"
                                f"Error Type:    {error_type}\n"
                                f"Error Message: {error_message}\n"
                                f"Status Code:   {status_code}\n"
                                f"Worker ID:     {self.worker_id}\n"
                                f"Shard ID:      {shard_id}\n"
                                f"{'='*60}"
                            )

                            self.queue_manager.mark_completed(
                                result['url'],
                                success=False,
                                error_info=error_info,
                                shard_id=shard_id
                            )
                            failed_count += 1

                            # 샤드별 실패 통계
                            if shard_id not in self.shard_distribution:
                                self.shard_distribution[shard_id] = {'success': 0, 'failed': 0}
                            self.shard_distribution[shard_id]['failed'] += 1

                            # 에러 타입별 통계
                            self.error_type_stats[error_type] = self.error_type_stats.get(error_type, 0) + 1

                            # Prometheus 메트릭 기록 (실패)
                            self.metrics.record_crawl_result(self.worker_id, {
                                'success': False,
                                'url': result['url'],
                                'domain': domain,
                                'error_type': error_type,
                                'status_code': status_code,
                                'response_time': result.get('response_time')
                            })

                    except Exception as e:
                        logging.error(f"샤딩 워커 {self.worker_id} 결과 처리 오류: {e}")
                        failed_count += 1

                # 통계 업데이트
                self.total_processed += len(results)
                self.total_successful += successful_count
                self.total_failed += failed_count

                # Prometheus 성공률 게이지 업데이트
                if self.total_processed > 0:
                    success_rate = (self.total_successful / self.total_processed) * 100
                    self.metrics.update_success_rate(self.worker_id, success_rate)

                logging.info(f"샤딩 워커 {self.worker_id} 배치 완료: {successful_count}개 성공, {failed_count}개 실패")
                return True

        except Exception as e:
            logging.error(f"샤딩 워커 {self.worker_id} 배치 처리 실패: {e}")
            return False

    async def run(self):
        """샤딩 워커 실행"""
        try:
            # 초기화
            if not await self.initialize():
                logging.error(f"샤딩 워커 {self.worker_id} 초기화 실패")
                return False

            logging.info(f"샤딩 워커 {self.worker_id} 작업 시작 (랜덤 샤드 선택)")

            consecutive_empty = 0

            while not self.should_stop:
                try:
                    # 샤딩된 큐에서 배치 가져오기 (랜덤 샤드 순서)
                    batch = self.queue_manager.get_next_batch(
                        batch_size=self.batch_size
                    )

                    if not batch:
                        consecutive_empty += 1
                        if consecutive_empty >= 10:  # 10회 연속 빈 배치면 종료
                            logging.info(f"샤딩 워커 {self.worker_id}: 더 이상 작업이 없습니다")
                            break
                        await asyncio.sleep(5)
                        continue

                    consecutive_empty = 0

                    # 배치 처리
                    await self.process_batch(batch)

                    # 짧은 대기
                    await asyncio.sleep(1)

                except Exception as e:
                    logging.error(f"샤딩 워커 {self.worker_id} 실행 오류: {e}")
                    await asyncio.sleep(5)

            # 최종 통계
            duration = datetime.now() - self.start_time
            duration_minutes = duration.total_seconds() / 60

            logging.info(f"=== 샤딩 워커 {self.worker_id} 완료 ===")
            logging.info(f"실행 시간: {duration_minutes:.1f}분")
            logging.info(f"총 처리: {self.total_processed}개")
            logging.info(f"성공: {self.total_successful}개")
            logging.info(f"실패: {self.total_failed}개")

            if self.total_processed > 0:
                success_rate = self.total_successful / self.total_processed * 100
                pages_per_minute = self.total_processed / duration_minutes if duration_minutes > 0 else 0
                logging.info(f"성공률: {success_rate:.1f}%")
                logging.info(f"처리속도: {pages_per_minute:.1f} pages/분")

            # 샤드별 처리 분포
            logging.info(f"샤드별 처리 분포:")
            for shard_id, stats in self.shard_distribution.items():
                total = stats['success'] + stats['failed']
                logging.info(f"  샤드 {shard_id}: {total}개 ({stats['success']}성공, {stats['failed']}실패)")

            # DB 정리
            if self.db_manager:
                self.db_manager.flush_batch()
                self.db_manager.close_all_connections()

            return True

        except Exception as e:
            logging.error(f"샤딩 워커 {self.worker_id} 실행 오류: {e}")
            return False

async def main():
    parser = argparse.ArgumentParser(description='Sharded Distributed Crawler - Worker')
    parser.add_argument('--worker-id', type=int, default=1, help='워커 ID')
    parser.add_argument('--batch-size', type=int, default=50, help='배치 크기')

    args = parser.parse_args()

    setup_logging(args.worker_id)

    print(f"샤딩된 분산 크롤러 워커 {args.worker_id} 시작 (랜덤 샤드 선택)")
    worker = ShardedCrawlerWorker(
        worker_id=args.worker_id,
        batch_size=args.batch_size
    )
    success = await worker.run()
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    asyncio.run(main())

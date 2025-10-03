#!/usr/bin/env python3
"""
Distributed Crawler - 마스터-워커 분산 크롤러
"""

import asyncio
import logging
import sys
import signal
import os
import time
from datetime import datetime
from typing import List, Dict, Any, Optional
import argparse

from tranco_manager import TrancoManager
from redis_queue_manager import RedisQueueManager
from multithreaded_crawler import MultithreadedCrawler
from database import DatabaseManager
from progress_tracker import ProgressTracker
from monitoring.metrics import MetricsMonitor

# 로깅 설정
def setup_logging(worker_id: Optional[int] = None):
    log_file = f'distributed_crawler_worker_{worker_id}.log' if worker_id else 'distributed_crawler_master.log'
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(f'logs/{log_file}', encoding='utf-8'),
            logging.StreamHandler()
        ]
    )

class DistributedCrawlerMaster:
    """분산 크롤러 마스터 노드"""

    def __init__(self, worker_count: int = 4):
        self.worker_count = worker_count
        self.should_stop = False

        # Redis 연결 설정
        redis_host = os.getenv('REDIS_HOST', 'localhost')
        self.queue_manager = RedisQueueManager(redis_host=redis_host)

        # 컴포넌트들
        self.tranco_manager: Optional[TrancoManager] = None
        self.progress_tracker: Optional[ProgressTracker] = None

        # 통계
        self.start_time = datetime.now()

        # 신호 핸들러
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """종료 신호 처리"""
        logging.info(f"마스터: 종료 신호 수신: {signum}")
        self.should_stop = True

    async def initialize(self) -> bool:
        """마스터 초기화"""
        try:
            logging.info("=== 분산 크롤러 마스터 초기화 시작 ===")

            # 1. Tranco Manager
            logging.info("1. Tranco Manager 초기화...")
            self.tranco_manager = TrancoManager()

            # 2. Redis Queue Manager
            logging.info("2. Redis Queue Manager 초기화...")
            if not self.queue_manager.test_connection():
                logging.error("Redis 연결 실패")
                return False

            # 3. Progress Tracker
            logging.info("3. Progress Tracker 초기화...")
            self.progress_tracker = ProgressTracker()
            if not self.progress_tracker.test_connection():
                logging.error("Progress Tracker Redis 연결 실패")
                return False

            logging.info(f"[OK] 마스터 초기화 완료 (워커 {self.worker_count}개 예상)")
            return True

        except Exception as e:
            logging.error(f"마스터 초기화 실패: {e}")
            return False

    async def prepare_work(self, url_count: int = 400) -> bool:
        """작업 준비 및 큐에 배포"""
        try:
            logging.info(f"=== 작업 준비 ({url_count}개 URL) ===")

            # URL 데이터셋 준비
            urls = await self.tranco_manager.prepare_url_dataset(initial_count=url_count)
            if not urls:
                logging.error("URL 데이터셋 준비 실패")
                return False

            # Redis 큐에 로드
            if not self.queue_manager.initialize_queues(urls):
                logging.error("큐 초기화 실패")
                return False

            # 통계 출력
            queue_stats = self.queue_manager.get_queue_stats()
            logging.info(f"큐 로딩 완료:")
            logging.info(f"  - 고우선순위: {queue_stats.get('queue_priority_high', 0)}")
            logging.info(f"  - 중우선순위: {queue_stats.get('queue_priority_medium', 0)}")
            logging.info(f"  - 일반우선순위: {queue_stats.get('queue_priority_normal', 0)}")
            logging.info(f"  - 저우선순위: {queue_stats.get('queue_priority_low', 0)}")
            logging.info(f"  - 총 URL: {queue_stats.get('total_urls', 0)}개")

            return True

        except Exception as e:
            logging.error(f"작업 준비 실패: {e}")
            return False

    async def monitor_workers(self):
        """워커들 모니터링"""
        logging.info("=== 워커 모니터링 시작 ===")

        last_report = datetime.now()

        while not self.should_stop:
            try:
                # 큐 상태 확인
                queue_stats = self.queue_manager.get_queue_stats()

                # 5분마다 리포트
                if (datetime.now() - last_report).total_seconds() > 300:
                    logging.info("📊 마스터 리포트:")
                    logging.info(f"  - 완료: {queue_stats.get('completed', 0)}개")
                    logging.info(f"  - 대기: {queue_stats.get('total_pending', 0)}개")
                    logging.info(f"  - 처리 중: {queue_stats.get('processing', 0)}개")
                    logging.info(f"  - 실패: {queue_stats.get('failed', 0)}개")
                    logging.info(f"  - 완료율: {queue_stats.get('completion_rate', 0):.1%}")

                    last_report = datetime.now()

                # 모든 작업 완료 확인
                if (queue_stats.get('total_pending', 0) == 0 and
                    queue_stats.get('processing', 0) == 0 and
                    queue_stats.get('completed', 0) > 0):
                    logging.info("✅ 모든 작업 완료")
                    break

                await asyncio.sleep(30)  # 30초마다 확인

            except Exception as e:
                logging.error(f"모니터링 오류: {e}")
                await asyncio.sleep(30)

    async def run(self, url_count: int = 400):
        """마스터 실행"""
        try:
            # 초기화
            if not await self.initialize():
                logging.error("마스터 초기화 실패")
                return False

            # 작업 준비
            if not await self.prepare_work(url_count):
                logging.error("작업 준비 실패")
                return False

            # 워커 모니터링
            await self.monitor_workers()

            return True

        except Exception as e:
            logging.error(f"마스터 실행 오류: {e}")
            return False

class DistributedCrawlerWorker:
    """분산 크롤러 워커 노드"""

    def __init__(self, worker_id: int, batch_size: int = 25, worker_threads: int = 2):
        self.worker_id = worker_id
        self.batch_size = batch_size
        self.worker_threads = worker_threads
        self.should_stop = False

        # Redis 연결 설정
        redis_host = os.getenv('REDIS_HOST', 'localhost')
        postgres_host = os.getenv('POSTGRES_HOST', 'localhost')

        # 컴포넌트들
        self.queue_manager = RedisQueueManager(redis_host=redis_host)
        self.db_manager = DatabaseManager(host=postgres_host)
        self.crawler: Optional[MultithreadedCrawler] = None

        # 통계
        self.total_processed = 0
        self.total_successful = 0
        self.total_failed = 0
        self.start_time = datetime.now()

        # 신호 핸들러
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """종료 신호 처리"""
        logging.info(f"워커 {self.worker_id}: 종료 신호 수신: {signum}")
        self.should_stop = True

    async def initialize(self) -> bool:
        """워커 초기화"""
        try:
            logging.info(f"=== 워커 {self.worker_id} 초기화 시작 ===")

            # Redis 연결 확인
            if not self.queue_manager.test_connection():
                logging.error("Redis 연결 실패")
                return False

            logging.info(f"[OK] 워커 {self.worker_id} 초기화 완료")
            return True

        except Exception as e:
            logging.error(f"워커 {self.worker_id} 초기화 실패: {e}")
            return False

    async def process_batch(self, batch: List[Dict]) -> bool:
        """배치 처리"""
        try:
            if not batch:
                return False

            batch_urls = [item['url'] for item in batch]
            logging.info(f"워커 {self.worker_id}: 배치 처리 시작 ({len(batch_urls)}개 URL)")

            # 임시 크롤러 생성 (배치별로)
            from polite_crawler import PoliteCrawler

            async with PoliteCrawler(respect_robots_txt=False) as crawler:
                results = await crawler.crawl_batch_politely(batch_urls)

                # 결과 처리
                successful_count = 0
                failed_count = 0

                for result in results:
                    try:
                        if result['success'] and result.get('content'):
                            # 성공 - 데이터베이스에 저장
                            self.db_manager.add_to_batch(result['url'], result['content'])
                            self.queue_manager.mark_completed(result['url'], success=True)
                            successful_count += 1

                        else:
                            # 실패 처리
                            error_message = result.get('error', 'Unknown error')
                            error_info = {
                                'type': 'crawl_error',
                                'message': error_message,
                                'recoverable': 'timeout' in error_message.lower(),
                                'worker_id': self.worker_id
                            }

                            self.queue_manager.mark_completed(
                                result['url'],
                                success=False,
                                error_info=error_info
                            )
                            failed_count += 1

                    except Exception as e:
                        logging.error(f"워커 {self.worker_id} 결과 처리 오류: {e}")
                        failed_count += 1

                # 통계 업데이트
                self.total_processed += len(results)
                self.total_successful += successful_count
                self.total_failed += failed_count

                logging.info(f"워커 {self.worker_id} 배치 완료: {successful_count}개 성공, {failed_count}개 실패")
                return True

        except Exception as e:
            logging.error(f"워커 {self.worker_id} 배치 처리 실패: {e}")
            return False

    async def run(self):
        """워커 실행"""
        try:
            # 초기화
            if not await self.initialize():
                logging.error(f"워커 {self.worker_id} 초기화 실패")
                return False

            logging.info(f"워커 {self.worker_id} 작업 시작")

            consecutive_empty = 0

            while not self.should_stop:
                try:
                    # 큐에서 배치 가져오기
                    batch = self.queue_manager.get_next_batch(self.batch_size)

                    if not batch:
                        consecutive_empty += 1
                        if consecutive_empty >= 10:  # 10회 연속 빈 배치면 종료
                            logging.info(f"워커 {self.worker_id}: 더 이상 작업이 없습니다")
                            break
                        await asyncio.sleep(5)
                        continue

                    consecutive_empty = 0

                    # 배치 처리
                    await self.process_batch(batch)

                    # 짧은 대기
                    await asyncio.sleep(1)

                except Exception as e:
                    logging.error(f"워커 {self.worker_id} 실행 오류: {e}")
                    await asyncio.sleep(5)

            # 최종 통계
            duration = datetime.now() - self.start_time
            duration_minutes = duration.total_seconds() / 60

            logging.info(f"=== 워커 {self.worker_id} 완료 ===")
            logging.info(f"실행 시간: {duration_minutes:.1f}분")
            logging.info(f"총 처리: {self.total_processed}개")
            logging.info(f"성공: {self.total_successful}개")
            logging.info(f"실패: {self.total_failed}개")
            if self.total_processed > 0:
                success_rate = self.total_successful / self.total_processed * 100
                pages_per_minute = self.total_processed / duration_minutes if duration_minutes > 0 else 0
                logging.info(f"성공률: {success_rate:.1f}%")
                logging.info(f"처리속도: {pages_per_minute:.1f} pages/분")

            # DB 정리
            if self.db_manager:
                self.db_manager.flush_batch()
                self.db_manager.close_all_connections()

            return True

        except Exception as e:
            logging.error(f"워커 {self.worker_id} 실행 오류: {e}")
            return False

async def main():
    """메인 함수"""
    parser = argparse.ArgumentParser(description='Distributed Crawler - Master/Worker')
    parser.add_argument('--mode', choices=['master', 'worker'], required=True,
                       help='실행 모드')
    parser.add_argument('--worker-id', type=int, default=1,
                       help='워커 ID (worker 모드일 때)')
    parser.add_argument('--count', type=int, default=400,
                       help='크롤링할 URL 개수 (master 모드일 때)')
    parser.add_argument('--workers', type=int, default=4,
                       help='워커 수 (master 모드일 때)')
    parser.add_argument('--batch-size', type=int, default=25,
                       help='배치 크기 (worker 모드일 때)')
    parser.add_argument('--threads', type=int, default=2,
                       help='워커별 스레드 수 (worker 모드일 때)')

    args = parser.parse_args()

    # 로그 디렉토리 생성
    os.makedirs('logs', exist_ok=True)

    # 로깅 설정
    setup_logging(args.worker_id if args.mode == 'worker' else None)

    try:
        if args.mode == 'master':
            print(f"분산 크롤러 마스터 시작 (워커 {args.workers}개, URL {args.count}개)")
            master = DistributedCrawlerMaster(worker_count=args.workers)
            success = await master.run(url_count=args.count)
            sys.exit(0 if success else 1)

        elif args.mode == 'worker':
            print(f"분산 크롤러 워커 {args.worker_id} 시작")
            worker = DistributedCrawlerWorker(
                worker_id=args.worker_id,
                batch_size=args.batch_size,
                worker_threads=args.threads
            )
            success = await worker.run()
            sys.exit(0 if success else 1)

    except KeyboardInterrupt:
        logging.info("사용자에 의한 중단")
        sys.exit(0)

    except Exception as e:
        logging.error(f"예상치 못한 오류: {e}")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())
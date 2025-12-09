#!/usr/bin/env python3
"""
Multithreaded Enterprise Crawler - 멀티스레딩을 활용한 고성능 크롤러
"""

import asyncio
import logging
import sys
import signal
import threading
import concurrent.futures
from datetime import datetime
from typing import List, Dict, Any, Optional
from queue import Queue, Empty
import time

from tranco_manager import TrancoManager
from redis_queue_manager import RedisQueueManager
from polite_crawler import PoliteCrawler
from database import DatabaseManager
from progress_tracker import ProgressTracker
from monitoring.metrics import MetricsMonitor

# Config import 추가
try:
    from config.settings import WORKER_THREADS, BATCH_SIZE
except ImportError:
    WORKER_THREADS = 16
    BATCH_SIZE = 100

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('multithreaded_crawler.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

class MultithreadedCrawler:
    """멀티스레딩을 활용한 대규모 크롤러"""

    def __init__(self, initial_url_count: int = 1000, worker_threads: int = None):
        self.initial_url_count = initial_url_count
        self.worker_threads = worker_threads if worker_threads is not None else WORKER_THREADS  # 기본 16개
        self.should_stop = False
        self.batch_size = BATCH_SIZE  # 공격적 배치 크기 100

        # 컴포넌트들
        self.tranco_manager: Optional[TrancoManager] = None
        self.queue_manager: Optional[RedisQueueManager] = None
        self.db_manager: Optional[DatabaseManager] = None
        self.progress_tracker: Optional[ProgressTracker] = None
        self.metrics_monitor: Optional[MetricsMonitor] = None

        # 멀티스레딩 관련
        self.thread_pool = None
        self.worker_queue = Queue()
        self.result_queue = Queue()
        self.active_workers = 0
        self.workers_lock = threading.Lock()

        # 통계
        self.total_processed = 0
        self.total_successful = 0
        self.total_failed = 0
        self.start_time = datetime.now()
        self.stats_lock = threading.Lock()

        # 신호 핸들러
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """종료 신호 처리"""
        logger.info(f"종료 신호 수신: {signum}")
        self.should_stop = True

    async def initialize(self) -> bool:
        """모든 컴포넌트 초기화"""
        try:
            logger.info("=== Multithreaded Crawler 초기화 시작 ===")

            # 1. Tranco Manager
            logger.info("1. Tranco Manager 초기화...")
            self.tranco_manager = TrancoManager()

            # 2. Redis Queue Manager
            logger.info("2. Redis Queue Manager 초기화...")
            self.queue_manager = RedisQueueManager()
            if not self.queue_manager.test_connection():
                logger.error("Redis 연결 실패")
                return False

            # 3. Database Manager
            logger.info("3. Database Manager 초기화...")
            self.db_manager = DatabaseManager()

            # 4. Progress Tracker
            logger.info("4. Progress Tracker 초기화...")
            self.progress_tracker = ProgressTracker()
            if not self.progress_tracker.test_connection():
                logger.error("Progress Tracker Redis 연결 실패")
                return False

            # 5. Metrics Monitor
            logger.info("5. Metrics Monitor 초기화...")
            self.metrics_monitor = MetricsMonitor()
            await self.metrics_monitor.start_monitoring()

            # 6. Thread Pool 초기화
            logger.info(f"6. Thread Pool 초기화 ({self.worker_threads}개 워커)...")
            self.thread_pool = concurrent.futures.ThreadPoolExecutor(
                max_workers=self.worker_threads,
                thread_name_prefix="CrawlerWorker"
            )

            logger.info("[OK] 모든 컴포넌트 초기화 완료")
            return True

        except Exception as e:
            logger.error(f"초기화 실패: {e}")
            return False

    async def prepare_url_dataset(self) -> bool:
        """URL 데이터셋 준비"""
        try:
            logger.info(f"=== URL 데이터셋 준비 ({self.initial_url_count}개) ===")

            # Tranco 리스트에서 URL 생성
            urls = await self.tranco_manager.prepare_url_dataset(
                initial_count=self.initial_url_count
            )

            if not urls:
                logger.error("URL 데이터셋 준비 실패")
                return False

            # Redis 큐에 로드
            if not self.queue_manager.initialize_queues(urls):
                logger.error("큐 초기화 실패")
                return False

            # 통계 출력
            queue_stats = self.queue_manager.get_queue_stats()
            logger.info(f"큐 로딩 완료:")
            logger.info(f"  - 고우선순위: {queue_stats.get('queue_priority_high', 0)}")
            logger.info(f"  - 중우선순위: {queue_stats.get('queue_priority_medium', 0)}")
            logger.info(f"  - 일반우선순위: {queue_stats.get('queue_priority_normal', 0)}")
            logger.info(f"  - 저우선순위: {queue_stats.get('queue_priority_low', 0)}")
            logger.info(f"  - 총 URL: {queue_stats.get('total_urls', 0)}개")

            return True

        except Exception as e:
            logger.error(f"URL 데이터셋 준비 실패: {e}")
            return False

    def crawl_worker(self, worker_id: int):
        """워커 스레드에서 실행되는 크롤링 함수"""
        logger.info(f"Worker {worker_id} 시작")

        # 각 워커별로 독립적인 크롤러 생성
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        try:
            # PoliteCrawler를 동기 방식으로 초기화
            crawler = PoliteCrawler(respect_robots_txt=False)

            # asyncio context manager 설정
            async def init_crawler():
                await crawler.__aenter__()
                return crawler

            crawler = loop.run_until_complete(init_crawler())

            while not self.should_stop:
                try:
                    # 작업 큐에서 배치 가져오기
                    batch = self.worker_queue.get(timeout=5)
                    if batch is None:  # 종료 신호
                        break

                    with self.workers_lock:
                        self.active_workers += 1

                    try:
                        # 비동기 크롤링 실행
                        results = loop.run_until_complete(
                            crawler.crawl_batch_politely([item['url'] for item in batch])
                        )

                        # 결과를 결과 큐에 추가
                        self.result_queue.put({
                            'worker_id': worker_id,
                            'batch': batch,
                            'results': results,
                            'timestamp': datetime.now()
                        })

                    finally:
                        with self.workers_lock:
                            self.active_workers -= 1
                        self.worker_queue.task_done()

                except Empty:
                    continue
                except Exception as e:
                    logger.error(f"Worker {worker_id} 오류: {e}")
                    with self.workers_lock:
                        self.active_workers -= 1

        except Exception as e:
            logger.error(f"Worker {worker_id} 초기화 실패: {e}")
        finally:
            # 크롤러 정리
            try:
                loop.run_until_complete(crawler.__aexit__(None, None, None))
            except:
                pass
            loop.close()
            logger.info(f"Worker {worker_id} 종료")

    async def process_results(self):
        """결과 처리 코루틴"""
        while not self.should_stop:
            try:
                # 결과 큐에서 처리할 결과 가져오기
                if self.result_queue.empty():
                    await asyncio.sleep(0.1)
                    continue

                try:
                    result_data = self.result_queue.get_nowait()
                except Empty:
                    continue

                batch = result_data['batch']
                results = result_data['results']
                worker_id = result_data['worker_id']

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

                            if self.metrics_monitor:
                                self.metrics_monitor.increment_pages()

                        else:
                            # 실패 처리
                            error_message = result.get('error', 'Unknown error')
                            error_info = {
                                'type': 'crawl_error',
                                'message': error_message,
                                'recoverable': 'timeout' in error_message.lower()
                            }

                            self.queue_manager.mark_completed(
                                result['url'],
                                success=False,
                                error_info=error_info
                            )
                            failed_count += 1

                            # Progress tracker에 에러 저장
                            if self.progress_tracker:
                                self.progress_tracker.save_error(error_info)

                    except Exception as e:
                        logger.error(f"결과 처리 오류: {e}")
                        failed_count += 1

                # 통계 업데이트
                with self.stats_lock:
                    self.total_processed += len(results)
                    self.total_successful += successful_count
                    self.total_failed += failed_count

                logger.info(f"Worker {worker_id} 배치 완료: {successful_count}개 성공, {failed_count}개 실패")

            except Exception as e:
                logger.error(f"결과 처리 중 오류: {e}")

    async def run_continuous(self):
        """연속 크롤링 실행"""
        logger.info("=== 멀티스레딩 연속 크롤링 시작 ===")

        # 워커 스레드들 시작
        logger.info(f"{self.worker_threads}개 워커 스레드 시작...")
        worker_futures = []
        for i in range(self.worker_threads):
            future = self.thread_pool.submit(self.crawl_worker, i)
            worker_futures.append(future)

        # 결과 처리 태스크 시작
        result_processor = asyncio.create_task(self.process_results())

        cycle_count = 0
        last_progress_save = datetime.now()

        try:
            while not self.should_stop:
                cycle_count += 1
                logger.info(f"\n--- 사이클 #{cycle_count} ---")

                # 큐에서 다음 배치들 가져오기
                total_batch_size = self.batch_size * self.worker_threads
                batch = self.queue_manager.get_next_batch(total_batch_size)

                if not batch:
                    logger.info("큐에 더 이상 URL이 없습니다")
                    break

                # 배치를 워커들에게 분배
                batch_per_worker = len(batch) // self.worker_threads
                for i in range(self.worker_threads):
                    start_idx = i * batch_per_worker
                    end_idx = start_idx + batch_per_worker if i < self.worker_threads - 1 else len(batch)
                    worker_batch = batch[start_idx:end_idx]

                    if worker_batch:
                        self.worker_queue.put(worker_batch)

                logger.info(f"배치 분배 완료: {len(batch)}개 URL을 {self.worker_threads}개 워커에게 할당")

                # 워커들이 작업을 완료할 때까지 대기
                while self.active_workers > 0 and not self.should_stop:
                    await asyncio.sleep(1)

                # 주기적으로 진행 상황 저장 (5분마다)
                if (datetime.now() - last_progress_save).total_seconds() > 300:
                    self.save_progress()
                    last_progress_save = datetime.now()

                    # 큐 통계 출력
                    queue_stats = self.queue_manager.get_queue_stats()
                    logger.info(f"📊 진행상황: {queue_stats.get('completed', 0)}개 완료, "
                              f"{queue_stats.get('total_pending', 0)}개 대기 "
                              f"(완료율: {queue_stats.get('completion_rate', 0):.1%})")

                # 짧은 대기
                await asyncio.sleep(2)

        except Exception as e:
            logger.error(f"연속 크롤링 오류: {e}")

        finally:
            # 워커들에게 종료 신호 전송
            for _ in range(self.worker_threads):
                self.worker_queue.put(None)

            # 결과 처리 태스크 종료
            result_processor.cancel()

            # 최종 진행 상황 저장
            self.save_progress()

    def save_progress(self):
        """진행 상황 저장"""
        try:
            if not self.progress_tracker:
                return

            uptime_minutes = int((datetime.now() - self.start_time).total_seconds() / 60)
            pages_per_minute = self.total_processed / uptime_minutes if uptime_minutes > 0 else 0

            # 큐 상태
            queue_stats = self.queue_manager.get_queue_stats()

            # 도메인 통계
            domain_stats = self.queue_manager.get_domain_stats()

            stats = {
                'total_processed': self.total_processed,
                'success_count': self.total_successful,
                'error_count': self.total_failed,
                'error_rate': self.total_failed / self.total_processed if self.total_processed > 0 else 0,
                'pages_per_minute': round(pages_per_minute, 2),
                'uptime_minutes': uptime_minutes,
                'queue_pending': queue_stats.get('total_pending', 0),
                'queue_completed': queue_stats.get('completed', 0),
                'completion_rate': queue_stats.get('completion_rate', 0),
                'worker_threads': self.worker_threads,
                'active_workers': self.active_workers,
            }

            # DB 상태
            db_pool_stats = self.db_manager.get_pool_stats() if self.db_manager else {}
            stats.update({
                'db_pages_count': self.db_manager.get_crawled_count() if self.db_manager else 0,
                'db_pool_min': db_pool_stats.get('pool_min', 0),
                'db_pool_max': db_pool_stats.get('pool_max', 0),
            })

            self.progress_tracker.save_progress(stats)
        except Exception as e:
            logger.error(f"진행 상황 저장 실패: {e}")

    async def cleanup(self):
        """리소스 정리"""
        try:
            logger.info("=== 리소스 정리 시작 ===")

            if self.thread_pool:
                self.thread_pool.shutdown(wait=True)

            if self.db_manager:
                self.db_manager.flush_batch()
                self.db_manager.close_all_connections()

            if self.metrics_monitor:
                await self.metrics_monitor.stop_monitoring()

            if self.progress_tracker:
                self.progress_tracker.mark_session_completed()

            logger.info("[OK] 리소스 정리 완료")

        except Exception as e:
            logger.error(f"리소스 정리 오류: {e}")

    def print_final_stats(self):
        """최종 통계 출력"""
        duration = datetime.now() - self.start_time
        duration_minutes = duration.total_seconds() / 60

        logger.info("\n" + "="*60)
        logger.info("            최종 멀티스레딩 크롤링 통계")
        logger.info("="*60)
        logger.info(f"실행 시간      : {duration_minutes:.1f}분")
        logger.info(f"워커 스레드    : {self.worker_threads}개")
        logger.info(f"총 처리        : {self.total_processed:,}개")
        logger.info(f"성공          : {self.total_successful:,}개")
        logger.info(f"실패          : {self.total_failed:,}개")

        if self.total_processed > 0:
            success_rate = self.total_successful / self.total_processed * 100
            pages_per_minute = self.total_processed / duration_minutes if duration_minutes > 0 else 0

            logger.info(f"성공률        : {success_rate:.1f}%")
            logger.info(f"처리속도      : {pages_per_minute:.1f} pages/분")
            logger.info(f"처리속도      : {pages_per_minute/60:.2f} pages/초")

        # DB 통계
        if self.db_manager:
            db_count = self.db_manager.get_crawled_count()
            logger.info(f"DB 저장       : {db_count:,}개")
            pool_stats = self.db_manager.get_pool_stats()
            logger.info(f"DB 풀 (최대)  : {pool_stats.get('pool_max', 0)}")

        logger.info("="*60)

async def main():
    """메인 함수"""
    print("멀티스레딩 크롤러 로그는 multithreaded_crawler.log 파일에 기록됩니다.")
    print("실시간 로그를 보려면 'tail -f multithreaded_crawler.log' 명령을 사용하세요.")
    import argparse

    parser = argparse.ArgumentParser(description='Multithreaded Enterprise Crawler')
    parser.add_argument('--count', type=int, default=1000,
                       help='크롤링할 URL 개수 (기본: 1000)')
    parser.add_argument('--batch-size', type=int, default=BATCH_SIZE,
                       help=f'워커별 배치 크기 (기본: {BATCH_SIZE})')
    parser.add_argument('--workers', type=int, default=WORKER_THREADS,
                       help=f'워커 스레드 수 (기본: {WORKER_THREADS})')

    args = parser.parse_args()

    # 크롤러 생성
    crawler = MultithreadedCrawler(
        initial_url_count=args.count,
        worker_threads=args.workers
    )
    crawler.batch_size = args.batch_size

    try:
        # 초기화
        if not await crawler.initialize():
            logger.error("크롤러 초기화 실패")
            sys.exit(1)

        # URL 데이터셋 준비
        if not await crawler.prepare_url_dataset():
            logger.error("URL 데이터셋 준비 실패")
            sys.exit(1)

        # 연속 크롤링 실행
        await crawler.run_continuous()

    except KeyboardInterrupt:
        logger.info("사용자에 의한 중단")

    except Exception as e:
        logger.error(f"예상치 못한 오류: {e}")

    finally:
        # 정리
        crawler.print_final_stats()
        await crawler.cleanup()

if __name__ == "__main__":
    asyncio.run(main())
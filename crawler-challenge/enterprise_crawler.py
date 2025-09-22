#!/usr/bin/env python3
"""
Enterprise Crawler - Tranco Top 1M 대규모 정중한 크롤러
"""

import asyncio
import logging
import sys
import signal
from datetime import datetime
from typing import List, Dict, Any, Optional

from tranco_manager import TrancoManager
from redis_queue_manager import RedisQueueManager
from polite_crawler import PoliteCrawler
from database import DatabaseManager
from progress_tracker import ProgressTracker
from monitoring.metrics import MetricsMonitor

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('enterprise_crawler.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class EnterpriseCrawler:
    """엔터프라이즈급 대규모 크롤러"""

    def __init__(self, initial_url_count: int = 1000):
        self.initial_url_count = initial_url_count
        self.should_stop = False
        self.batch_size = 50

        # 컴포넌트들
        self.tranco_manager: Optional[TrancoManager] = None
        self.queue_manager: Optional[RedisQueueManager] = None
        self.polite_crawler: Optional[PoliteCrawler] = None
        self.db_manager: Optional[DatabaseManager] = None
        self.progress_tracker: Optional[ProgressTracker] = None
        self.metrics_monitor: Optional[MetricsMonitor] = None

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
        logger.info(f"종료 신호 수신: {signum}")
        self.should_stop = True

    async def initialize(self) -> bool:
        """모든 컴포넌트 초기화"""
        try:
            logger.info("=== Enterprise Crawler 초기화 시작 ===")

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
            self.db_manager.connect()

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

            # 6. Polite Crawler
            logger.info("6. Polite Crawler 초기화...")
            self.polite_crawler = PoliteCrawler()
            await self.polite_crawler.__aenter__()

            logger.info("✅ 모든 컴포넌트 초기화 완료")
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
                initial_count=self.initial_url_count,
                force_update=False
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

    async def crawl_batch(self) -> bool:
        """단일 배치 크롤링"""
        try:
            # 큐에서 다음 배치 가져오기
            batch = self.queue_manager.get_next_batch(self.batch_size)
            if not batch:
                logger.info("큐에 더 이상 URL이 없습니다")
                return False

            batch_urls = [item['url'] for item in batch]
            logger.info(f"배치 크롤링 시작: {len(batch_urls)}개 URL")

            # 정중한 크롤링 실행
            results = await self.polite_crawler.crawl_batch_politely(batch_urls)

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
                        error_info = {
                            'type': 'crawl_error',
                            'message': result.get('error', 'Unknown error'),
                            'recoverable': 'timeout' in result.get('error', '').lower()
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
            self.total_processed += len(results)
            self.total_successful += successful_count
            self.total_failed += failed_count

            logger.info(f"배치 완료: {successful_count}개 성공, {failed_count}개 실패")
            return True

        except Exception as e:
            logger.error(f"배치 크롤링 실패: {e}")
            return False

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

            # 크롤러 통계
            crawler_summary = self.polite_crawler.get_crawling_summary()

            stats = {
                'total_processed': self.total_processed,
                'success_count': self.total_successful,
                'error_count': self.total_failed,
                'error_rate': self.total_failed / self.total_processed if self.total_processed > 0 else 0,
                'pages_per_minute': round(pages_per_minute, 2),
                'uptime_minutes': uptime_minutes,

                # 큐 상태
                'queue_pending': queue_stats.get('total_pending', 0),
                'queue_completed': queue_stats.get('completed', 0),
                'completion_rate': queue_stats.get('completion_rate', 0),

                # 크롤러 상태
                'crawler_domains': crawler_summary.get('total_domains', 0),
                'crawler_blocked_domains': crawler_summary.get('blocked_domains', 0),
                'crawler_average_delay': crawler_summary.get('average_delay', 0),

                # DB 상태
                'db_pages_count': self.db_manager.get_crawled_count() if self.db_manager else 0
            }

            self.progress_tracker.save_progress(stats)

        except Exception as e:
            logger.error(f"진행 상황 저장 실패: {e}")

    async def run_continuous(self):
        """연속 크롤링 실행"""
        logger.info("=== 연속 크롤링 시작 ===")

        cycle_count = 0
        last_progress_save = datetime.now()

        try:
            while not self.should_stop:
                cycle_count += 1
                logger.info(f"\n--- 사이클 #{cycle_count} ---")

                # 배치 크롤링
                has_more = await self.crawl_batch()

                if not has_more:
                    logger.info("모든 URL 크롤링 완료")
                    break

                # 재시도 배치 처리
                retry_batch = self.queue_manager.get_retry_batch(20)
                if retry_batch:
                    logger.info(f"재시도 배치 처리: {len(retry_batch)}개")
                    retry_urls = [item['url'] for item in retry_batch]
                    retry_results = await self.polite_crawler.crawl_batch_politely(retry_urls)

                    # 재시도 결과 처리
                    for result in retry_results:
                        if result['success'] and result.get('content'):
                            self.db_manager.add_to_batch(result['url'], result['content'])
                            self.queue_manager.mark_completed(result['url'], success=True)
                            self.total_successful += 1
                        else:
                            self.total_failed += 1

                    self.total_processed += len(retry_results)

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
            # 최종 진행 상황 저장
            self.save_progress()

    async def cleanup(self):
        """리소스 정리"""
        try:
            logger.info("=== 리소스 정리 시작 ===")

            if self.polite_crawler:
                await self.polite_crawler.__aexit__(None, None, None)

            if self.db_manager:
                self.db_manager.disconnect()

            if self.metrics_monitor:
                await self.metrics_monitor.stop_monitoring()

            if self.progress_tracker:
                self.progress_tracker.mark_session_completed()

            logger.info("✅ 리소스 정리 완료")

        except Exception as e:
            logger.error(f"리소스 정리 오류: {e}")

    def print_final_stats(self):
        """최종 통계 출력"""
        duration = datetime.now() - self.start_time
        duration_minutes = duration.total_seconds() / 60

        logger.info("\n" + "="*60)
        logger.info("               최종 크롤링 통계")
        logger.info("="*60)
        logger.info(f"실행 시간      : {duration_minutes:.1f}분")
        logger.info(f"총 처리        : {self.total_processed:,}개")
        logger.info(f"성공          : {self.total_successful:,}개")
        logger.info(f"실패          : {self.total_failed:,}개")

        if self.total_processed > 0:
            success_rate = self.total_successful / self.total_processed * 100
            pages_per_minute = self.total_processed / duration_minutes if duration_minutes > 0 else 0

            logger.info(f"성공률        : {success_rate:.1f}%")
            logger.info(f"처리속도      : {pages_per_minute:.1f} pages/분")

        # 도메인 통계
        if self.polite_crawler:
            crawler_summary = self.polite_crawler.get_crawling_summary()
            logger.info(f"처리 도메인    : {crawler_summary.get('total_domains', 0)}개")
            logger.info(f"차단 도메인    : {crawler_summary.get('blocked_domains', 0)}개")
            logger.info(f"평균 딜레이    : {crawler_summary.get('average_delay', 0):.1f}초")

        # DB 통계
        if self.db_manager:
            db_count = self.db_manager.get_crawled_count()
            logger.info(f"DB 저장       : {db_count:,}개")

        logger.info("="*60)

async def main():
    """메인 함수"""
    import argparse

    parser = argparse.ArgumentParser(description='Enterprise Crawler - Tranco Top 1M')
    parser.add_argument('--count', type=int, default=1000,
                       help='크롤링할 URL 개수 (기본: 1000)')
    parser.add_argument('--batch-size', type=int, default=50,
                       help='배치 크기 (기본: 50)')

    args = parser.parse_args()

    # 크롤러 생성
    crawler = EnterpriseCrawler(initial_url_count=args.count)
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
        await crawler.cleanup()
        crawler.print_final_stats()

if __name__ == "__main__":
    asyncio.run(main())
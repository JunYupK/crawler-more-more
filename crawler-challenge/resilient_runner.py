import asyncio
import time
import gc
import sys
import signal
import traceback
from typing import Optional, Dict, Any
from datetime import datetime
import logging
import psutil
import os

from core.crawler import WebCrawler
from core.tranco import get_tranco_urls
from monitoring.metrics import MetricsMonitor
from database import DatabaseManager
from progress_tracker import ProgressTracker

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('crawler_resilient.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class ResilientCrawlerRunner:
    """24시간 안정 실행을 위한 복원력 있는 크롤러 러너"""

    def __init__(self):
        self.should_stop = False
        self.restart_count = 0
        self.max_restarts = 100  # 최대 재시작 횟수
        self.start_time = datetime.now()

        # 컴포넌트들
        self.progress_tracker: Optional[ProgressTracker] = None
        self.metrics_monitor: Optional[MetricsMonitor] = None
        self.db_manager: Optional[DatabaseManager] = None

        # 통계
        self.total_processed = 0
        self.total_errors = 0
        self.last_memory_cleanup = time.time()
        self.last_progress_save = time.time()

        # 신호 핸들러 설정
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """종료 신호 처리"""
        logger.info(f"종료 신호 수신: {signum}")
        self.should_stop = True

    async def initialize_components(self) -> bool:
        """모든 컴포넌트 초기화"""
        try:
            logger.info("컴포넌트 초기화 시작...")

            # Progress Tracker 초기화
            self.progress_tracker = ProgressTracker()
            if not self.progress_tracker.test_connection():
                logger.error("Redis 연결 실패")
                return False

            # Metrics Monitor 초기화
            self.metrics_monitor = MetricsMonitor()
            await self.metrics_monitor.start_monitoring()

            # Database Manager 초기화
            self.db_manager = DatabaseManager()
            self.db_manager.connect()

            logger.info("모든 컴포넌트 초기화 완료")
            return True

        except Exception as e:
            logger.error(f"컴포넌트 초기화 실패: {e}")
            logger.error(traceback.format_exc())
            return False

    async def cleanup_components(self):
        """컴포넌트 정리"""
        try:
            if self.metrics_monitor:
                await self.metrics_monitor.stop_monitoring()

            if self.db_manager:
                self.db_manager.disconnect()

            if self.progress_tracker:
                self.progress_tracker.mark_session_completed()

            logger.info("컴포넌트 정리 완료")

        except Exception as e:
            logger.error(f"컴포넌트 정리 중 오류: {e}")

    def get_memory_info(self) -> Dict[str, Any]:
        """메모리 사용량 정보 수집"""
        try:
            process = psutil.Process()
            memory_info = process.memory_info()
            system_memory = psutil.virtual_memory()

            return {
                'process_memory_mb': round(memory_info.rss / 1024 / 1024, 1),
                'process_memory_percent': round(process.memory_percent(), 1),
                'system_memory_total_gb': round(system_memory.total / 1024**3, 1),
                'system_memory_used_gb': round(system_memory.used / 1024**3, 1),
                'system_memory_percent': round(system_memory.percent, 1),
                'memory_percent': round(system_memory.percent, 1)  # progress_tracker용
            }
        except Exception as e:
            logger.error(f"메모리 정보 수집 실패: {e}")
            return {'memory_percent': 0}

    def should_cleanup_memory(self) -> bool:
        """메모리 정리가 필요한지 판단"""
        current_time = time.time()

        # 10분마다 메모리 정리
        if current_time - self.last_memory_cleanup > 600:
            return True

        # 메모리 사용량이 80% 초과시
        memory_info = self.get_memory_info()
        if memory_info.get('memory_percent', 0) > 80:
            return True

        return False

    def cleanup_memory(self):
        """메모리 정리"""
        try:
            logger.info("메모리 정리 시작...")

            # 가비지 컬렉션 강제 실행
            collected = gc.collect()
            logger.info(f"가비지 컬렉션: {collected}개 객체 정리")

            # 메모리 정보 업데이트
            memory_info = self.get_memory_info()
            logger.info(f"메모리 사용량: {memory_info['process_memory_mb']}MB ({memory_info['memory_percent']}%)")

            # 진행 상황에 메모리 통계 저장
            if self.progress_tracker:
                self.progress_tracker.save_memory_stats(memory_info)

            self.last_memory_cleanup = time.time()

        except Exception as e:
            logger.error(f"메모리 정리 실패: {e}")

    def classify_error(self, error: Exception) -> Dict[str, Any]:
        """에러 분류 및 정보 수집"""
        error_type = type(error).__name__
        error_message = str(error)

        # 에러 분류
        if "timeout" in error_message.lower():
            category = "timeout"
            recoverable = True
        elif "connection" in error_message.lower():
            category = "connection"
            recoverable = True
        elif "memory" in error_message.lower():
            category = "memory"
            recoverable = True
        elif "disk" in error_message.lower():
            category = "disk"
            recoverable = False
        else:
            category = "other"
            recoverable = True

        return {
            'type': error_type,
            'category': category,
            'message': error_message[:200],  # 메시지 길이 제한
            'recoverable': recoverable,
            'traceback': traceback.format_exc()[:1000]  # 스택 트레이스 제한
        }

    async def get_urls_to_crawl(self) -> list:
        """크롤링할 URL 목록 획득"""
        try:
            urls = await get_tranco_urls()
            logger.info(f"Tranco에서 {len(urls)}개 URL 로드")
            return urls
        except Exception as e:
            logger.warning(f"Tranco URL 로드 실패: {e}, 테스트 URL 사용")
            # 테스트용 URL (더 많은 실제 사이트)
            test_urls = [
                "https://jsonplaceholder.typicode.com/posts/1",
                "https://jsonplaceholder.typicode.com/posts/2",
                "https://jsonplaceholder.typicode.com/users/1",
                "https://jsonplaceholder.typicode.com/users/2",
                "https://jsonplaceholder.typicode.com/albums/1",
                "https://httpbin.org/json",
                "https://httpbin.org/html",
                "https://httpbin.org/xml",
                "https://httpbin.org/delay/1",
                "https://httpbin.org/status/200"
            ]
            return test_urls * 50  # 500개 URL로 확장

    async def crawl_batch(self, urls: list) -> Dict[str, Any]:
        """URL 배치 크롤링"""
        try:
            async with WebCrawler(self.metrics_monitor, self.db_manager) as crawler:
                results = await crawler.crawl_urls(urls)

                # 결과 통계
                successful = sum(1 for r in results if r['success'])
                failed = len(results) - successful

                self.total_processed += len(results)
                self.total_errors += failed

                return {
                    'processed': len(results),
                    'successful': successful,
                    'failed': failed,
                    'success_rate': successful / len(results) if results else 0
                }

        except Exception as e:
            logger.error(f"배치 크롤링 실패: {e}")
            error_info = self.classify_error(e)
            if self.progress_tracker:
                self.progress_tracker.save_error(error_info)
            raise

    def save_progress_stats(self):
        """진행 상황 통계 저장"""
        try:
            if not self.progress_tracker:
                return

            current_time = time.time()
            if current_time - self.last_progress_save < 60:  # 1분마다만 저장
                return

            uptime_minutes = int((datetime.now() - self.start_time).total_seconds() / 60)
            pages_per_minute = self.total_processed / uptime_minutes if uptime_minutes > 0 else 0

            stats = {
                'total_processed': self.total_processed,
                'success_count': self.total_processed - self.total_errors,
                'error_count': self.total_errors,
                'error_rate': self.total_errors / self.total_processed if self.total_processed > 0 else 0,
                'restart_count': self.restart_count,
                'pages_per_minute': round(pages_per_minute, 2),
                'uptime_minutes': uptime_minutes
            }

            self.progress_tracker.save_progress(stats)
            self.last_progress_save = current_time

        except Exception as e:
            logger.error(f"진행 상황 저장 실패: {e}")

    async def run_single_cycle(self) -> bool:
        """단일 실행 사이클"""
        try:
            # URL 목록 획득
            urls = await self.get_urls_to_crawl()

            # 배치 크롤링 (50개씩)
            batch_size = 50
            for i in range(0, len(urls), batch_size):
                if self.should_stop:
                    logger.info("중단 신호로 인한 사이클 종료")
                    return False

                batch = urls[i:i + batch_size]
                logger.info(f"배치 크롤링 시작: {i+1}-{min(i+batch_size, len(urls))}/{len(urls)}")

                batch_stats = await self.crawl_batch(batch)
                logger.info(f"배치 완료: {batch_stats['successful']}/{batch_stats['processed']} 성공")

                # 메모리 정리 필요 시
                if self.should_cleanup_memory():
                    self.cleanup_memory()

                # 진행 상황 저장
                self.save_progress_stats()

                # 짧은 대기 (과부하 방지)
                await asyncio.sleep(1)

            return True

        except Exception as e:
            logger.error(f"실행 사이클 오류: {e}")
            error_info = self.classify_error(e)

            if self.progress_tracker:
                self.progress_tracker.save_error(error_info)

            # 복구 불가능한 에러인 경우
            if not error_info['recoverable']:
                logger.error("복구 불가능한 에러 발생, 종료")
                return False

            # 복구 가능한 에러인 경우 재시도
            logger.info(f"복구 가능한 에러, 5초 후 재시도...")
            await asyncio.sleep(5)
            return True

    async def run_continuous(self):
        """연속 실행"""
        logger.info("24시간 연속 크롤러 시작")

        while not self.should_stop and self.restart_count < self.max_restarts:
            try:
                # 컴포넌트 초기화
                if not await self.initialize_components():
                    logger.error("컴포넌트 초기화 실패, 재시작...")
                    self.restart_count += 1
                    await asyncio.sleep(30)
                    continue

                # 메인 실행 루프
                while not self.should_stop:
                    success = await self.run_single_cycle()
                    if not success:
                        break

                    # 재시작 필요성 체크
                    if self.progress_tracker and self.progress_tracker.should_restart():
                        logger.info("자동 재시작 조건 충족, 재시작...")
                        break

                    # 사이클 간 대기
                    await asyncio.sleep(10)

                # 정상 종료
                await self.cleanup_components()

                if not self.should_stop:
                    # 자동 재시작
                    self.restart_count += 1
                    logger.info(f"재시작 #{self.restart_count}, 30초 대기...")
                    await asyncio.sleep(30)

            except Exception as e:
                logger.error(f"치명적 오류: {e}")
                logger.error(traceback.format_exc())

                await self.cleanup_components()

                self.restart_count += 1
                if self.restart_count < self.max_restarts:
                    logger.info(f"치명적 오류 후 재시작 #{self.restart_count}, 60초 대기...")
                    await asyncio.sleep(60)

        # 최종 정리
        logger.info(f"크롤러 종료 (재시작 횟수: {self.restart_count})")
        await self.cleanup_components()

async def main():
    """메인 실행 함수"""
    runner = ResilientCrawlerRunner()
    await runner.run_continuous()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("사용자에 의한 중단")
    except Exception as e:
        logger.error(f"예상치 못한 오류: {e}")
        logger.error(traceback.format_exc())
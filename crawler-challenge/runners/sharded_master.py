#!/usr/bin/env python3
"""
Sharded Distributed Crawler - Master Node
"""
import sys
import os
import asyncio
import logging
import signal
import argparse
import subprocess
import time
from datetime import datetime
from typing import Optional
from src.monitoring.metrics import MetricsManager
# Add src to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.managers.tranco_manager import TrancoManager
from src.managers.sharded_queue_manager import ShardedRedisQueueManager
from src.managers.progress_tracker import ProgressTracker

# 로깅 설정
def setup_logging():
    log_dir = os.path.join(os.path.dirname(__file__), '../logs')
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, 'sharded_crawler_master.log')
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )

class ShardedCrawlerMaster:
    """샤딩된 분산 크롤러 마스터 노드"""
    def __init__(self, worker_count: int = 4):
        self.worker_count = worker_count
        self.should_stop = False

        # 샤딩된 Redis 연결 설정 (3개 DB 샤드)
        redis_host = os.getenv('REDIS_HOST', 'localhost')
        shard_configs = [
            {'host': redis_host, 'port': 6379, 'db': 1},
            {'host': redis_host, 'port': 6379, 'db': 2},
            {'host': redis_host, 'port': 6379, 'db': 3}
        ]
        self.queue_manager = ShardedRedisQueueManager(shard_configs)

        # 컴포넌트들
        self.tranco_manager: Optional[TrancoManager] = None
        self.progress_tracker: Optional[ProgressTracker] = None
        
        # [Metric] 2. MetricsManager 초기화 (포트 8000, Master 역할)
        self.metrics = MetricsManager(port=8000, role='master')
        # 통계
        self.start_time = datetime.now()

        # 신호 핸들러
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)


    def _signal_handler(self, signum, frame):
        """종료 신호 처리"""
        logging.info(f"샤딩 마스터: 종료 신호 수신: {signum}")
        self.should_stop = True

    async def initialize(self) -> bool:
        """마스터 초기화"""
        try:
            logging.info("=== 샤딩된 분산 크롤러 마스터 초기화 시작 ===")
            # [Metric] 3. 메트릭 서버 시작
            self.metrics.start_server()

            # 1. Tranco Manager
            logging.info("1. Tranco Manager 초기화...")
            self.tranco_manager = TrancoManager()

            # 2. Sharded Redis Queue Manager
            logging.info("2. Sharded Redis Queue Manager 초기화...")
            if not self.queue_manager.test_connection():
                logging.error("Redis 샤드 연결 실패")
                return False

            # 3. Progress Tracker
            logging.info("3. Progress Tracker 초기화...")
            self.progress_tracker = ProgressTracker()
            if not self.progress_tracker.test_connection():
                logging.error("Progress Tracker Redis 연결 실패")
                return False

            logging.info(f"[OK] 샤딩 마스터 초기화 완료 (워커 {self.worker_count}개, 샤드 {self.queue_manager.num_shards}개)")
            return True

        except Exception as e:
            logging.error(f"샤딩 마스터 초기화 실패: {e}")
            return False

    async def prepare_work(self, url_count: int = 400) -> bool:
        """작업 준비 및 샤딩된 큐에 배포"""
        try:
            logging.info(f"=== 샤딩된 작업 준비 ({url_count}개 URL) ===")

            # URL 데이터셋 준비
            urls = await self.tranco_manager.prepare_url_dataset(initial_count=url_count)
            if not urls:
                logging.error("URL 데이터셋 준비 실패")
                return False

            # 샤딩된 Redis 큐에 로드
            if not self.queue_manager.initialize_queues(urls):
                logging.error("샤딩된 큐 초기화 실패")
                return False
            # 통계 출력
            queue_stats = self.queue_manager.get_queue_stats()
            # [Metric] 4. 초기 상태 반영
            self.metrics.update_queue_stats(queue_stats)

            # [Metric] 크롤링 시작 시간 기록
            self.metrics.session_start_timestamp.set(time.time())
            logging.info(f"✅ 크롤링 세션 시작 시간 기록: {datetime.now().isoformat()}")

            # 통계 출력
            queue_stats = self.queue_manager.get_queue_stats()
            logging.info(f"샤딩된 큐 로딩 완료:")
            logging.info(f"  - 총 URL: {queue_stats.get('total_urls', 0)}개")
            logging.info(f"  - 샤드 수: {queue_stats.get('num_shards', 0)}개")
            logging.info(f"  - 고우선순위: {queue_stats.get('queue_priority_high', 0)}")
            logging.info(f"  - 중우선순위: {queue_stats.get('queue_priority_medium', 0)}")
            logging.info(f"  - 일반우선순위: {queue_stats.get('queue_priority_normal', 0)}")
            logging.info(f"  - 저우선순위: {queue_stats.get('queue_priority_low', 0)}")

            # 샤드별 분산 현황
            load_balance = self.queue_manager.get_shard_load_balance()
            logging.info(f"샤드 로드 분산:")
            for shard_id, load in load_balance.items():
                logging.info(f"  - 샤드 {shard_id}: {load}개 작업")

            return True

        except Exception as e:
            logging.error(f"샤딩된 작업 준비 실패: {e}")
            return False

    async def monitor_workers(self):
        """워커들 모니터링 및 동적 리밸런싱"""
        logging.info("=== 샤딩된 워커 모니터링 시작 ===")

        last_report = datetime.now()
        last_rebalance = datetime.now()

        while not self.should_stop:
            try:
                # 큐 상태 확인
                queue_stats = self.queue_manager.get_queue_stats()

                # [Metric] 5. 실시간 통계 업데이트 (중요!)
                self.metrics.update_queue_stats(queue_stats)

                # 5분마다 리포트
                if (datetime.now() - last_report).total_seconds() > 300:
                    logging.info("📊 샤딩 마스터 리포트:")
                    logging.info(f"  - 완료: {queue_stats.get('completed', 0)}개")
                    logging.info(f"  - 대기: {queue_stats.get('total_pending', 0)}개")
                    logging.info(f"  - 처리 중: {queue_stats.get('processing', 0)}개")
                    logging.info(f"  - 실패: {queue_stats.get('failed', 0)}개")
                    logging.info(f"  - 완료율: {queue_stats.get('completion_rate', 0):.1%}")
                    

                    # 샤드별 상세 정보
                    for shard in queue_stats.get('shard_details', []):
                        shard_id = shard['shard_id']
                        pending = shard['total_pending']
                        completed = shard['completed']
                        logging.info(f"  - 샤드 {shard_id}: 대기 {pending}, 완료 {completed}")

                    last_report = datetime.now()

                # 10분마다 샤드 리밸런싱
                if (datetime.now() - last_rebalance).total_seconds() > 600:
                    logging.info("🔄 샤드 리밸런싱 확인...")
                    self.queue_manager.rebalance_shards()
                    last_rebalance = datetime.now()

                # 모든 작업 완료 확인
                if (queue_stats.get('total_pending', 0) == 0 and
                    queue_stats.get('processing', 0) == 0 and
                    queue_stats.get('completed', 0) > 0):
                    logging.info("✅ 모든 샤딩된 작업 완료")

                    # [Metric] 크롤링 종료 시간 기록
                    self.metrics.session_end_timestamp.set(time.time())
                    logging.info(f"✅ 크롤링 세션 종료 시간 기록: {datetime.now().isoformat()}")

                    # Prometheus scrape 반영을 위한 대기 (30초)
                    logging.info("⏳ Prometheus 메트릭 반영 대기 중 (30초)...")
                    await asyncio.sleep(30)

                    # 자동 리포트 생성
                    await self.generate_completion_report()

                    # 대기 모드 진입
                    logging.info("=" * 60)
                    logging.info("🎉 크롤링 완료 및 보고서 생성 완료")
                    logging.info("📊 메트릭 서버가 계속 실행 중입니다 (port 8000)")
                    logging.info("💡 윈도우에서 추가 보고서를 생성할 수 있습니다:")
                    logging.info("   python scripts/generate_ai_report.py")
                    logging.info("⏹️  종료하려면 Ctrl+C를 누르세요")
                    logging.info("=" * 60)

                    # 대기 모드 (SIGINT/SIGTERM 수신 시까지)
                    while not self.should_stop:
                        await asyncio.sleep(10)

                    logging.info("👋 마스터 종료 중...")
                    break

                await asyncio.sleep(30)  # 30초마다 확인

            except Exception as e:
                logging.error(f"샤딩 모니터링 오류: {e}")
                self.metrics.inc_error(error_type='monitor_loop_error')
                await asyncio.sleep(30)

    async def run_tests_and_generate_report(self):
        """크롤링 완료 후 테스트 실행 및 테스트 리포트 생성"""
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

        try:
            logging.info("🧪 테스트 실행 및 리포트 생성 시작...")

            # pytest 실행하여 test_report.json 생성
            process = await asyncio.create_subprocess_exec(
                sys.executable, '-m', 'pytest',
                '--json-report',
                '--json-report-file=test_report.json',
                '--json-report-indent=2',
                '-v',
                cwd=project_root,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )

            stdout, stderr = await process.communicate()

            if process.returncode == 0:
                logging.info("✅ 테스트 실행 완료 (모두 통과)")
            else:
                # 테스트 실패해도 리포트는 생성됨
                logging.warning(f"⚠️ 일부 테스트 실패 (종료 코드: {process.returncode})")

            # test_report.json이 생성되었는지 확인
            test_report_path = os.path.join(project_root, 'test_report.json')
            if os.path.exists(test_report_path):
                logging.info(f"📄 테스트 리포트 생성됨: {test_report_path}")
            else:
                logging.warning("테스트 리포트 파일이 생성되지 않았습니다")

        except Exception as e:
            logging.error(f"테스트 실행 중 오류: {e}")
            # 테스트 실패해도 AI 리포트 생성은 계속 진행

    async def generate_completion_report(self):
        """크롤링 완료 후 테스트 실행 및 AI 리포트 자동 생성"""
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

        # 1단계: 테스트 실행 및 test_report.json 생성
        await self.run_tests_and_generate_report()

        # 2단계: AI 리포트 생성
        try:
            logging.info("📊 AI 리포트 생성 시작...")

            report_script = os.path.join(project_root, 'scripts', 'generate_ai_report.py')

            if not os.path.exists(report_script):
                logging.warning(f"리포트 스크립트를 찾을 수 없습니다: {report_script}")
                return

            # 비동기로 리포트 생성 스크립트 실행
            process = await asyncio.create_subprocess_exec(
                sys.executable, report_script,
                cwd=project_root,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )

            stdout, stderr = await process.communicate()

            if process.returncode == 0:
                logging.info("✅ AI 리포트 생성 완료")
                if stdout:
                    logging.info(stdout.decode('utf-8', errors='ignore'))
            else:
                logging.warning(f"AI 리포트 생성 실패 (종료 코드: {process.returncode})")
                if stderr:
                    logging.warning(stderr.decode('utf-8', errors='ignore'))

        except Exception as e:
            logging.error(f"AI 리포트 생성 중 오류: {e}")
            # 리포트 생성 실패해도 크롤링은 성공으로 처리

    async def run(self, url_count: int = 400):
        """샤딩 마스터 실행"""
        try:
            # 초기화
            if not await self.initialize():
                logging.error("샤딩 마스터 초기화 실패")
                return False

            # 작업 준비
            if not await self.prepare_work(url_count):
                logging.error("샤딩된 작업 준비 실패")
                return False

            # 워커 모니터링
            await self.monitor_workers()

            return True

        except Exception as e:
            logging.error(f"샤딩 마스터 실행 오류: {e}")
            return False

async def main():
    parser = argparse.ArgumentParser(description='Sharded Distributed Crawler - Master')
    parser.add_argument('--count', type=int, default=400, help='크롤링할 URL 개수')
    parser.add_argument('--workers', type=int, default=4, help='워커 수')
    
    args = parser.parse_args()

    setup_logging()
    
    print(f"샤딩된 분산 크롤러 마스터 시작 (워커 {args.workers}개, URL {args.count}개)")
    master = ShardedCrawlerMaster(worker_count=args.workers)
    success = await master.run(url_count=args.count)
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    asyncio.run(main())

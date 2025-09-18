#!/usr/bin/env python3
"""
안정성 테스트용 스크립트 - 짧은 시간 동안 실행하여 안정성 확인
"""

import asyncio
import time
import sys
from datetime import datetime
import logging
from resilient_runner import ResilientCrawlerRunner

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('test_resilient.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class TestRunner:
    """테스트용 러너 - 제한된 시간동안만 실행"""

    def __init__(self, duration_minutes=10):
        self.duration_minutes = duration_minutes
        self.start_time = datetime.now()

    async def run_test(self):
        """테스트 실행"""
        logger.info(f"🧪 {self.duration_minutes}분 안정성 테스트 시작")
        logger.info(f"시작 시간: {self.start_time}")

        # 크롤러 러너 생성 및 실행
        runner = ResilientCrawlerRunner()

        try:
            # 컴포넌트 초기화
            if not await runner.initialize_components():
                logger.error("❌ 컴포넌트 초기화 실패")
                return False

            logger.info("✅ 컴포넌트 초기화 성공")

            # 테스트 실행
            cycles_completed = 0
            while True:
                # 시간 체크
                elapsed = (datetime.now() - self.start_time).total_seconds()
                if elapsed > self.duration_minutes * 60:
                    logger.info(f"⏰ 테스트 시간 완료 ({self.duration_minutes}분)")
                    break

                # 한 사이클 실행
                logger.info(f"🔄 사이클 #{cycles_completed + 1} 시작")
                success = await runner.run_single_cycle()

                if not success:
                    logger.warning("❌ 사이클 실패")
                    break

                cycles_completed += 1
                logger.info(f"✅ 사이클 #{cycles_completed} 완료")

                # 짧은 대기
                await asyncio.sleep(5)

                # 진행 상황 출력
                if runner.progress_tracker:
                    stats = runner.progress_tracker.get_dashboard_stats()
                    logger.info(f"📊 진행상황: {stats.get('total_processed', 0)} 처리, "
                              f"{stats.get('error_rate', 0)*100:.1f}% 에러율")

            # 최종 통계
            logger.info(f"🏁 테스트 완료!")
            logger.info(f"실행 시간: {elapsed/60:.1f}분")
            logger.info(f"완료 사이클: {cycles_completed}")
            logger.info(f"총 처리: {runner.total_processed}")
            logger.info(f"총 에러: {runner.total_errors}")
            logger.info(f"재시작 횟수: {runner.restart_count}")

            if runner.total_processed > 0:
                error_rate = runner.total_errors / runner.total_processed
                logger.info(f"에러율: {error_rate*100:.1f}%")

            return True

        except Exception as e:
            logger.error(f"❌ 테스트 중 오류: {e}")
            return False

        finally:
            # 정리
            await runner.cleanup_components()
            logger.info("🧹 리소스 정리 완료")

async def main():
    """메인 함수"""
    import sys

    duration = 10  # 기본 10분
    if len(sys.argv) > 1:
        try:
            duration = int(sys.argv[1])
        except ValueError:
            print("사용법: python test_resilient.py [지속시간_분]")
            sys.exit(1)

    tester = TestRunner(duration)
    success = await tester.run_test()

    if success:
        logger.info("✅ 안정성 테스트 성공!")
        sys.exit(0)
    else:
        logger.error("❌ 안정성 테스트 실패!")
        sys.exit(1)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("🛑 사용자에 의한 테스트 중단")
    except Exception as e:
        logger.error(f"❌ 예상치 못한 오류: {e}")
        sys.exit(1)
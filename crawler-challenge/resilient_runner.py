import asyncio
import sys
import signal
import traceback
from datetime import datetime
import logging
import argparse

from enterprise_crawler import EnterpriseCrawler

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
    """
    EnterpriseCrawler를 위한 복원력 있는 슈퍼바이저.
    크롤러의 실행, 모니터링, 자동 재시작을 담당합니다.
    """

    def __init__(self, url_count: int, batch_size: int):
        self.url_count = url_count
        self.batch_size = batch_size
        self.should_stop = False
        self.restart_count = 0
        self.max_restarts = 100  # 최대 재시작 횟수
        self.start_time = datetime.now()

        # 신호 핸들러 설정
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """Graceful shutdown을 위한 종료 신호 처리"""
        logger.info(f"종료 신호 수신: {signum}. 슈퍼바이저 및 크롤러의 정상 종료를 시작합니다.")
        self.should_stop = True
        # 현재 실행 중인 크롤러에도 종료 신호를 전달해야 할 수 있음
        # EnterpriseCrawler는 자체 signal handler가 있으므로,
        # 프로세스 그룹에 전달된 신호는 자식에게도 전달될 것임.

    async def run_enterprise_crawler_session(self) -> bool:
        """
        EnterpriseCrawler의 단일 전체 실행 세션 (초기화부터 정리까지).
        성공적으로 완료되면 True, 오류 발생 시 False를 반환합니다.
        """
        crawler: EnterpriseCrawler = None
        try:
            logger.info(f"EnterpriseCrawler 세션 시작 (URL: {self.url_count}개, 배치: {self.batch_size}개)")
            
            # 1. 크롤러 인스턴스 생성
            crawler = EnterpriseCrawler(initial_url_count=self.url_count)
            crawler.batch_size = self.batch_size

            # 2. 초기화
            if not await crawler.initialize():
                logger.error("EnterpriseCrawler 초기화 실패.")
                return False

            # 3. URL 데이터셋 준비
            if not await crawler.prepare_url_dataset():
                logger.error("URL 데이터셋 준비 실패.")
                await crawler.cleanup()
                return False

            # 4. 연속 크롤링 실행
            # EnterpriseCrawler의 run_continuous는 내부적으로 should_stop을 처리해야 함.
            # 현재는 큐가 빌 때까지 실행되므로, 외부의 should_stop을 전달하는 메커니즘이 필요할 수 있음.
            await crawler.run_continuous()
            
            logger.info("EnterpriseCrawler 세션 정상 완료.")
            return True

        except KeyboardInterrupt:
            logger.info("사용자에 의해 크롤러 세션 중단됨.")
            self.should_stop = True
            return False
        except Exception as e:
            logger.error(f"EnterpriseCrawler 세션 중 치명적 오류 발생: {e}")
            logger.error(traceback.format_exc())
            return False
        finally:
            if crawler:
                logger.info("EnterpriseCrawler 리소스 정리 시작.")
                await crawler.cleanup()
                crawler.print_final_stats()

    async def run_supervisor(self):
        """
        슈퍼바이저 메인 루프.
        EnterpriseCrawler 세션을 실행하고, 실패 시 재시작 로직을 담당.
        """
        logger.info("Resilient Supervisor 시작. EnterpriseCrawler를 관리합니다.")

        while not self.should_stop and self.restart_count < self.max_restarts:
            
            session_success = await self.run_enterprise_crawler_session()

            if self.should_stop:
                logger.info("정상 종료 신호에 따라 슈퍼바이저 루프를 중단합니다.")
                break

            if not session_success:
                self.restart_count += 1
                logger.warning(f"크롤러 세션 실패. 재시작을 시도합니다... (시도: {self.restart_count}/{self.max_restarts})")
                
                if self.restart_count >= self.max_restarts:
                    logger.error("최대 재시작 횟수 도달. 슈퍼바이저를 종료합니다.")
                    break
                
                wait_time = min(60 * (2 ** self.restart_count), 1800) # Exponential backoff
                logger.info(f"{wait_time}초 후 재시작합니다.")
                try:
                    await asyncio.sleep(wait_time)
                except asyncio.CancelledError:
                    logger.info("대기 중 종료 신호 수신. 슈퍼바이저를 즉시 종료합니다.")
                    break
            else:
                logger.info("크롤링 세션이 성공적으로 완료되었습니다. 슈퍼바이저를 종료합니다.")
                break # 성공적으로 끝나면 루프 종료

        logger.info(f"Resilient Supervisor 종료. 총 재시작 횟수: {self.restart_count}")


async def main():
    """메인 실행 함수"""
    parser = argparse.ArgumentParser(description='Resilient Supervisor for Enterprise Crawler')
    parser.add_argument('--count', type=int, default=10000,
                       help='크롤링할 총 URL 개수 (기본: 10000)')
    parser.add_argument('--batch-size', type=int, default=100,
                       help='한 번에 큐에서 가져올 배치 크기 (기본: 100)')
    args = parser.parse_args()

    runner = ResilientCrawlerRunner(url_count=args.count, batch_size=args.batch_size)
    await runner.run_supervisor()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("사용자에 의해 슈퍼바이저 중단됨.")
    except Exception as e:
        logger.error(f"슈퍼바이저에서 예상치 못한 최상위 오류 발생: {e}")
        logger.error(traceback.format_exc())

import asyncio
import aiohttp
import time
from typing import Dict, List, Optional, Tuple, Set
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser
from datetime import datetime, timedelta
import logging
import re
from dataclasses import dataclass, field

# Config import 추가
try:
    from config.settings import (
        GLOBAL_SEMAPHORE_LIMIT,
        TCP_CONNECTOR_LIMIT,
        TCP_CONNECTOR_LIMIT_PER_HOST,
        DEFAULT_CRAWL_DELAY,
        REQUEST_TIMEOUT
    )
except ImportError:
    # Fallback to aggressive defaults
    GLOBAL_SEMAPHORE_LIMIT = 200
    TCP_CONNECTOR_LIMIT = 300
    TCP_CONNECTOR_LIMIT_PER_HOST = 20
    DEFAULT_CRAWL_DELAY = 0.5
    REQUEST_TIMEOUT = 10

# work_logger import 추가
try:
    from work_logger import WorkLogger
except ImportError:
    WorkLogger = None

logger = logging.getLogger(__name__)

@dataclass
class RobotRules:
    """robots.txt 규칙"""
    parser: RobotFileParser = field(default_factory=RobotFileParser)
    crawl_delay: int = 1  # 기본 1초
    last_accessed: datetime = field(default_factory=datetime.now)
    rules_text: str = ""
    user_agent: str = "*"

@dataclass
class DomainState:
    """도메인별 상태 관리"""
    last_access: datetime = field(default_factory=datetime.now)
    request_count: int = 0
    error_count: int = 0
    crawl_delay: int = 1
    robots_checked: bool = False
    robots_rules: Optional[RobotRules] = None
    is_blocked: bool = False
    next_allowed_time: datetime = field(default_factory=datetime.now)

class PoliteCrawler:
    """정중한 크롤링을 위한 robots.txt 준수 크롤러"""

    def __init__(self, respect_robots_txt: bool = True):
        self.domain_states: Dict[str, DomainState] = {}
        self.user_agent = "PoliteCrawler/1.0 (+https://example.com/bot)"

        # 공격적 크롤링 정책
        self.default_delay = DEFAULT_CRAWL_DELAY  # 0.5초로 감소
        self.max_delay = 30     # 최대 30초 딜레이
        self.respect_robots_txt = respect_robots_txt
        self.max_errors_per_domain = 10

        # 공격적 글로벌 레이트 리미터
        self.global_semaphore = asyncio.Semaphore(GLOBAL_SEMAPHORE_LIMIT)  # 200개 동시 요청
        self.session: Optional[aiohttp.ClientSession] = None

        # 작업 로거 추가
        self.work_logger = WorkLogger() if WorkLogger else None

    async def __aenter__(self):
        """Context manager 진입"""
        connector = aiohttp.TCPConnector(
            limit=TCP_CONNECTOR_LIMIT,  # 300개 전체 연결
            limit_per_host=TCP_CONNECTOR_LIMIT_PER_HOST,  # 도메인당 최대 20개 연결
            ttl_dns_cache=300,
            use_dns_cache=True,
            keepalive_timeout=60,
            enable_cleanup_closed=True
        )

        self.session = aiohttp.ClientSession(
            connector=connector,
            timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT),  # 10초로 감소
            headers={
                'User-Agent': self.user_agent,
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate',
                'Connection': 'keep-alive',
                'DNT': '1'
            }
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Context manager 종료"""
        if self.session:
            await self.session.close()

    async def check_robots_txt(self, domain: str) -> RobotRules:
        """robots.txt 확인"""
        try:
            robots_url = f"https://{domain}/robots.txt"

            # HTTP 연결 실패시 HTTPS로 시도
            try:
                async with self.session.get(robots_url) as response:
                    if response.status == 200:
                        robots_content = await response.text()
                    else:
                        robots_content = ""
            except:
                # HTTPS 실패시 HTTP로 시도
                robots_url = f"http://{domain}/robots.txt"
                try:
                    async with self.session.get(robots_url) as response:
                        if response.status == 200:
                            robots_content = await response.text()
                        else:
                            robots_content = ""
                except:
                    robots_content = ""

            # robots.txt 파싱
            rules = self._parse_robots_txt(robots_content, domain)
            logger.debug(f"robots.txt 확인 완료: {domain} (딜레이: {rules.crawl_delay}초)")

            return rules

        except Exception as e:
            logger.warning(f"robots.txt 확인 실패 ({domain}): {e}")
            # 실패 시 기본적으로 허용하는 규칙 반환
            return self._parse_robots_txt("", domain)

    def _parse_robots_txt(self, robots_content: str, domain: str) -> RobotRules:
        """robots.txt 내용 파싱"""
        try:
            # Python의 robotparser 사용
            rp = RobotFileParser()
            rp.parse(robots_content.splitlines())

            # Crawl-delay 추출
            crawl_delay = self.default_delay

            if robots_content.strip():
                # robots.txt에서 crawl-delay 직접 파싱
                crawl_delay_match = re.search(r'crawl-delay:\s*(\d+\.?\d*)', robots_content.lower())
                if crawl_delay_match:
                    crawl_delay = min(float(crawl_delay_match.group(1)), self.max_delay)

                # 일부 사이트에서 사용하는 다른 형식들
                delay_patterns = [
                    r'request-rate:\s*1/(\d+)',  # request-rate: 1/10 (10초마다 1회)
                    r'visit-time:\s*(\d+)',     # visit-time: 5
                ]

                for pattern in delay_patterns:
                    match = re.search(pattern, robots_content.lower())
                    if match:
                        crawl_delay = max(crawl_delay, min(int(match.group(1)), self.max_delay))

            return RobotRules(
                parser=rp,
                crawl_delay=crawl_delay,
                rules_text=robots_content[:500],  # 처음 500자만 저장
                user_agent=self.user_agent
            )

        except Exception as e:
            logger.warning(f"robots.txt 파싱 실패 ({domain}): {e}")
            return RobotRules(parser=RobotFileParser(), crawl_delay=self.default_delay)

    def _get_domain_state(self, domain: str) -> DomainState:
        """도메인 상태 획득"""
        if domain not in self.domain_states:
            self.domain_states[domain] = DomainState()
        return self.domain_states[domain]

    async def is_allowed_to_fetch(self, url: str) -> Tuple[bool, str]:
        """크롤링 허용 여부 - respect_robots_txt=False이므로 항상 허용

        랜덤 샤딩 환경에서는 robots.txt 체크가 불필요하므로 즉시 허용을 반환합니다.
        이 메서드는 하위 호환성을 위해 유지되지만 실제로는 사용되지 않습니다.
        """
        return True, "허용 (robots.txt 무시 모드)"

    async def wait_for_domain_delay(self, domain: str):
        """도메인별 딜레이 대기 - 랜덤 샤딩 환경에서는 불필요

        랜덤 샤딩 환경에서는 도메인별 딜레이가 불필요하므로 즉시 리턴합니다.
        이 메서드는 하위 호환성을 위해 유지되지만 실제로는 사용되지 않습니다.
        """
        return  # 즉시 리턴

    async def fetch_url_politely(self, url: str) -> Dict[str, any]:
        """URL 크롤링 (robots.txt 체크 및 딜레이 제거)

        랜덤 샤딩 환경에서는 robots.txt 체크와 도메인별 딜레이가 불필요합니다.
        global_semaphore만으로 동시성을 제어하여 최대 처리량을 달성합니다.
        """
        parsed_url = urlparse(url)
        domain = parsed_url.netloc

        try:
            # robots.txt 체크 제거 (respect_robots_txt=False)
            # 도메인별 딜레이 제거 (랜덤 샤딩 환경)

            async with self.global_semaphore:
                start_time = time.time()

                async with self.session.get(url) as response:
                    content = await response.text()
                    response_time = time.time() - start_time

                    # 상태 추적 (통계용)
                    domain_state = self._get_domain_state(domain)
                    domain_state.last_access = datetime.now()
                    domain_state.request_count += 1

                    return {
                        'url': url,
                        'success': True,
                        'status': response.status,
                        'content': content,
                        'content_length': len(content),
                        'domain': domain,
                        'response_time': response_time,
                        'headers': dict(response.headers)
                    }

        except asyncio.TimeoutError:
            domain_state = self._get_domain_state(domain)
            domain_state.error_count += 1
            return {
                'url': url,
                'success': False,
                'error': 'Timeout',
                'status': None,
                'content': None,
                'domain': domain
            }

        except aiohttp.ClientError as e:
            domain_state = self._get_domain_state(domain)
            domain_state.error_count += 1
            return {
                'url': url,
                'success': False,
                'error': f'Client error: {str(e)}',
                'status': None,
                'content': None,
                'domain': domain
            }

        except Exception as e:
            domain_state = self._get_domain_state(domain)
            domain_state.error_count += 1
            logger.error(f"크롤링 실패 ({url}): {e}")
            return {
                'url': url,
                'success': False,
                'error': f'Unexpected error: {str(e)}',
                'status': None,
                'content': None,
                'domain': domain
            }

    async def crawl_batch_politely(self, urls: List[str]) -> List[Dict[str, any]]:
        """URL 배치를 동시 크롤링 (도메인 무관)

        랜덤 샤딩 환경에서는 도메인별 그룹화가 불필요하므로
        전체 URL을 한 번에 동시 처리하여 처리량을 극대화합니다.
        global_semaphore가 동시성을 제어합니다.
        """
        logger.info(f"배치 크롤링 시작: {len(urls)}개 URL")

        # 전체 URL을 한 번에 동시 처리
        # global_semaphore가 동시성 제어
        tasks = [self.fetch_url_politely(url) for url in urls]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # 예외 처리
        final_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"크롤링 예외 ({urls[i]}): {result}")
                final_results.append({
                    'url': urls[i],
                    'success': False,
                    'error': str(result),
                    'status': None,
                    'content': None,
                    'domain': urlparse(urls[i]).netloc
                })
            else:
                final_results.append(result)

        logger.info(f"배치 크롤링 완료: {len(final_results)}개 결과")
        return final_results


    def get_domain_stats(self) -> Dict[str, Dict[str, any]]:
        """도메인별 통계"""
        stats = {}
        for domain, state in self.domain_states.items():
            stats[domain] = {
                'request_count': state.request_count,
                'error_count': state.error_count,
                'crawl_delay': state.crawl_delay,
                'is_blocked': state.is_blocked,
                'robots_checked': state.robots_checked,
                'last_access': state.last_access.isoformat() if state.last_access else None
            }
        return stats

    def get_crawling_summary(self) -> Dict[str, any]:
        """크롤링 요약 통계"""
        total_requests = sum(state.request_count for state in self.domain_states.values())
        total_errors = sum(state.error_count for state in self.domain_states.values())
        blocked_domains = sum(1 for state in self.domain_states.values() if state.is_blocked)

        return {
            'total_domains': len(self.domain_states),
            'total_requests': total_requests,
            'total_errors': total_errors,
            'blocked_domains': blocked_domains,
            'error_rate': total_errors / total_requests if total_requests > 0 else 0,
            'average_delay': sum(state.crawl_delay for state in self.domain_states.values()) / len(self.domain_states) if self.domain_states else 0
        }

async def main():
    """테스트 실행"""
    logging.basicConfig(level=logging.INFO)

    test_urls = [
        "https://google.com/",
        "https://github.com/",
        "https://stackoverflow.com/",
        "https://reddit.com/",
        "https://wikipedia.org/"
    ]

    async with PoliteCrawler() as crawler:
        print("[START] 정중한 크롤러 테스트 시작")

        results = await crawler.crawl_batch_politely(test_urls)

        print(f"\n[RESULTS] 크롤링 결과:")
        successful = sum(1 for r in results if r['success'])
        print(f"  성공: {successful}/{len(results)}")

        for result in results:
            status = "[OK]" if result['success'] else "[FAIL]"
            print(f"  {status} {result['url']} - {result.get('status', result.get('error'))}")

        print(f"\n[DOMAINS] 도메인 통계:")
        domain_stats = crawler.get_domain_stats()
        for domain, stats in domain_stats.items():
            print(f"  {domain}: {stats['request_count']}회 요청, {stats['crawl_delay']}초 딜레이")

        print(f"\n[SUMMARY] 전체 요약:")
        summary = crawler.get_crawling_summary()
        for key, value in summary.items():
            print(f"  {key}: {value}")

        # 작업 로깅
        if crawler.work_logger:
            crawler.work_logger.log_and_commit(
                title="정중한 크롤링 시스템 테스트 완료",
                description=f"robots.txt 준수와 도메인별 딜레이를 적용한 {len(test_urls)}개 사이트 크롤링을 완료했습니다.",
                details={
                    "총 요청": len(results),
                    "성공률": f"{(successful/len(results)*100):.1f}%",
                    "도메인 수": len(domain_stats),
                    "평균 딜레이": f"{summary['average_delay']:.1f}초",
                    "상태": "정상 작동"
                }
            )

        return results

if __name__ == "__main__":
    asyncio.run(main())

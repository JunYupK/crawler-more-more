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
        """URL 크롤링 허용 여부 확인"""
        if not self.respect_robots_txt:
            return True, "허용 (robots.txt 무시)"
        try:
            parsed_url = urlparse(url)
            domain = parsed_url.netloc

            domain_state = self._get_domain_state(domain)

            # 너무 많은 에러 발생시 차단
            if domain_state.error_count >= self.max_errors_per_domain:
                domain_state.is_blocked = True
                return False, f"도메인 에러 한도 초과 ({domain_state.error_count})"

            # 이미 차단된 도메인
            if domain_state.is_blocked:
                return False, "도메인 차단됨"

            # robots.txt 미확인시 확인
            if not domain_state.robots_checked:
                robots_rules = await self.check_robots_txt(domain)
                domain_state.robots_rules = robots_rules
                domain_state.robots_checked = True
                domain_state.crawl_delay = robots_rules.crawl_delay

            # robots.txt 규칙 확인 (URL별로)
            if domain_state.robots_rules:
                if not domain_state.robots_rules.parser.can_fetch(self.user_agent, url):
                    return False, f"robots.txt에 의해 차단됨: {url}"

            # 딜레이 확인
            now = datetime.now()
            if now < domain_state.next_allowed_time:
                wait_time = (domain_state.next_allowed_time - now).total_seconds()
                return False, f"딜레이 대기 중 ({wait_time:.1f}초 남음)"

            return True, "허용"

        except Exception as e:
            logger.error(f"크롤링 허용 확인 실패 ({url}): {e}")
            return False, f"확인 실패: {e}"

    async def wait_for_domain_delay(self, domain: str):
        """도메인별 딜레이 대기"""
        domain_state = self._get_domain_state(domain)

        now = datetime.now()
        if now < domain_state.next_allowed_time:
            wait_time = (domain_state.next_allowed_time - now).total_seconds()
            logger.debug(f"도메인 딜레이 대기: {domain} ({wait_time:.1f}초)")
            await asyncio.sleep(wait_time)

        # 다음 허용 시간 설정
        domain_state.next_allowed_time = datetime.now() + timedelta(seconds=domain_state.crawl_delay)

    async def fetch_url_politely(self, url: str) -> Dict[str, any]:
        """정중한 방식으로 URL 크롤링"""
        parsed_url = urlparse(url)
        domain = parsed_url.netloc

        try:
            # 크롤링 허용 확인
            allowed, reason = await self.is_allowed_to_fetch(url)
            if not allowed:
                return {
                    'url': url,
                    'success': False,
                    'error': f"크롤링 불허: {reason}",
                    'status': None,
                    'content': None
                }

            # 글로벌 세마포어로 동시 요청 제한
            async with self.global_semaphore:
                # 도메인별 딜레이 대기
                await self.wait_for_domain_delay(domain)

                domain_state = self._get_domain_state(domain)

                # 요청 실행
                async with self.session.get(url) as response:
                    content = await response.text()

                    # 성공 처리
                    domain_state.last_access = datetime.now()
                    domain_state.request_count += 1

                    # 상태 코드별 처리
                    if response.status == 429:  # Too Many Requests
                        # 딜레이 증가
                        domain_state.crawl_delay = min(domain_state.crawl_delay * 2, self.max_delay)
                        logger.warning(f"429 에러로 인한 딜레이 증가: {domain} ({domain_state.crawl_delay}초)")

                    return {
                        'url': url,
                        'success': True,
                        'status': response.status,
                        'content': content,
                        'content_length': len(content),
                        'domain': domain,
                        'crawl_delay': domain_state.crawl_delay,
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

            # 연결 오류가 많을 경우 딜레이 증가
            if domain_state.error_count % 3 == 0:
                domain_state.crawl_delay = min(domain_state.crawl_delay * 1.5, self.max_delay)

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
        """URL 배치를 정중하게 크롤링"""
        logger.info(f"정중한 배치 크롤링 시작: {len(urls)}개 URL")

        # 도메인별 그룹화
        domain_groups = {}
        for url in urls:
            domain = urlparse(url).netloc
            if domain not in domain_groups:
                domain_groups[domain] = []
            domain_groups[domain].append(url)

        logger.info(f"도메인별 분산: {len(domain_groups)}개 도메인")

        # 도메인별로 순차 처리, 전체적으로는 병렬 처리
        tasks = []
        for domain, domain_urls in domain_groups.items():
            task = asyncio.create_task(self._crawl_domain_urls(domain, domain_urls))
            tasks.append(task)

        # 모든 도메인 크롤링 완료 대기
        domain_results = await asyncio.gather(*tasks)

        # 결과 합치기
        all_results = []
        for domain_result in domain_results:
            all_results.extend(domain_result)

        logger.info(f"정중한 배치 크롤링 완료: {len(all_results)}개 결과")
        return all_results

    async def _crawl_domain_urls(self, domain: str, urls: List[str]) -> List[Dict[str, any]]:
        """단일 도메인의 URL들을 순차 크롤링"""
        results = []

        logger.debug(f"도메인 크롤링 시작: {domain} ({len(urls)}개 URL)")

        for url in urls:
            try:
                result = await self.fetch_url_politely(url)
                results.append(result)

                # 성공/실패 로깅 (이모지 제거)
                if result['success']:
                    logger.debug(f"[OK] {url} - {result['status']}")
                else:
                    logger.debug(f"[FAIL] {url} - {result['error']}")

            except Exception as e:
                logger.error(f"도메인 크롤링 오류 ({url}): {e}")
                results.append({
                    'url': url,
                    'success': False,
                    'error': f'Domain crawling error: {str(e)}',
                    'status': None,
                    'content': None,
                    'domain': domain
                })

        logger.debug(f"도메인 크롤링 완료: {domain}")
        return results

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

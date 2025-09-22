#!/usr/bin/env python3
"""
엔터프라이즈 크롤러 1,000개 사이트 테스트
- Tranco Top 1M 데이터 활용
- 정중한 크롤링 정책 적용
- Redis 큐 관리
- 실시간 진행 상황 모니터링
"""

import asyncio
import time
from typing import List, Dict
import logging
from datetime import datetime

from test_tranco import create_sample_tranco_data
from tranco_manager import TrancoManager
from polite_crawler import PoliteCrawler
from work_logger import WorkLogger

logger = logging.getLogger(__name__)


class EnterpriseCrawlerTest:
    """엔터프라이즈 크롤러 테스트 시스템"""

    def __init__(self):
        self.tranco_manager = TrancoManager()
        self.work_logger = WorkLogger()
        self.test_results = []

    async def prepare_test_urls(self, count: int = 1000) -> List[Dict]:
        """테스트용 URL 준비"""
        print(f"[PREP] {count}개 테스트 URL 준비 중...")

        # 1. 샘플 Tranco 데이터 생성 (실제 상위 사이트들)
        create_sample_tranco_data()

        # 2. URL 파싱 및 우선순위 할당
        urls = self.tranco_manager.parse_csv_to_urls(limit=count, add_www=False)

        if not urls:
            print("[ERROR] URL 준비 실패")
            return []

        print(f"[OK] {len(urls)}개 URL 준비 완료")

        # 3. 우선순위별 분포 확인
        priority_count = {}
        for url_info in urls:
            priority = url_info['priority']
            priority_count[priority] = priority_count.get(priority, 0) + 1

        print("[STATS] 우선순위별 분포:")
        for priority in sorted(priority_count.keys(), reverse=True):
            print(f"  우선순위 {priority}: {priority_count[priority]}개")

        return urls

    async def run_crawl_test(self, urls: List[Dict], concurrent_limit: int = 25) -> List[Dict]:
        """실제 크롤링 테스트 실행"""
        print(f"[START] {len(urls)}개 URL 크롤링 테스트 시작")
        print(f"  동시 요청 제한: {concurrent_limit}개")
        print(f"  robots.txt 준수: 활성화")
        print(f"  도메인별 딜레이: 활성화")

        start_time = time.time()
        results = []

        async with PoliteCrawler() as crawler:
            # 동시 실행 제한을 위한 세마포어
            semaphore = asyncio.Semaphore(concurrent_limit)

            async def crawl_single_url(url_info: Dict) -> Dict:
                """단일 URL 크롤링"""
                async with semaphore:
                    url = url_info['url']
                    crawl_start = time.time()

                    try:
                        result = await crawler.fetch_url_politely(url)

                        # 결과에 추가 정보 포함
                        result.update({
                            'rank': url_info.get('rank', 999999),
                            'priority': url_info.get('priority', 600),
                            'url_type': url_info.get('url_type', 'unknown'),
                            'crawl_duration': time.time() - crawl_start
                        })

                        return result

                    except Exception as e:
                        return {
                            'url': url,
                            'success': False,
                            'error': str(e),
                            'rank': url_info.get('rank', 999999),
                            'priority': url_info.get('priority', 600),
                            'crawl_duration': time.time() - crawl_start
                        }

            # 진행 상황 모니터링
            async def progress_monitor():
                """진행 상황 모니터링"""
                while len(results) < len(urls):
                    await asyncio.sleep(5)  # 5초마다 체크

                    completed = len(results)
                    if completed > 0:
                        elapsed = time.time() - start_time
                        rate = completed / elapsed
                        eta = (len(urls) - completed) / rate if rate > 0 else 0

                        successful = sum(1 for r in results if r.get('success', False))
                        success_rate = (successful / completed) * 100

                        print(f"[PROGRESS] {completed}/{len(urls)} 완료 "
                              f"({success_rate:.1f}% 성공) "
                              f"- {rate:.2f} req/sec "
                              f"- ETA: {eta/60:.1f}분")

            # 모니터링 태스크 시작
            monitor_task = asyncio.create_task(progress_monitor())

            try:
                # 모든 URL에 대해 크롤링 태스크 생성
                tasks = [crawl_single_url(url_info) for url_info in urls]

                # 배치 단위로 실행 (메모리 관리)
                batch_size = 100
                for i in range(0, len(tasks), batch_size):
                    batch = tasks[i:i + batch_size]
                    batch_results = await asyncio.gather(*batch, return_exceptions=True)

                    for result in batch_results:
                        if isinstance(result, Exception):
                            results.append({
                                'url': 'unknown',
                                'success': False,
                                'error': str(result),
                                'crawl_duration': 0
                            })
                        else:
                            results.append(result)

                    print(f"[BATCH] {min(i + batch_size, len(urls))}/{len(urls)} 배치 완료")

            finally:
                monitor_task.cancel()

        total_time = time.time() - start_time
        print(f"[COMPLETE] 크롤링 완료 - 총 소요시간: {total_time:.1f}초")

        return results

    def analyze_results(self, results: List[Dict]) -> Dict:
        """결과 분석"""
        successful = [r for r in results if r.get('success', False)]
        failed = [r for r in results if not r.get('success', False)]

        # 기본 통계
        stats = {
            'total_requests': len(results),
            'successful_requests': len(successful),
            'failed_requests': len(failed),
            'success_rate': (len(successful) / len(results)) * 100 if results else 0,
            'total_time': sum(r.get('crawl_duration', 0) for r in results),
            'average_response_time': 0,
        }

        if successful:
            stats['average_response_time'] = sum(r.get('crawl_duration', 0) for r in successful) / len(successful)

        # 도메인별 통계
        domain_stats = {}
        for result in results:
            domain = result.get('domain', 'unknown')
            if domain not in domain_stats:
                domain_stats[domain] = {'total': 0, 'success': 0, 'failed': 0}

            domain_stats[domain]['total'] += 1
            if result.get('success', False):
                domain_stats[domain]['success'] += 1
            else:
                domain_stats[domain]['failed'] += 1

        # 상위 성공/실패 도메인
        top_success_domains = sorted(
            domain_stats.items(),
            key=lambda x: x[1]['success'],
            reverse=True
        )[:10]

        top_failed_domains = sorted(
            domain_stats.items(),
            key=lambda x: x[1]['failed'],
            reverse=True
        )[:10]

        # 우선순위별 성공률
        priority_stats = {}
        for result in results:
            priority = result.get('priority', 600)
            if priority not in priority_stats:
                priority_stats[priority] = {'total': 0, 'success': 0}

            priority_stats[priority]['total'] += 1
            if result.get('success', False):
                priority_stats[priority]['success'] += 1

        # 오류 유형별 통계
        error_types = {}
        for result in failed:
            error = result.get('error', 'Unknown')
            error_type = error.split(':')[0] if ':' in error else error
            error_types[error_type] = error_types.get(error_type, 0) + 1

        return {
            'basic_stats': stats,
            'domain_stats': domain_stats,
            'top_success_domains': top_success_domains,
            'top_failed_domains': top_failed_domains,
            'priority_stats': priority_stats,
            'error_types': error_types
        }

    def print_analysis(self, analysis: Dict):
        """분석 결과 출력"""
        stats = analysis['basic_stats']

        print(f"\n[ANALYSIS] 크롤링 결과 분석")
        print(f"=" * 50)

        print(f"📊 기본 통계:")
        print(f"  총 요청: {stats['total_requests']:,}")
        print(f"  성공: {stats['successful_requests']:,}")
        print(f"  실패: {stats['failed_requests']:,}")
        print(f"  성공률: {stats['success_rate']:.1f}%")
        print(f"  평균 응답시간: {stats['average_response_time']:.2f}초")

        print(f"\n🏆 상위 성공 도메인:")
        for domain, domain_stat in analysis['top_success_domains'][:5]:
            success_rate = (domain_stat['success'] / domain_stat['total']) * 100
            print(f"  {domain}: {domain_stat['success']}/{domain_stat['total']} ({success_rate:.1f}%)")

        print(f"\n❌ 상위 실패 도메인:")
        for domain, domain_stat in analysis['top_failed_domains'][:5]:
            if domain_stat['failed'] > 0:
                print(f"  {domain}: {domain_stat['failed']} 실패")

        print(f"\n🎯 우선순위별 성공률:")
        for priority in sorted(analysis['priority_stats'].keys(), reverse=True):
            pstat = analysis['priority_stats'][priority]
            success_rate = (pstat['success'] / pstat['total']) * 100
            print(f"  우선순위 {priority}: {success_rate:.1f}% ({pstat['success']}/{pstat['total']})")

        print(f"\n⚠️ 오류 유형별 통계:")
        for error_type, count in sorted(analysis['error_types'].items(), key=lambda x: x[1], reverse=True)[:5]:
            print(f"  {error_type}: {count}회")

    async def run_full_test(self, url_count: int = 1000):
        """전체 테스트 실행"""
        print(f"[ENTERPRISE] {url_count}개 사이트 크롤링 테스트 시작")

        # 1. URL 준비
        urls = await self.prepare_test_urls(url_count)
        if not urls:
            return False

        # 2. 크롤링 실행
        results = await self.run_crawl_test(urls, concurrent_limit=25)

        # 3. 결과 분석
        analysis = self.analyze_results(results)
        self.print_analysis(analysis)

        # 4. 결과 저장 및 로깅
        self.test_results = results

        success_rate = analysis['basic_stats']['success_rate']
        avg_response_time = analysis['basic_stats']['average_response_time']

        self.work_logger.log_and_commit(
            title=f"엔터프라이즈 크롤러 {url_count}개 사이트 테스트 완료",
            description=f"Tranco 상위 사이트 {url_count}개를 대상으로 정중한 크롤링 테스트를 완료했습니다.",
            details={
                "총 요청": f"{len(results):,}",
                "성공률": f"{success_rate:.1f}%",
                "평균 응답시간": f"{avg_response_time:.2f}초",
                "도메인 수": len(analysis['domain_stats']),
                "robots.txt": "준수",
                "동시 요청": "25개 제한",
                "상태": "테스트 완료"
            }
        )

        print(f"\n[SUCCESS] 엔터프라이즈 크롤링 테스트 완료!")
        print(f"성공률: {success_rate:.1f}%, 평균 응답시간: {avg_response_time:.2f}초")

        return True


async def main():
    """메인 테스트"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    test_system = EnterpriseCrawlerTest()

    try:
        success = await test_system.run_full_test(url_count=1000)
        return success
    except Exception as e:
        print(f"[ERROR] 테스트 실행 중 오류: {e}")
        return False


if __name__ == "__main__":
    success = asyncio.run(main())
    if success:
        print("\n🎉 모든 테스트 완료!")
    else:
        print("\n💥 테스트 실패!")
#!/usr/bin/env python3
"""
공격적 최적화 성능 테스트 - 데이터베이스 없이 순수 크롤링 성능만 측정
"""

import asyncio
import logging
import time
from datetime import datetime
from typing import List
import sys

# Config import
try:
    from config.settings import (
        GLOBAL_SEMAPHORE_LIMIT,
        TCP_CONNECTOR_LIMIT,
        TCP_CONNECTOR_LIMIT_PER_HOST,
        DEFAULT_CRAWL_DELAY,
        REQUEST_TIMEOUT
    )
except ImportError:
    GLOBAL_SEMAPHORE_LIMIT = 200
    TCP_CONNECTOR_LIMIT = 300
    TCP_CONNECTOR_LIMIT_PER_HOST = 20
    DEFAULT_CRAWL_DELAY = 0.5
    REQUEST_TIMEOUT = 10

from polite_crawler import PoliteCrawler
from tranco_manager import TrancoManager

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def run_performance_test(url_count: int):
    """성능 테스트 실행"""
    logger.info("="*80)
    logger.info(f"공격적 최적화 성능 테스트 시작 - {url_count}개 URL")
    logger.info("="*80)

    # 설정 출력
    logger.info("\n현재 최적화 설정:")
    logger.info(f"  - Global Semaphore: {GLOBAL_SEMAPHORE_LIMIT}")
    logger.info(f"  - TCP Connector Limit: {TCP_CONNECTOR_LIMIT}")
    logger.info(f"  - TCP Limit Per Host: {TCP_CONNECTOR_LIMIT_PER_HOST}")
    logger.info(f"  - Default Crawl Delay: {DEFAULT_CRAWL_DELAY}s")
    logger.info(f"  - Request Timeout: {REQUEST_TIMEOUT}s")
    logger.info("")

    # URL 리스트 준비 (로컬 파일 사용)
    logger.info("1. URL 리스트 준비 중...")
    url_file = "data/tranco_top_500_urls.txt"

    try:
        with open(url_file, 'r') as f:
            all_urls = [line.strip() for line in f if line.strip()]

        # 요청된 수만큼만 선택 (최대 500개)
        urls = all_urls[:min(url_count, len(all_urls))]
        logger.info(f"   ✓ {len(urls)}개 URL 준비 완료 (from {url_file})")
    except Exception as e:
        logger.error(f"URL 파일 읽기 실패: {e}")
        return None

    # PoliteCrawler 초기화
    logger.info("2. 공격적 크롤러 초기화 중...")
    async with PoliteCrawler(respect_robots_txt=False) as crawler:
        logger.info("   ✓ 크롤러 초기화 완료")

        # 크롤링 시작
        logger.info(f"\n3. 크롤링 시작 ({len(urls)}개 URL)...")
        start_time = time.time()
        start_datetime = datetime.now()

        results = await crawler.crawl_batch_politely(urls)

        end_time = time.time()
        end_datetime = datetime.now()
        duration = end_time - start_time

        # 통계 계산
        successful = sum(1 for r in results if r['success'])
        failed = len(results) - successful
        success_rate = (successful / len(results)) * 100 if results else 0
        pages_per_second = len(results) / duration if duration > 0 else 0
        pages_per_minute = pages_per_second * 60

        # 결과 출력
        logger.info("\n" + "="*80)
        logger.info("공격적 최적화 성능 테스트 결과")
        logger.info("="*80)
        logger.info(f"\n📊 기본 통계:")
        logger.info(f"  시작 시간         : {start_datetime.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"  종료 시간         : {end_datetime.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"  총 실행 시간      : {duration:.2f}초 ({duration/60:.2f}분)")
        logger.info(f"  총 URL 수         : {len(results)}개")
        logger.info(f"  성공              : {successful}개")
        logger.info(f"  실패              : {failed}개")
        logger.info(f"  성공률            : {success_rate:.1f}%")

        logger.info(f"\n🚀 성능 지표:")
        logger.info(f"  처리 속도         : {pages_per_second:.2f} pages/sec")
        logger.info(f"  처리 속도         : {pages_per_minute:.1f} pages/min")
        logger.info(f"  평균 응답 시간    : {duration/len(results):.3f}초/URL")

        # 크롤러 통계
        crawler_summary = crawler.get_crawling_summary()
        logger.info(f"\n🌐 크롤러 통계:")
        logger.info(f"  처리 도메인 수    : {crawler_summary.get('total_domains', 0)}개")
        logger.info(f"  총 요청 수        : {crawler_summary.get('total_requests', 0)}개")
        logger.info(f"  총 에러 수        : {crawler_summary.get('total_errors', 0)}개")
        logger.info(f"  차단된 도메인     : {crawler_summary.get('blocked_domains', 0)}개")
        logger.info(f"  에러율            : {crawler_summary.get('error_rate', 0):.1%}")
        logger.info(f"  평균 딜레이       : {crawler_summary.get('average_delay', 0):.2f}초")

        # 에러 분석
        if failed > 0:
            logger.info(f"\n⚠️  에러 분석:")
            error_types = {}
            for result in results:
                if not result['success']:
                    error = result.get('error', 'Unknown')
                    error_type = error.split(':')[0] if ':' in error else error
                    error_types[error_type] = error_types.get(error_type, 0) + 1

            for error_type, count in sorted(error_types.items(), key=lambda x: x[1], reverse=True):
                logger.info(f"  {error_type}: {count}개 ({count/failed*100:.1f}%)")

        logger.info("\n" + "="*80)

        return {
            'url_count': len(results),
            'successful': successful,
            'failed': failed,
            'success_rate': success_rate,
            'duration_seconds': duration,
            'pages_per_second': pages_per_second,
            'pages_per_minute': pages_per_minute,
            'total_domains': crawler_summary.get('total_domains', 0),
            'settings': {
                'semaphore': GLOBAL_SEMAPHORE_LIMIT,
                'tcp_limit': TCP_CONNECTOR_LIMIT,
                'tcp_per_host': TCP_CONNECTOR_LIMIT_PER_HOST,
                'crawl_delay': DEFAULT_CRAWL_DELAY,
                'timeout': REQUEST_TIMEOUT
            }
        }

async def main():
    """메인 함수"""
    import argparse

    parser = argparse.ArgumentParser(description='Aggressive Performance Test')
    parser.add_argument('--count', type=int, default=100,
                       help='테스트할 URL 개수 (기본: 100)')

    args = parser.parse_args()

    try:
        result = await run_performance_test(args.count)

        if result:
            # 간단한 요약
            print(f"\n✅ 테스트 완료!")
            print(f"   {result['url_count']}개 URL을 {result['duration_seconds']:.1f}초에 처리")
            print(f"   성능: {result['pages_per_second']:.2f} pages/sec")
            print(f"   성공률: {result['success_rate']:.1f}%")

            # JSON 형태로도 출력
            import json
            with open(f'aggressive_test_{args.count}_urls.json', 'w') as f:
                json.dump(result, f, indent=2)
            print(f"\n📝 상세 결과가 aggressive_test_{args.count}_urls.json에 저장되었습니다.")

            return 0
        else:
            return 1

    except Exception as e:
        logger.error(f"테스트 실패: {e}", exc_info=True)
        return 1

if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)

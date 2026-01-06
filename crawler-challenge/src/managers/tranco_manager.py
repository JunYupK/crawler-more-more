import asyncio
import csv
import os
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
import logging
from pathlib import Path
import tranco
import functools

# work_logger import 추가
try:
    from work_logger import WorkLogger
except ImportError:
    WorkLogger = None

logger = logging.getLogger(__name__)

class TrancoManager:
    """Tranco Top 1M 리스트 관리자 (공식 라이브러리 사용)"""

    def __init__(self, data_dir="data"):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(exist_ok=True)
        
        # Tranco 라이브러리 초기화 (캐시 디렉토리 및 서브도메인 포함 여부 지정)
        self.tranco_fetcher = tranco.Tranco(cache_dir=self.data_dir / '.tranco_cache', include_subdomains=True)

        # 파일 경로
        self.processed_file = self.data_dir / "tranco_urls.txt"
        
        # 작업 로거 초기화
        self.work_logger = WorkLogger() if WorkLogger else None

    async def get_latest_list(self, limit: int = 1000000) -> Optional[List[Tuple[int, str]]]:
        """
        Tranco 라이브러리를 사용하여 최신 목록을 가져옵니다.
        오늘 목록이 없으면 어제 목록으로 대체합니다.
        """
        loop = asyncio.get_running_loop()
        try:
            logger.info("Tranco 라이브러리를 통해 최신 목록 가져오는 중...")
            latest_list = await loop.run_in_executor(None, self.tranco_fetcher.list)
            
            top_sites_fetcher = functools.partial(latest_list.top, limit)
            top_sites = await loop.run_in_executor(None, top_sites_fetcher)

            logger.info(f"최신 Tranco 목록 로드 완료: {len(top_sites)}개 사이트")
            return [(i + 1, domain) for i, domain in enumerate(top_sites)]
        except Exception as e:
            if 'unavailable' in str(e):
                logger.warning(f"오늘 Tranco 목록을 사용할 수 없습니다. 어제 목록으로 대체합니다. 오류: {e}")
                try:
                    yesterday = datetime.now().date() - timedelta(days=1)
                    list_fetcher = functools.partial(self.tranco_fetcher.list, date=yesterday)
                    yesterday_list = await loop.run_in_executor(None, list_fetcher)

                    top_sites_fetcher = functools.partial(yesterday_list.top, limit)
                    top_sites = await loop.run_in_executor(None, top_sites_fetcher)

                    logger.info(f"어제 Tranco 목록 로드 완료: {len(top_sites)}개 사이트")
                    return [(i + 1, domain) for i, domain in enumerate(top_sites)]
                except Exception as e2:
                    logger.error(f"어제 Tranco 목록도 가져오지 못했습니다: {e2}")
                    return None
            else:
                logger.error(f"Tranco 라이브러리 사용 중 예상치 못한 오류 발생: {e}")
                return None

    def parse_tranco_list_to_urls(self, tranco_list: List[Tuple[int, str]], limit: Optional[int] = None) -> List[Dict[str, any]]:
        """가져온 Tranco 목록을 기반으로 URL 리스트 생성 (1 도메인 = 1 https URL)"""
        urls = []

        if not tranco_list:
            return []

        logger.info(f"Tranco 목록 파싱 시작 (최대 {limit or len(tranco_list)}개 도메인)")

        for rank, domain in tranco_list:
            if limit and len(urls) >= limit:
                break

            domain = domain.strip()
            # 각 도메인에 대해 단일 https:// URL만 생성
            urls.append({
                'url': f"https://{domain}/",
                'rank': rank,
                'domain': domain,
                'priority': self._calculate_priority(rank),
                'url_type': 'root'
            })

        logger.info(f"URL 생성 완료: {len(urls)}개 (고유 도메인)")
        return urls

    def _calculate_priority(self, rank: int) -> int:
        """순위 기반 우선순위 계산"""
        if rank <= 100: return 1000
        elif rank <= 1000: return 900
        elif rank <= 10000: return 800
        elif rank <= 100000: return 700
        else: return 600

    def generate_extended_urls(self, domain_data: List[Dict], common_paths: Optional[List[str]] = None) -> List[Dict]:
        """도메인에서 확장 URL 생성"""
        if common_paths is None:
            common_paths = ['', 'about/', 'contact/', 'products/', 'services/', 'news/', 'blog/', 'support/', 'help/', 'privacy/', 'terms/', 'sitemap.xml', 'robots.txt']

        extended_urls = []
        for domain_info in domain_data[:1000]:  # 상위 1000개 도메인만
            base_url = domain_info['url'].rstrip('/')
            domain = domain_info['domain']
            rank = domain_info['rank']
            base_priority = domain_info['priority']

            for i, path in enumerate(common_paths):
                extended_urls.append({
                    'url': f"{base_url}/{path}",
                    'rank': rank,
                    'domain': domain,
                    'priority': base_priority - i - 10,
                    'url_type': 'extended',
                    'path': path
                })
        logger.info(f"확장 URL 생성 완료: {len(extended_urls)}개")
        return extended_urls

    def save_urls_to_file(self, urls: List[Dict], filename: Optional[str] = None):
        """URL 리스트를 파일로 저장"""
        if filename is None: filename = self.processed_file
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                for url_info in urls:
                    line = f"{url_info['priority']},{url_info['rank']},{url_info['domain']},{url_info['url']},{url_info['url_type']}\n"
                    f.write(line)
            logger.info(f"URL 리스트 저장 완료: {filename} ({len(urls)}개)")
        except Exception as e:
            logger.error(f"URL 리스트 저장 실패: {e}")

    async def prepare_url_dataset(self, initial_count: int = 10000) -> List[Dict]:
        """URL 데이터셋 준비 (1 도메인 = 1 URL)"""
        logger.info(f"URL 데이터셋 준비 시작 (목표: {initial_count}개 도메인)")

        # 1. Tranco 라이브러리로 목록 가져오기 (요청한 count만큼 정확히 가져옴)
        latest_tranco_list = await self.get_latest_list(limit=initial_count)
        if not latest_tranco_list:
            logger.error("Tranco 목록을 가져오지 못했습니다.")
            return []

        # 2. 기본 URL 생성 (1 도메인 = 1 https URL)
        urls = self.parse_tranco_list_to_urls(latest_tranco_list)
        urls.sort(key=lambda x: x['priority'], reverse=True)

        # 3. 파일로 저장
        self.save_urls_to_file(urls)

        logger.info(f"URL 데이터셋 준비 완료: {len(urls)}개 (고유 도메인)")
        return urls

async def main():
    """테스트 실행"""
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    manager = TrancoManager()
    urls = await manager.prepare_url_dataset(initial_count=1000)

    if urls:
        print(f"\n[OK] 총 {len(urls)}개 URL 준비 완료")
        print("\n[LIST] 상위 10개 URL:")
        for i, url_info in enumerate(urls[:10], 1):
            print(f"{i:2d}. [{url_info['priority']:4d}] {url_info['url']} (순위: {url_info['rank']})")
    else:
        print("[ERROR] URL 데이터셋 준비 실패")

if __name__ == "__main__":
    asyncio.run(main())

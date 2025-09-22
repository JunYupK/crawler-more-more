import asyncio
import aiohttp
import csv
import os
import gzip
import io
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
from urllib.parse import urlparse, urljoin
import logging
import hashlib
from pathlib import Path

# work_logger import 추가
try:
    from work_logger import WorkLogger
except ImportError:
    WorkLogger = None

logger = logging.getLogger(__name__)

class TrancoManager:
    """Tranco Top 1M 리스트 관리자"""

    def __init__(self, data_dir="data"):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(exist_ok=True)

        # Tranco 설정
        self.base_url = "https://tranco-list.eu"
        self.current_list_url = f"{self.base_url}/list/NQKMX/full"
        self.cache_duration = timedelta(hours=24)  # 24시간 캐시

        # 파일 경로
        self.csv_file = self.data_dir / "tranco_top1m.csv"
        self.processed_file = self.data_dir / "tranco_urls.txt"
        self.metadata_file = self.data_dir / "tranco_metadata.txt"

        # 작업 로거 초기화
        self.work_logger = WorkLogger() if WorkLogger else None

    async def download_latest_list(self, force_update=False) -> bool:
        """최신 Tranco 리스트 다운로드"""
        try:
            # 캐시 확인
            if not force_update and self._is_cache_valid():
                logger.info("유효한 캐시 파일 존재, 다운로드 건너뛰기")
                return True

            logger.info("Tranco Top 1M 리스트 다운로드 시작...")

            async with aiohttp.ClientSession() as session:
                # 최신 리스트 ID 확인
                list_id = await self._get_latest_list_id(session)
                if not list_id:
                    logger.error("최신 리스트 ID 획득 실패")
                    return False

                download_url = f"{self.base_url}/list/{list_id}/1000000"
                logger.info(f"다운로드 URL: {download_url}")

                async with session.get(download_url) as response:
                    if response.status != 200:
                        logger.error(f"다운로드 실패: HTTP {response.status}")
                        return False

                    content = await response.read()

                    # 압축된 데이터인지 확인
                    if content.startswith(b'\x1f\x8b'):  # gzip magic number
                        content = gzip.decompress(content)

                    # CSV 파일로 저장
                    with open(self.csv_file, 'wb') as f:
                        f.write(content)

                    # 메타데이터 저장
                    self._save_metadata(list_id, len(content))

                    logger.info(f"Tranco 리스트 다운로드 완료: {len(content)} bytes")
                    return True

        except Exception as e:
            logger.error(f"Tranco 리스트 다운로드 실패: {e}")
            return False

    async def _get_latest_list_id(self, session: aiohttp.ClientSession) -> Optional[str]:
        """최신 리스트 ID 획득"""
        try:
            async with session.get(f"{self.base_url}/list/NQKMX") as response:
                if response.status == 200:
                    return "NQKMX"  # 고정 ID 사용
                return None
        except Exception as e:
            logger.error(f"리스트 ID 획득 실패: {e}")
            return None

    def _is_cache_valid(self) -> bool:
        """캐시 유효성 검사"""
        if not self.csv_file.exists():
            return False

        file_time = datetime.fromtimestamp(self.csv_file.stat().st_mtime)
        return datetime.now() - file_time < self.cache_duration

    def _save_metadata(self, list_id: str, file_size: int):
        """메타데이터 저장"""
        metadata = {
            'list_id': list_id,
            'download_time': datetime.now().isoformat(),
            'file_size': file_size,
            'file_path': str(self.csv_file)
        }

        with open(self.metadata_file, 'w') as f:
            for key, value in metadata.items():
                f.write(f"{key}: {value}\n")

    def parse_csv_to_urls(self, limit: Optional[int] = None,
                         start_rank: int = 1, add_www: bool = True) -> List[Dict[str, any]]:
        """CSV를 파싱하여 URL 리스트 생성"""
        try:
            if not self.csv_file.exists():
                logger.error("Tranco CSV 파일이 존재하지 않습니다")
                return []

            urls = []
            protocols = ['https://', 'http://']

            logger.info(f"CSV 파싱 시작 (순위 {start_rank}부터 {limit or '끝'}까지)")

            with open(self.csv_file, 'r', encoding='utf-8') as f:
                csv_reader = csv.reader(f)

                for row_num, row in enumerate(csv_reader, 1):
                    if row_num < start_rank:
                        continue

                    if limit and len(urls) >= limit:
                        break

                    if len(row) >= 2:
                        rank = int(row[0])
                        domain = row[1].strip()

                        # 기본 도메인 URL들 생성
                        domain_urls = []

                        # 프로토콜별 기본 URL
                        for protocol in protocols:
                            domain_urls.append({
                                'url': f"{protocol}{domain}/",
                                'rank': rank,
                                'domain': domain,
                                'priority': self._calculate_priority(rank),
                                'url_type': 'root'
                            })

                            # www 버전도 추가 (옵션)
                            if add_www and not domain.startswith('www.'):
                                domain_urls.append({
                                    'url': f"{protocol}www.{domain}/",
                                    'rank': rank,
                                    'domain': f"www.{domain}",
                                    'priority': self._calculate_priority(rank) - 1,
                                    'url_type': 'www'
                                })

                        urls.extend(domain_urls)

            logger.info(f"URL 생성 완료: {len(urls)}개")
            return urls

        except Exception as e:
            logger.error(f"CSV 파싱 실패: {e}")
            return []

    def _calculate_priority(self, rank: int) -> int:
        """순위 기반 우선순위 계산"""
        if rank <= 100:
            return 1000  # 최고 우선순위
        elif rank <= 1000:
            return 900
        elif rank <= 10000:
            return 800
        elif rank <= 100000:
            return 700
        else:
            return 600  # 기본 우선순위

    def generate_extended_urls(self, domain_data: List[Dict],
                              common_paths: Optional[List[str]] = None) -> List[Dict]:
        """도메인에서 확장 URL 생성"""
        if common_paths is None:
            common_paths = [
                '',  # 루트
                'about/',
                'contact/',
                'products/',
                'services/',
                'news/',
                'blog/',
                'support/',
                'help/',
                'privacy/',
                'terms/',
                'sitemap.xml',
                'robots.txt'
            ]

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
                    'priority': base_priority - i - 10,  # 경로별로 우선순위 조정
                    'url_type': 'extended',
                    'path': path
                })

        logger.info(f"확장 URL 생성 완료: {len(extended_urls)}개")
        return extended_urls

    def save_urls_to_file(self, urls: List[Dict], filename: Optional[str] = None):
        """URL 리스트를 파일로 저장"""
        if filename is None:
            filename = self.processed_file

        try:
            with open(filename, 'w', encoding='utf-8') as f:
                for url_info in urls:
                    line = f"{url_info['priority']},{url_info['rank']},{url_info['domain']},{url_info['url']},{url_info['url_type']}\n"
                    f.write(line)

            logger.info(f"URL 리스트 저장 완료: {filename} ({len(urls)}개)")

        except Exception as e:
            logger.error(f"URL 리스트 저장 실패: {e}")

    def load_urls_from_file(self, filename: Optional[str] = None, limit: Optional[int] = None) -> List[Dict]:
        """파일에서 URL 리스트 로드"""
        if filename is None:
            filename = self.processed_file

        try:
            if not Path(filename).exists():
                logger.error(f"URL 파일이 존재하지 않습니다: {filename}")
                return []

            urls = []
            with open(filename, 'r', encoding='utf-8') as f:
                for line_num, line in enumerate(f, 1):
                    if limit and len(urls) >= limit:
                        break

                    line = line.strip()
                    if line:
                        parts = line.split(',', 4)
                        if len(parts) >= 5:
                            urls.append({
                                'priority': int(parts[0]),
                                'rank': int(parts[1]),
                                'domain': parts[2],
                                'url': parts[3],
                                'url_type': parts[4]
                            })

            logger.info(f"URL 리스트 로드 완료: {len(urls)}개")
            return urls

        except Exception as e:
            logger.error(f"URL 리스트 로드 실패: {e}")
            return []

    async def prepare_url_dataset(self, initial_count: int = 10000,
                                 force_update: bool = False) -> List[Dict]:
        """URL 데이터셋 준비"""
        logger.info(f"URL 데이터셋 준비 시작 (목표: {initial_count}개)")

        # 1. Tranco 리스트 다운로드
        if not await self.download_latest_list(force_update):
            logger.error("Tranco 리스트 다운로드 실패")
            return []

        # 2. 기본 URL 생성 (상위 N개 도메인)
        domain_count = min(initial_count // 4, 2500)  # 도메인당 평균 4개 URL
        basic_urls = self.parse_csv_to_urls(
            limit=domain_count,
            start_rank=1,
            add_www=True
        )

        # 3. 확장 URL 생성 (상위 도메인들)
        if len(basic_urls) > 0:
            # 상위 500개 도메인에서 확장 URL 생성
            top_domains = [url for url in basic_urls if url['rank'] <= 500]
            extended_urls = self.generate_extended_urls(
                top_domains,
                common_paths=['', 'about/', 'contact/', 'products/', 'news/']
            )

            # 기본 + 확장 URL 합치기
            all_urls = basic_urls + extended_urls
        else:
            all_urls = basic_urls

        # 4. 우선순위별 정렬
        all_urls.sort(key=lambda x: x['priority'], reverse=True)

        # 5. 목표 개수만큼 자르기
        if len(all_urls) > initial_count:
            all_urls = all_urls[:initial_count]

        # 6. 파일로 저장
        self.save_urls_to_file(all_urls)

        logger.info(f"URL 데이터셋 준비 완료: {len(all_urls)}개")
        return all_urls

    def get_domain_stats(self, urls: List[Dict]) -> Dict[str, int]:
        """도메인별 통계"""
        domain_count = {}
        for url_info in urls:
            domain = url_info['domain']
            domain_count[domain] = domain_count.get(domain, 0) + 1

        return dict(sorted(domain_count.items(), key=lambda x: x[1], reverse=True))

async def main():
    """테스트 실행"""
    logging.basicConfig(level=logging.INFO)

    manager = TrancoManager()

    # URL 데이터셋 준비
    urls = await manager.prepare_url_dataset(initial_count=1000, force_update=False)

    if urls:
        print(f"\n[OK] 총 {len(urls)}개 URL 준비 완료")

        # 상위 10개 출력
        print("\n[LIST] 상위 10개 URL:")
        for i, url_info in enumerate(urls[:10], 1):
            print(f"{i:2d}. [{url_info['priority']:4d}] {url_info['url']} (순위: {url_info['rank']})")

        # 도메인 통계
        domain_stats = manager.get_domain_stats(urls)
        print(f"\n[STATS] 상위 5개 도메인별 URL 수:")
        for domain, count in list(domain_stats.items())[:5]:
            print(f"  {domain}: {count}개")

    else:
        print("[ERROR] URL 데이터셋 준비 실패")

if __name__ == "__main__":
    asyncio.run(main())
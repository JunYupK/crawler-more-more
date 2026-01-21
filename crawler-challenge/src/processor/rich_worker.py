"""
Rich Worker - Crawl4AI 기반 처리기
==================================

동적 페이지 처리 (예상 30% 트래픽)
- Crawl4AI로 JavaScript 렌더링
- 브라우저 풀링으로 리소스 최적화
- LLM 학습용 고품질 Markdown 추출
"""

import asyncio
import logging
from typing import Optional
from urllib.parse import urljoin

from .base_worker import BaseWorker, ProcessorType, ProcessedResult

import sys
sys.path.insert(0, '/home/user/crawler-more-more/crawler-challenge')
from config.kafka_config import get_config

logger = logging.getLogger(__name__)

# Crawl4AI import (선택적)
try:
    from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig
    CRAWL4AI_AVAILABLE = True
except ImportError:
    CRAWL4AI_AVAILABLE = False
    logger.warning("Crawl4AI not installed. Rich worker will use fallback mode.")


class RichWorker(BaseWorker):
    """
    Crawl4AI 기반 리치 처리 워커

    동적 페이지 (score < 80) 처리
    - JavaScript 렌더링
    - 동적 콘텐츠 추출
    - LLM 최적화 Markdown 생성
    """

    def __init__(
        self,
        bootstrap_servers: Optional[str] = None,
        group_id: Optional[str] = None,
        worker_id: int = 0,
        headless: bool = True,
        browser_type: str = "chromium",
    ):
        config = get_config()
        super().__init__(
            source_topic=config.topics.process_rich,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            worker_id=worker_id,
        )

        self.headless = headless
        self.browser_type = browser_type
        self._crawler: Optional[AsyncWebCrawler] = None

    def get_processor_type(self) -> ProcessorType:
        return ProcessorType.RICH

    async def start(self) -> None:
        """워커 시작 (브라우저 초기화 포함)"""
        await super().start()

        if CRAWL4AI_AVAILABLE:
            try:
                # 브라우저 설정
                browser_config = BrowserConfig(
                    headless=self.headless,
                    browser_type=self.browser_type,
                    viewport_width=1280,
                    viewport_height=800,
                )

                self._crawler = AsyncWebCrawler(config=browser_config)
                await self._crawler.start()

                logger.info(f"Crawl4AI browser initialized (worker_id={self.worker_id})")

            except Exception as e:
                logger.error(f"Failed to initialize Crawl4AI: {e}")
                self._crawler = None

    async def stop(self) -> None:
        """워커 중지 (브라우저 정리 포함)"""
        if self._crawler:
            try:
                await self._crawler.close()
            except Exception as e:
                logger.error(f"Error closing Crawl4AI: {e}")
            self._crawler = None

        await super().stop()

    async def process_html(self, html: str, url: str, metadata: dict) -> ProcessedResult:
        """
        HTML 처리 (Crawl4AI)

        라우터에서 이미 HTML을 가져왔지만, 동적 페이지는
        JavaScript 렌더링이 필요하므로 URL을 다시 방문

        Args:
            html: 원본 HTML (참고용)
            url: 페이지 URL
            metadata: 추가 메타데이터

        Returns:
            ProcessedResult 객체
        """
        if CRAWL4AI_AVAILABLE and self._crawler:
            return await self._process_with_crawl4ai(url, metadata)
        else:
            return await self._process_fallback(html, url, metadata)

    async def _process_with_crawl4ai(self, url: str, metadata: dict) -> ProcessedResult:
        """Crawl4AI로 처리"""
        try:
            # 크롤러 실행 설정
            run_config = CrawlerRunConfig(
                word_count_threshold=50,
                excluded_tags=['nav', 'footer', 'aside', 'header'],
                remove_overlay_elements=True,
                process_iframes=False,
            )

            # 페이지 크롤링
            result = await self._crawler.arun(
                url=url,
                config=run_config,
            )

            if not result.success:
                return ProcessedResult(
                    url=url,
                    success=False,
                    processor_type=ProcessorType.RICH,
                    error_type="CrawlError",
                    error_message=result.error_message or "Crawl4AI failed",
                )

            # 결과 추출
            markdown = result.markdown or ""
            plain_text = result.cleaned_html or ""

            # 메타데이터
            extracted_metadata = result.metadata or {}
            title = extracted_metadata.get('title')
            description = extracted_metadata.get('description')

            # 링크 추출
            links = []
            if hasattr(result, 'links') and result.links:
                links = [link.get('href', '') for link in result.links if link.get('href')][:100]

            # 이미지 추출
            images = []
            if hasattr(result, 'images') and result.images:
                images = [img.get('src', '') for img in result.images if img.get('src')][:50]

            return ProcessedResult(
                url=url,
                success=True,
                processor_type=ProcessorType.RICH,
                markdown=markdown,
                plain_text=plain_text[:10000],
                title=title,
                description=description,
                links=links,
                images=images,
                content_length=len(result.html or ''),
                markdown_length=len(markdown),
            )

        except asyncio.TimeoutError:
            return ProcessedResult(
                url=url,
                success=False,
                processor_type=ProcessorType.RICH,
                error_type="TimeoutError",
                error_message="Crawl4AI timeout",
            )
        except Exception as e:
            logger.error(f"Crawl4AI error for {url}: {e}")
            return ProcessedResult(
                url=url,
                success=False,
                processor_type=ProcessorType.RICH,
                error_type=type(e).__name__,
                error_message=str(e),
            )

    async def _process_fallback(self, html: str, url: str, metadata: dict) -> ProcessedResult:
        """
        폴백 처리 (Crawl4AI 없을 때)

        BeautifulSoup으로 기본 처리 수행
        """
        try:
            from bs4 import BeautifulSoup
            import html2text

            soup = BeautifulSoup(html, 'lxml')

            # 불필요한 태그 제거
            for tag in soup(['script', 'style', 'noscript', 'nav', 'footer']):
                tag.decompose()

            # 제목 추출
            title = None
            if soup.title:
                title = soup.title.string
            elif soup.find('h1'):
                title = soup.find('h1').get_text(strip=True)

            # 설명 추출
            description = None
            desc_meta = soup.find('meta', attrs={'name': 'description'})
            if desc_meta:
                description = desc_meta.get('content')

            # Markdown 변환
            h2t = html2text.HTML2Text()
            h2t.ignore_links = False
            h2t.body_width = 0

            body = soup.find('body') or soup
            markdown = h2t.handle(str(body))

            # Plain text
            plain_text = body.get_text(separator=' ', strip=True)

            # 링크 추출
            links = []
            for a in soup.find_all('a', href=True):
                href = a['href']
                if href and not href.startswith(('#', 'javascript:')):
                    links.append(urljoin(url, href))

            # 이미지 추출
            images = []
            for img in soup.find_all('img', src=True):
                src = img['src']
                if src and not src.startswith('data:'):
                    images.append(urljoin(url, src))

            return ProcessedResult(
                url=url,
                success=True,
                processor_type=ProcessorType.RICH,
                markdown=markdown,
                plain_text=plain_text[:10000],
                title=title,
                description=description,
                links=links[:100],
                images=images[:50],
                content_length=len(html),
                markdown_length=len(markdown),
            )

        except Exception as e:
            logger.error(f"Fallback processing error for {url}: {e}")
            return ProcessedResult(
                url=url,
                success=False,
                processor_type=ProcessorType.RICH,
                error_type=type(e).__name__,
                error_message=f"Fallback error: {str(e)}",
            )


class RichWorkerPool:
    """
    Rich Worker 풀

    브라우저 리소스가 많이 필요하므로 제한된 수의 워커 사용
    """

    def __init__(
        self,
        num_workers: int = 2,  # Rich는 리소스 집약적이므로 적게
        bootstrap_servers: Optional[str] = None,
        group_id: Optional[str] = None,
        headless: bool = True,
    ):
        self.num_workers = num_workers
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.headless = headless
        self.workers: list[RichWorker] = []

    async def start(self) -> None:
        """모든 워커 시작"""
        self.workers = [
            RichWorker(
                bootstrap_servers=self.bootstrap_servers,
                group_id=self.group_id,
                worker_id=i,
                headless=self.headless,
            )
            for i in range(self.num_workers)
        ]

        for worker in self.workers:
            await worker.start()

        logger.info(f"Started {self.num_workers} rich workers")

    async def stop(self) -> None:
        """모든 워커 중지"""
        for worker in self.workers:
            await worker.stop()

        logger.info("All rich workers stopped")

    async def run(self, max_messages_per_worker: Optional[int] = None) -> None:
        """모든 워커 실행"""
        tasks = [
            worker.run(max_messages=max_messages_per_worker)
            for worker in self.workers
        ]
        await asyncio.gather(*tasks, return_exceptions=True)

    def get_combined_stats(self) -> dict:
        """전체 워커 통계"""
        total_processed = sum(w.stats.messages_processed for w in self.workers)
        total_failed = sum(w.stats.messages_failed for w in self.workers)
        total_time = sum(w.stats.total_processing_time_ms for w in self.workers)

        return {
            'num_workers': self.num_workers,
            'crawl4ai_available': CRAWL4AI_AVAILABLE,
            'total_processed': total_processed,
            'total_failed': total_failed,
            'average_processing_time_ms': total_time / total_processed if total_processed > 0 else 0,
            'workers': [w.get_stats() for w in self.workers],
        }


def check_crawl4ai_installation() -> dict:
    """Crawl4AI 설치 상태 확인"""
    result = {
        'installed': CRAWL4AI_AVAILABLE,
        'version': None,
        'browser_available': False,
    }

    if CRAWL4AI_AVAILABLE:
        try:
            import crawl4ai
            result['version'] = getattr(crawl4ai, '__version__', 'unknown')

            # 브라우저 확인 (비동기)
            async def check_browser():
                try:
                    crawler = AsyncWebCrawler()
                    await crawler.start()
                    await crawler.close()
                    return True
                except Exception:
                    return False

            # 이벤트 루프 체크
            try:
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    result['browser_available'] = 'unknown (async context)'
                else:
                    result['browser_available'] = loop.run_until_complete(check_browser())
            except RuntimeError:
                result['browser_available'] = 'unknown'

        except Exception as e:
            result['error'] = str(e)

    return result

"""
Fast Worker - BeautifulSoup 기반 처리기
======================================

정적 페이지 처리 (예상 70% 트래픽)
- BeautifulSoup + lxml 파싱
- html2text로 Markdown 변환
- 빠른 메타데이터 추출
"""

import re
import logging
from typing import Optional
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup
import html2text

from .base_worker import BaseWorker, ProcessorType, ProcessedResult

import sys
sys.path.insert(0, '/home/user/crawler-more-more/crawler-challenge')
from config.kafka_config import get_config

logger = logging.getLogger(__name__)


class FastWorker(BaseWorker):
    """
    BeautifulSoup 기반 빠른 처리 워커

    정적 페이지 (score >= 80) 처리
    - HTML 파싱 및 정제
    - Markdown 변환
    - 메타데이터/링크/이미지 추출
    """

    # 제거할 태그들
    REMOVE_TAGS = [
        'script', 'style', 'noscript', 'iframe',
        'nav', 'footer', 'aside', 'header',
        'form', 'button', 'input', 'select',
        'svg', 'canvas', 'video', 'audio',
    ]

    # 광고/추적 관련 클래스 패턴
    AD_PATTERNS = [
        r'ad[-_]?', r'ads[-_]?', r'advert',
        r'banner', r'sponsor', r'promo',
        r'social[-_]?share', r'cookie[-_]?',
        r'popup', r'modal', r'overlay',
        r'sidebar', r'widget',
    ]

    def __init__(
        self,
        bootstrap_servers: Optional[str] = None,
        group_id: Optional[str] = None,
        worker_id: int = 0,
    ):
        config = get_config()
        super().__init__(
            source_topic=config.topics.process_fast,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            worker_id=worker_id,
        )

        # html2text 설정
        self.h2t = html2text.HTML2Text()
        self.h2t.ignore_links = False
        self.h2t.ignore_images = False
        self.h2t.ignore_emphasis = False
        self.h2t.body_width = 0  # 줄바꿈 없음
        self.h2t.unicode_snob = True
        self.h2t.skip_internal_links = True
        self.h2t.inline_links = True

        # 광고 패턴 컴파일
        self._ad_pattern = re.compile(
            '|'.join(self.AD_PATTERNS),
            re.IGNORECASE
        )

    def get_processor_type(self) -> ProcessorType:
        return ProcessorType.FAST

    async def process_html(self, html: str, url: str, metadata: dict) -> ProcessedResult:
        """
        HTML 처리 (BeautifulSoup)

        Args:
            html: HTML 콘텐츠
            url: 페이지 URL
            metadata: 추가 메타데이터

        Returns:
            ProcessedResult 객체
        """
        try:
            # BeautifulSoup 파싱
            soup = BeautifulSoup(html, 'lxml')

            # 메타데이터 추출 (정제 전)
            title = self._extract_title(soup)
            description = self._extract_description(soup)
            language = self._extract_language(soup)
            keywords = self._extract_keywords(soup)

            # 콘텐츠 정제
            self._clean_soup(soup)

            # 메인 콘텐츠 추출
            main_content = self._extract_main_content(soup)

            # 링크/이미지/헤딩 추출
            links = self._extract_links(soup, url)
            images = self._extract_images(soup, url)
            headings = self._extract_headings(soup)

            # Markdown 변환
            markdown = self._to_markdown(main_content)

            # Plain text
            plain_text = main_content.get_text(separator=' ', strip=True)

            return ProcessedResult(
                url=url,
                success=True,
                processor_type=ProcessorType.FAST,
                markdown=markdown,
                plain_text=plain_text[:10000],  # 최대 10KB
                title=title,
                description=description,
                language=language,
                keywords=keywords,
                links=links[:100],  # 최대 100개
                images=images[:50],  # 최대 50개
                headings=headings[:20],  # 최대 20개
                content_length=len(html),
                markdown_length=len(markdown),
            )

        except Exception as e:
            logger.error(f"Error processing {url}: {e}")
            return ProcessedResult(
                url=url,
                success=False,
                processor_type=ProcessorType.FAST,
                error_type=type(e).__name__,
                error_message=str(e),
            )

    def _extract_title(self, soup: BeautifulSoup) -> Optional[str]:
        """제목 추출"""
        # <title> 태그
        if soup.title and soup.title.string:
            return soup.title.string.strip()[:200]

        # og:title
        og_title = soup.find('meta', property='og:title')
        if og_title and og_title.get('content'):
            return og_title['content'].strip()[:200]

        # <h1> 태그
        h1 = soup.find('h1')
        if h1:
            return h1.get_text(strip=True)[:200]

        return None

    def _extract_description(self, soup: BeautifulSoup) -> Optional[str]:
        """설명 추출"""
        # meta description
        desc = soup.find('meta', attrs={'name': 'description'})
        if desc and desc.get('content'):
            return desc['content'].strip()[:500]

        # og:description
        og_desc = soup.find('meta', property='og:description')
        if og_desc and og_desc.get('content'):
            return og_desc['content'].strip()[:500]

        return None

    def _extract_language(self, soup: BeautifulSoup) -> Optional[str]:
        """언어 추출"""
        # html lang 속성
        html_tag = soup.find('html')
        if html_tag and html_tag.get('lang'):
            return html_tag['lang'][:10]

        # meta language
        lang_meta = soup.find('meta', attrs={'name': 'language'})
        if lang_meta and lang_meta.get('content'):
            return lang_meta['content'][:10]

        return None

    def _extract_keywords(self, soup: BeautifulSoup) -> Optional[list[str]]:
        """키워드 추출"""
        keywords_meta = soup.find('meta', attrs={'name': 'keywords'})
        if keywords_meta and keywords_meta.get('content'):
            keywords = [k.strip() for k in keywords_meta['content'].split(',')]
            return keywords[:20]

        return None

    def _clean_soup(self, soup: BeautifulSoup) -> None:
        """불필요한 요소 제거 (in-place)"""
        # 지정된 태그 제거
        for tag_name in self.REMOVE_TAGS:
            for tag in soup.find_all(tag_name):
                tag.decompose()

        # 광고/추적 관련 요소 제거
        for element in soup.find_all(class_=self._ad_pattern):
            element.decompose()

        for element in soup.find_all(id=self._ad_pattern):
            element.decompose()

        # 주석 제거
        from bs4 import Comment
        for comment in soup.find_all(string=lambda text: isinstance(text, Comment)):
            comment.extract()

        # 빈 태그 제거
        for tag in soup.find_all():
            if not tag.get_text(strip=True) and tag.name not in ['img', 'br', 'hr']:
                tag.decompose()

    def _extract_main_content(self, soup: BeautifulSoup) -> BeautifulSoup:
        """메인 콘텐츠 영역 추출"""
        # 우선순위 순으로 메인 콘텐츠 영역 찾기
        main_selectors = [
            'article',
            'main',
            '[role="main"]',
            '#content',
            '#main-content',
            '.content',
            '.post-content',
            '.article-content',
            '.entry-content',
        ]

        for selector in main_selectors:
            content = soup.select_one(selector)
            if content and len(content.get_text(strip=True)) > 100:
                return content

        # 찾지 못하면 body 반환
        body = soup.find('body')
        return body if body else soup

    def _extract_links(self, soup: BeautifulSoup, base_url: str) -> list[str]:
        """링크 추출"""
        links = []
        seen = set()

        for a_tag in soup.find_all('a', href=True):
            href = a_tag['href'].strip()

            # 빈 링크, 앵커, JavaScript 제외
            if not href or href.startswith(('#', 'javascript:', 'mailto:', 'tel:')):
                continue

            # 상대 URL을 절대 URL로 변환
            absolute_url = urljoin(base_url, href)

            # 중복 제거
            if absolute_url not in seen:
                seen.add(absolute_url)
                links.append(absolute_url)

        return links

    def _extract_images(self, soup: BeautifulSoup, base_url: str) -> list[str]:
        """이미지 URL 추출"""
        images = []
        seen = set()

        for img_tag in soup.find_all('img'):
            # src 또는 data-src
            src = img_tag.get('src') or img_tag.get('data-src')
            if not src:
                continue

            # base64 이미지 제외
            if src.startswith('data:'):
                continue

            # 상대 URL을 절대 URL로 변환
            absolute_url = urljoin(base_url, src)

            # 중복 제거
            if absolute_url not in seen:
                seen.add(absolute_url)
                images.append(absolute_url)

        return images

    def _extract_headings(self, soup: BeautifulSoup) -> list[str]:
        """헤딩 추출"""
        headings = []

        for level in range(1, 7):
            for heading in soup.find_all(f'h{level}'):
                text = heading.get_text(strip=True)
                if text:
                    headings.append(f"H{level}: {text[:100]}")

        return headings

    def _to_markdown(self, soup: BeautifulSoup) -> str:
        """HTML을 Markdown으로 변환"""
        try:
            html_str = str(soup)
            markdown = self.h2t.handle(html_str)

            # 정리
            # 연속 빈 줄 제거
            markdown = re.sub(r'\n{3,}', '\n\n', markdown)
            # 앞뒤 공백 제거
            markdown = markdown.strip()

            return markdown

        except Exception as e:
            logger.warning(f"Markdown conversion error: {e}")
            # 폴백: plain text
            return soup.get_text(separator='\n', strip=True)


class FastWorkerPool:
    """
    Fast Worker 풀

    여러 워커를 병렬로 실행
    """

    def __init__(
        self,
        num_workers: int = 4,
        bootstrap_servers: Optional[str] = None,
        group_id: Optional[str] = None,
    ):
        self.num_workers = num_workers
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.workers: list[FastWorker] = []

    async def start(self) -> None:
        """모든 워커 시작"""
        self.workers = [
            FastWorker(
                bootstrap_servers=self.bootstrap_servers,
                group_id=self.group_id,
                worker_id=i,
            )
            for i in range(self.num_workers)
        ]

        for worker in self.workers:
            await worker.start()

        logger.info(f"Started {self.num_workers} fast workers")

    async def stop(self) -> None:
        """모든 워커 중지"""
        for worker in self.workers:
            await worker.stop()

        logger.info("All fast workers stopped")

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
            'total_processed': total_processed,
            'total_failed': total_failed,
            'average_processing_time_ms': total_time / total_processed if total_processed > 0 else 0,
            'workers': [w.get_stats() for w in self.workers],
        }


# Import asyncio for FastWorkerPool
import asyncio

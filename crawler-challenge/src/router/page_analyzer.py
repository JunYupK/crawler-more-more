"""
Page Analyzer
=============

HTML 페이지 분석기
- 메타데이터 추출 (title, description, language)
- 기술 스택 감지
- 콘텐츠 품질 평가
- 정적/동적 점수 계산
"""

import re
import logging
from dataclasses import dataclass, field
from typing import Optional
from urllib.parse import urlparse

from bs4 import BeautifulSoup

from .scoring import StaticScoreCalculator, ScoreResult, RouteDecision

logger = logging.getLogger(__name__)


@dataclass
class PageMetadata:
    """페이지 메타데이터"""
    url: str
    title: Optional[str] = None
    description: Optional[str] = None
    keywords: Optional[list[str]] = None
    language: Optional[str] = None
    og_image: Optional[str] = None
    og_type: Optional[str] = None
    canonical_url: Optional[str] = None
    author: Optional[str] = None
    published_date: Optional[str] = None


@dataclass
class TechStack:
    """감지된 기술 스택"""
    frameworks: list[str] = field(default_factory=list)
    cms: Optional[str] = None
    server: Optional[str] = None
    cdn: Optional[str] = None
    analytics: list[str] = field(default_factory=list)


@dataclass
class ContentStats:
    """콘텐츠 통계"""
    text_length: int = 0
    word_count: int = 0
    paragraph_count: int = 0
    heading_count: int = 0
    link_count: int = 0
    image_count: int = 0
    script_count: int = 0
    style_count: int = 0


@dataclass
class AnalysisResult:
    """페이지 분석 결과"""
    url: str
    metadata: PageMetadata
    tech_stack: TechStack
    content_stats: ContentStats
    score_result: ScoreResult

    # 편의 속성
    @property
    def score(self) -> int:
        return self.score_result.score

    @property
    def route(self) -> RouteDecision:
        return self.score_result.route

    @property
    def should_use_browser(self) -> bool:
        return self.route == RouteDecision.RICH

    def to_dict(self) -> dict:
        """딕셔너리로 변환 (직렬화용)"""
        return {
            'url': self.url,
            'score': self.score,
            'route': self.route.value,
            'metadata': {
                'title': self.metadata.title,
                'description': self.metadata.description,
                'language': self.metadata.language,
                'keywords': self.metadata.keywords,
            },
            'tech_stack': {
                'frameworks': self.tech_stack.frameworks,
                'cms': self.tech_stack.cms,
            },
            'content_stats': {
                'text_length': self.content_stats.text_length,
                'word_count': self.content_stats.word_count,
                'paragraph_count': self.content_stats.paragraph_count,
            },
            'score_reasons': self.score_result.reasons[:5],  # 상위 5개만
        }


class PageAnalyzer:
    """
    HTML 페이지 분석기

    페이지의 메타데이터, 기술 스택, 콘텐츠 통계를 분석하고
    정적/동적 점수를 계산하여 라우팅 결정
    """

    # 프레임워크 감지 패턴
    FRAMEWORK_PATTERNS = {
        'React': [r'react', r'_react', r'__REACT'],
        'Vue.js': [r'vue', r'v-cloak', r'v-if'],
        'Angular': [r'angular', r'ng-app', r'ng-controller'],
        'Next.js': [r'__NEXT_DATA__', r'_next/'],
        'Nuxt.js': [r'__NUXT__', r'_nuxt/'],
        'Svelte': [r'svelte', r'__svelte'],
        'jQuery': [r'jquery', r'\$\('],
        'Bootstrap': [r'bootstrap'],
        'Tailwind': [r'tailwindcss', r'tailwind'],
    }

    # CMS 감지 패턴
    CMS_PATTERNS = {
        'WordPress': [r'wp-content', r'wordpress'],
        'Drupal': [r'drupal', r'/sites/default/'],
        'Joomla': [r'joomla', r'/components/com_'],
        'Shopify': [r'shopify', r'cdn\.shopify'],
        'Wix': [r'wix\.com', r'wixstatic'],
        'Squarespace': [r'squarespace', r'sqsp'],
        'Ghost': [r'ghost', r'/ghost/'],
    }

    # 분석 도구 감지 패턴
    ANALYTICS_PATTERNS = {
        'Google Analytics': [r'google-analytics', r'googletagmanager', r'gtag\(', r'ga\('],
        'Facebook Pixel': [r'facebook.*pixel', r'fbq\('],
        'Hotjar': [r'hotjar'],
        'Mixpanel': [r'mixpanel'],
        'Amplitude': [r'amplitude'],
    }

    def __init__(self, score_threshold: int = 80):
        """
        Args:
            score_threshold: FAST/RICH 라우팅 임계값
        """
        self.score_calculator = StaticScoreCalculator(threshold=score_threshold)

    def analyze(self, html: str, url: str = "") -> AnalysisResult:
        """
        HTML 페이지 분석

        Args:
            html: HTML 콘텐츠
            url: 페이지 URL

        Returns:
            AnalysisResult 객체
        """
        try:
            soup = BeautifulSoup(html, 'lxml')
        except Exception:
            # lxml 실패 시 html.parser 사용
            soup = BeautifulSoup(html, 'html.parser')

        # 각 분석 수행
        metadata = self._extract_metadata(soup, url)
        tech_stack = self._detect_tech_stack(html, soup)
        content_stats = self._analyze_content(html, soup)
        score_result = self.score_calculator.calculate(html)

        return AnalysisResult(
            url=url,
            metadata=metadata,
            tech_stack=tech_stack,
            content_stats=content_stats,
            score_result=score_result,
        )

    def _extract_metadata(self, soup: BeautifulSoup, url: str) -> PageMetadata:
        """메타데이터 추출"""
        metadata = PageMetadata(url=url)

        # Title
        if soup.title and soup.title.string:
            metadata.title = soup.title.string.strip()

        # Meta tags
        for meta in soup.find_all('meta'):
            name = meta.get('name', '').lower()
            property_ = meta.get('property', '').lower()
            content = meta.get('content', '')

            if not content:
                continue

            # Description
            if name == 'description' or property_ == 'og:description':
                if not metadata.description:
                    metadata.description = content[:500]

            # Keywords
            elif name == 'keywords':
                metadata.keywords = [k.strip() for k in content.split(',')][:10]

            # Language
            elif name == 'language' or property_ == 'og:locale':
                metadata.language = content[:10]

            # OG Image
            elif property_ == 'og:image':
                metadata.og_image = content

            # OG Type
            elif property_ == 'og:type':
                metadata.og_type = content

            # Author
            elif name == 'author':
                metadata.author = content[:100]

            # Published date
            elif name in ['date', 'pubdate', 'article:published_time']:
                metadata.published_date = content

        # Canonical URL
        canonical = soup.find('link', rel='canonical')
        if canonical:
            metadata.canonical_url = canonical.get('href')

        # HTML lang attribute
        if not metadata.language:
            html_tag = soup.find('html')
            if html_tag:
                metadata.language = html_tag.get('lang', '')[:10]

        return metadata

    def _detect_tech_stack(self, html: str, soup: BeautifulSoup) -> TechStack:
        """기술 스택 감지"""
        tech = TechStack()
        html_lower = html.lower()

        # 프레임워크 감지
        for framework, patterns in self.FRAMEWORK_PATTERNS.items():
            for pattern in patterns:
                if re.search(pattern, html_lower):
                    if framework not in tech.frameworks:
                        tech.frameworks.append(framework)
                    break

        # CMS 감지
        for cms, patterns in self.CMS_PATTERNS.items():
            for pattern in patterns:
                if re.search(pattern, html_lower):
                    tech.cms = cms
                    break
            if tech.cms:
                break

        # 분석 도구 감지
        for analytics, patterns in self.ANALYTICS_PATTERNS.items():
            for pattern in patterns:
                if re.search(pattern, html_lower):
                    if analytics not in tech.analytics:
                        tech.analytics.append(analytics)
                    break

        # 서버 정보 (meta generator)
        generator = soup.find('meta', attrs={'name': 'generator'})
        if generator:
            tech.server = generator.get('content', '')[:50]

        return tech

    def _analyze_content(self, html: str, soup: BeautifulSoup) -> ContentStats:
        """콘텐츠 통계 분석"""
        stats = ContentStats()

        # 텍스트 추출 (script, style 제외)
        for tag in soup(['script', 'style', 'noscript']):
            tag.decompose()

        text = soup.get_text(separator=' ', strip=True)
        stats.text_length = len(text)
        stats.word_count = len(text.split())

        # HTML 요소 카운트 (원본 HTML에서)
        stats.paragraph_count = len(re.findall(r'<p[^>]*>', html, re.IGNORECASE))
        stats.heading_count = len(re.findall(r'<h[1-6][^>]*>', html, re.IGNORECASE))
        stats.link_count = len(re.findall(r'<a[^>]*href=', html, re.IGNORECASE))
        stats.image_count = len(re.findall(r'<img[^>]*>', html, re.IGNORECASE))
        stats.script_count = len(re.findall(r'<script[^>]*>', html, re.IGNORECASE))
        stats.style_count = len(re.findall(r'<style[^>]*>|<link[^>]*stylesheet', html, re.IGNORECASE))

        return stats

    def quick_score(self, html: str) -> int:
        """빠른 점수 계산 (상세 분석 없이)"""
        return self.score_calculator.calculate(html).score

    def should_use_browser(self, html: str) -> bool:
        """브라우저 필요 여부 (빠른 판단)"""
        result = self.score_calculator.calculate(html)
        return result.route == RouteDecision.RICH


class AnalyzerStats:
    """분석기 통계"""

    def __init__(self):
        self.total_analyzed = 0
        self.fast_routed = 0
        self.rich_routed = 0
        self.total_score = 0
        self.score_distribution = {
            '0-20': 0,
            '21-40': 0,
            '41-60': 0,
            '61-80': 0,
            '81-100': 0,
        }

    def record(self, result: AnalysisResult):
        """분석 결과 기록"""
        self.total_analyzed += 1
        self.total_score += result.score

        if result.route == RouteDecision.FAST:
            self.fast_routed += 1
        else:
            self.rich_routed += 1

        # 점수 분포
        if result.score <= 20:
            self.score_distribution['0-20'] += 1
        elif result.score <= 40:
            self.score_distribution['21-40'] += 1
        elif result.score <= 60:
            self.score_distribution['41-60'] += 1
        elif result.score <= 80:
            self.score_distribution['61-80'] += 1
        else:
            self.score_distribution['81-100'] += 1

    @property
    def average_score(self) -> float:
        return self.total_score / self.total_analyzed if self.total_analyzed > 0 else 0

    @property
    def fast_ratio(self) -> float:
        return self.fast_routed / self.total_analyzed if self.total_analyzed > 0 else 0

    def __str__(self) -> str:
        return (
            f"AnalyzerStats("
            f"total={self.total_analyzed}, "
            f"fast={self.fast_routed} ({self.fast_ratio:.1%}), "
            f"rich={self.rich_routed}, "
            f"avg_score={self.average_score:.1f})"
        )

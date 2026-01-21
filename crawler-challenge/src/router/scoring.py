"""
Static Score Calculator
=======================

페이지의 정적/동적 여부를 점수로 계산
- 100점: 완전 정적 (BeautifulSoup으로 충분)
- 0점: 완전 동적 (Crawl4AI/브라우저 필요)
"""

import re
import logging
from dataclasses import dataclass, field
from typing import Optional
from enum import Enum

logger = logging.getLogger(__name__)


class RouteDecision(Enum):
    """라우팅 결정"""
    FAST = "fast"   # BeautifulSoup (정적)
    RICH = "rich"   # Crawl4AI (동적)


@dataclass
class ScoreResult:
    """점수 계산 결과"""
    score: int                          # 최종 점수 (0-100)
    route: RouteDecision                # 라우팅 결정
    reasons: list[str] = field(default_factory=list)  # 점수 변동 이유
    details: dict = field(default_factory=dict)       # 상세 분석 정보

    def add_reason(self, reason: str, delta: int):
        """점수 변동 이유 추가"""
        sign = "+" if delta > 0 else ""
        self.reasons.append(f"{reason} ({sign}{delta})")

    def __str__(self) -> str:
        return (
            f"ScoreResult(score={self.score}, route={self.route.value}, "
            f"reasons=[{', '.join(self.reasons[:3])}...])"
        )


@dataclass
class ScoringRule:
    """점수 계산 규칙"""
    name: str           # 규칙 이름
    pattern: str        # 정규표현식 패턴
    delta: int          # 점수 변동 (-값: 동적 지표, +값: 정적 지표)
    description: str    # 설명
    flags: int = re.IGNORECASE  # 정규표현식 플래그


class StaticScoreCalculator:
    """
    정적 점수 계산기

    페이지가 얼마나 정적인지 (BeautifulSoup으로 처리 가능한지) 점수화
    - 기본 점수: 100점
    - 동적 지표 발견 시 감점
    - 정적 지표 발견 시 가점
    """

    # 기본 점수
    BASE_SCORE = 100

    # 라우팅 임계값 (이 값 이상이면 FAST)
    THRESHOLD = 80

    # 동적 페이지 지표 (감점)
    DYNAMIC_INDICATORS: list[ScoringRule] = [
        # JavaScript 프레임워크 지표
        ScoringRule(
            name="next_js",
            pattern=r'__NEXT_DATA__|_next/static',
            delta=-25,
            description="Next.js detected (SSR possible but needs verification)",
        ),
        ScoringRule(
            name="nuxt_js",
            pattern=r'window\.__NUXT__|_nuxt/',
            delta=-20,
            description="Nuxt.js detected",
        ),
        ScoringRule(
            name="angular",
            pattern=r'ng-app|ng-controller|angular\.module',
            delta=-40,
            description="Angular detected (heavy JS dependency)",
        ),
        ScoringRule(
            name="react_csr",
            pattern=r'<div\s+id=["\']root["\']\s*>\s*</div>|<div\s+id=["\']app["\']\s*>\s*</div>',
            delta=-35,
            description="Empty React/Vue mount point (CSR likely)",
        ),
        ScoringRule(
            name="react_ssr",
            pattern=r'data-reactroot|data-react-helmet',
            delta=-10,
            description="React SSR detected (content likely present)",
        ),
        ScoringRule(
            name="vue_js",
            pattern=r'v-cloak|v-if|v-for|Vue\.component',
            delta=-30,
            description="Vue.js directives detected",
        ),
        ScoringRule(
            name="svelte",
            pattern=r'svelte-\w+|__svelte',
            delta=-20,
            description="Svelte detected",
        ),

        # JavaScript 번들 지표
        ScoringRule(
            name="webpack_bundle",
            pattern=r'<script[^>]*src=["\'][^"\']*(?:bundle|chunk|vendor)\.[a-f0-9]+\.js',
            delta=-20,
            description="Webpack bundle detected",
        ),
        ScoringRule(
            name="heavy_js",
            pattern=r'<script[^>]*src=["\'][^"\']*\.js["\'][^>]*>\s*</script>',
            delta=-5,
            description="External JS file (minor)",
        ),

        # 동적 로딩 지표
        ScoringRule(
            name="lazy_loading",
            pattern=r'loading=["\']lazy["\']|data-src=|lazyload',
            delta=-15,
            description="Lazy loading detected",
        ),
        ScoringRule(
            name="infinite_scroll",
            pattern=r'infinite.?scroll|load.?more|pagination',
            delta=-10,
            description="Infinite scroll/pagination detected",
        ),
        ScoringRule(
            name="ajax_content",
            pattern=r'\.ajax\(|fetch\(|XMLHttpRequest|axios\.',
            delta=-25,
            description="AJAX content loading detected",
        ),

        # 콘텐츠 부족 지표
        ScoringRule(
            name="noscript_warning",
            pattern=r'<noscript[^>]*>.*(?:enable|javascript|browser)',
            delta=-20,
            description="NoScript warning (JS required)",
        ),
        ScoringRule(
            name="loading_ui",
            pattern=r'class=["\'][^"\']*(?:loading|spinner|skeleton|placeholder)[^"\']*["\']',
            delta=-25,
            description="Loading UI detected (content not rendered)",
        ),
        ScoringRule(
            name="empty_body",
            pattern=r'<body[^>]*>\s*(?:<div[^>]*>\s*</div>\s*)*</body>',
            delta=-40,
            description="Nearly empty body (JS rendering required)",
        ),

        # SPA 라우터
        ScoringRule(
            name="spa_router",
            pattern=r'react-router|vue-router|@angular/router|history\.push',
            delta=-15,
            description="SPA router detected",
        ),

        # Web Components
        ScoringRule(
            name="web_components",
            pattern=r'customElements\.define|<template\s+id=',
            delta=-15,
            description="Web Components detected",
        ),
    ]

    # 정적 페이지 지표 (가점)
    STATIC_INDICATORS: list[ScoringRule] = [
        # 시맨틱 HTML 구조
        ScoringRule(
            name="article_tag",
            pattern=r'<article[^>]*>',
            delta=+20,
            description="Semantic <article> tag found",
        ),
        ScoringRule(
            name="main_content",
            pattern=r'<main[^>]*>|id=["\'](?:content|main)["\']',
            delta=+15,
            description="Main content area identified",
        ),
        ScoringRule(
            name="header_structure",
            pattern=r'<h1[^>]*>.*?</h1>',
            delta=+10,
            description="H1 heading found",
        ),

        # 풍부한 텍스트 콘텐츠
        ScoringRule(
            name="long_paragraphs",
            pattern=r'<p[^>]*>.{200,}</p>',
            delta=+15,
            description="Long paragraph content found",
        ),
        ScoringRule(
            name="multiple_paragraphs",
            pattern=r'(?:<p[^>]*>.*?</p>.*?){3,}',
            delta=+10,
            description="Multiple paragraphs found",
            flags=re.IGNORECASE | re.DOTALL,
        ),

        # 정적 사이트 생성기 지표
        ScoringRule(
            name="static_generator",
            pattern=r'generator.*(?:Jekyll|Hugo|Gatsby|11ty|Astro|Hexo)',
            delta=+25,
            description="Static site generator detected",
        ),

        # 서버 사이드 렌더링 확인
        ScoringRule(
            name="ssr_content",
            pattern=r'<meta[^>]*(?:description|og:description)[^>]*content=["\'][^"\']{50,}',
            delta=+10,
            description="Rich meta description (SSR likely)",
        ),

        # 전통적인 웹 구조
        ScoringRule(
            name="table_data",
            pattern=r'<table[^>]*>.*?<tr>.*?</tr>.*?</table>',
            delta=+10,
            description="Data table found",
            flags=re.IGNORECASE | re.DOTALL,
        ),
        ScoringRule(
            name="list_content",
            pattern=r'<(?:ul|ol)[^>]*>(?:.*?<li[^>]*>.*?</li>.*?){3,}</(?:ul|ol)>',
            delta=+10,
            description="List content found",
            flags=re.IGNORECASE | re.DOTALL,
        ),

        # WordPress/CMS (보통 서버 렌더링)
        ScoringRule(
            name="wordpress",
            pattern=r'wp-content|wordpress|wp-includes',
            delta=+15,
            description="WordPress detected (server-rendered)",
        ),
        ScoringRule(
            name="cms_meta",
            pattern=r'generator.*(?:WordPress|Drupal|Joomla|Wix|Squarespace)',
            delta=+15,
            description="CMS detected",
        ),
    ]

    def __init__(self, threshold: int = THRESHOLD):
        """
        Args:
            threshold: FAST/RICH 라우팅 임계값 (기본 80)
        """
        self.threshold = threshold

        # 규칙 컴파일
        self._compiled_dynamic = [
            (rule, re.compile(rule.pattern, rule.flags))
            for rule in self.DYNAMIC_INDICATORS
        ]
        self._compiled_static = [
            (rule, re.compile(rule.pattern, rule.flags))
            for rule in self.STATIC_INDICATORS
        ]

    def calculate(self, html: str) -> ScoreResult:
        """
        HTML 콘텐츠의 정적 점수 계산

        Args:
            html: HTML 콘텐츠

        Returns:
            ScoreResult 객체
        """
        score = self.BASE_SCORE
        reasons = []
        details = {
            'dynamic_matches': [],
            'static_matches': [],
            'text_length': 0,
            'script_count': 0,
        }

        # 기본 분석
        text_length = self._extract_text_length(html)
        script_count = len(re.findall(r'<script[^>]*>', html, re.IGNORECASE))
        details['text_length'] = text_length
        details['script_count'] = script_count

        # 텍스트 길이 기반 점수
        if text_length < 300:
            score -= 35
            reasons.append(f"Very short content ({text_length} chars) (-35)")
        elif text_length < 500:
            score -= 20
            reasons.append(f"Short content ({text_length} chars) (-20)")
        elif text_length > 5000:
            score += 15
            reasons.append(f"Rich content ({text_length} chars) (+15)")
        elif text_length > 2000:
            score += 10
            reasons.append(f"Good content ({text_length} chars) (+10)")

        # 스크립트 수 기반 점수
        if script_count > 20:
            score -= 15
            reasons.append(f"Many scripts ({script_count}) (-15)")
        elif script_count > 10:
            score -= 5
            reasons.append(f"Several scripts ({script_count}) (-5)")

        # 동적 지표 검사
        for rule, pattern in self._compiled_dynamic:
            if pattern.search(html):
                score += rule.delta  # delta는 음수
                reasons.append(f"{rule.name} ({rule.delta})")
                details['dynamic_matches'].append(rule.name)

        # 정적 지표 검사
        for rule, pattern in self._compiled_static:
            if pattern.search(html):
                score += rule.delta  # delta는 양수
                reasons.append(f"{rule.name} (+{rule.delta})")
                details['static_matches'].append(rule.name)

        # 점수 범위 제한
        score = max(0, min(100, score))

        # 라우팅 결정
        route = RouteDecision.FAST if score >= self.threshold else RouteDecision.RICH

        result = ScoreResult(
            score=score,
            route=route,
            reasons=reasons,
            details=details,
        )

        logger.debug(f"Score calculated: {result}")
        return result

    def _extract_text_length(self, html: str) -> int:
        """HTML에서 텍스트 길이 추출 (간단한 방식)"""
        # 스크립트, 스타일 태그 제거
        text = re.sub(r'<script[^>]*>.*?</script>', '', html, flags=re.IGNORECASE | re.DOTALL)
        text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.IGNORECASE | re.DOTALL)
        # HTML 태그 제거
        text = re.sub(r'<[^>]+>', ' ', text)
        # 공백 정리
        text = re.sub(r'\s+', ' ', text).strip()
        return len(text)

    def get_route_reason(self, result: ScoreResult) -> str:
        """라우팅 결정 이유를 문자열로 반환"""
        if result.route == RouteDecision.FAST:
            return (
                f"Score {result.score} >= {self.threshold}: "
                f"Static page, suitable for BeautifulSoup"
            )
        else:
            return (
                f"Score {result.score} < {self.threshold}: "
                f"Dynamic page, requires Crawl4AI"
            )


# 편의 함수
_calculator: Optional[StaticScoreCalculator] = None


def get_calculator(threshold: int = 80) -> StaticScoreCalculator:
    """기본 계산기 인스턴스 반환"""
    global _calculator
    if _calculator is None:
        _calculator = StaticScoreCalculator(threshold=threshold)
    return _calculator


def calculate_score(html: str) -> ScoreResult:
    """간편 점수 계산 함수"""
    return get_calculator().calculate(html)


def should_use_browser(html: str, threshold: int = 80) -> bool:
    """브라우저 필요 여부 반환"""
    result = get_calculator(threshold).calculate(html)
    return result.route == RouteDecision.RICH

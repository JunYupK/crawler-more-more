"""
Router 모듈 테스트
==================

Usage:
    pytest tests/test_router.py -v
    pytest tests/test_router.py -v -k "scoring"
"""

import pytest
import sys
from pathlib import Path

# 프로젝트 루트를 path에 추가
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


class TestStaticScoreCalculator:
    """점수 계산기 테스트"""

    def test_base_score(self):
        """기본 점수 테스트"""
        from src.router.scoring import StaticScoreCalculator

        calc = StaticScoreCalculator()
        assert calc.BASE_SCORE == 100
        assert calc.THRESHOLD == 80

    def test_static_page_high_score(self):
        """정적 페이지 높은 점수 테스트"""
        from src.router.scoring import StaticScoreCalculator, RouteDecision

        calc = StaticScoreCalculator()

        html = """
        <html>
        <head><title>Blog Post</title></head>
        <body>
            <article>
                <h1>My Article Title</h1>
                <p>This is a very long paragraph with substantial content that
                demonstrates this is a real static page with meaningful text.
                The content continues here with more information about the topic.</p>
                <p>Another paragraph with additional content to meet the length
                requirements and show this is a content-rich page.</p>
            </article>
        </body>
        </html>
        """

        result = calc.calculate(html)

        assert result.score >= 70  # 정적 페이지는 높은 점수
        assert result.route == RouteDecision.FAST

    def test_spa_page_low_score(self):
        """SPA 페이지 낮은 점수 테스트"""
        from src.router.scoring import StaticScoreCalculator, RouteDecision

        calc = StaticScoreCalculator()

        html = """
        <html>
        <head><title>React App</title></head>
        <body>
            <div id="root"></div>
            <script src="/static/js/bundle.a1b2c3d4.js"></script>
            <noscript>You need to enable JavaScript to run this app.</noscript>
        </body>
        </html>
        """

        result = calc.calculate(html)

        assert result.score < 80  # SPA는 낮은 점수
        assert result.route == RouteDecision.RICH

    def test_short_content_penalty(self):
        """짧은 콘텐츠 감점 테스트"""
        from src.router.scoring import StaticScoreCalculator

        calc = StaticScoreCalculator()

        html = "<html><body><p>Short</p></body></html>"
        result = calc.calculate(html)

        assert result.score < 70  # 짧은 콘텐츠는 감점
        assert "short content" in str(result.reasons).lower() or "very short" in str(result.reasons).lower()

    def test_wordpress_bonus(self):
        """WordPress 가점 테스트"""
        from src.router.scoring import StaticScoreCalculator

        calc = StaticScoreCalculator()

        html = """
        <html>
        <head><meta name="generator" content="WordPress 6.0"></head>
        <body>
            <div class="wp-content">
                <p>WordPress content with enough text to avoid short content penalty.
                This paragraph needs to be long enough to demonstrate real content.</p>
                <p>Another paragraph to add more substantial content to the page.</p>
            </div>
        </body>
        </html>
        """

        result = calc.calculate(html)

        assert 'wordpress' in [r.split()[0].lower() for r in result.reasons]

    def test_angular_penalty(self):
        """Angular 감점 테스트"""
        from src.router.scoring import StaticScoreCalculator

        calc = StaticScoreCalculator()

        html = """
        <html ng-app="myApp">
        <body ng-controller="MainCtrl">
            <div>{{ content }}</div>
        </body>
        </html>
        """

        result = calc.calculate(html)

        assert result.score < 60  # Angular은 큰 감점
        assert 'angular' in [r.split()[0].lower() for r in result.reasons]

    def test_score_range(self):
        """점수 범위 제한 테스트"""
        from src.router.scoring import StaticScoreCalculator

        calc = StaticScoreCalculator()

        # 최악의 경우도 0 이상
        very_bad_html = """
        <html ng-app>
        <body>
            <div id="root"></div>
            <div class="loading spinner"></div>
            <script src="bundle.js"></script>
            <script src="chunk.js"></script>
            <noscript>Enable JS</noscript>
        </body>
        </html>
        """

        result = calc.calculate(very_bad_html)
        assert 0 <= result.score <= 100


class TestScoreResult:
    """ScoreResult 테스트"""

    def test_add_reason(self):
        """이유 추가 테스트"""
        from src.router.scoring import ScoreResult, RouteDecision

        result = ScoreResult(score=85, route=RouteDecision.FAST)
        result.add_reason("Long content", 10)
        result.add_reason("Angular detected", -40)

        assert len(result.reasons) == 2
        assert "+10" in result.reasons[0]
        assert "-40" in result.reasons[1]


class TestPageAnalyzer:
    """페이지 분석기 테스트"""

    def test_metadata_extraction(self):
        """메타데이터 추출 테스트"""
        from src.router.page_analyzer import PageAnalyzer

        analyzer = PageAnalyzer()

        html = """
        <html lang="ko">
        <head>
            <title>테스트 페이지</title>
            <meta name="description" content="페이지 설명입니다">
            <meta name="keywords" content="test, page, python">
            <meta property="og:image" content="https://example.com/image.jpg">
        </head>
        <body>
            <p>Content here with enough text to avoid short content penalty.
            Adding more text to make sure the page has substantial content.</p>
        </body>
        </html>
        """

        result = analyzer.analyze(html, "https://example.com")

        assert result.metadata.title == "테스트 페이지"
        assert result.metadata.description == "페이지 설명입니다"
        assert "test" in result.metadata.keywords
        assert result.metadata.language == "ko"
        assert result.metadata.og_image == "https://example.com/image.jpg"

    def test_tech_stack_detection(self):
        """기술 스택 감지 테스트"""
        from src.router.page_analyzer import PageAnalyzer

        analyzer = PageAnalyzer()

        html = """
        <html>
        <head>
            <script src="https://code.jquery.com/jquery-3.6.0.min.js"></script>
            <link href="bootstrap.css" rel="stylesheet">
        </head>
        <body>
            <p>jQuery and Bootstrap page with enough content to analyze properly.
            This needs to be long enough to avoid short content penalties.</p>
        </body>
        </html>
        """

        result = analyzer.analyze(html)

        assert 'jQuery' in result.tech_stack.frameworks
        assert 'Bootstrap' in result.tech_stack.frameworks

    def test_content_stats(self):
        """콘텐츠 통계 테스트"""
        from src.router.page_analyzer import PageAnalyzer

        analyzer = PageAnalyzer()

        html = """
        <html>
        <body>
            <h1>Title</h1>
            <h2>Subtitle</h2>
            <p>Paragraph 1 with some content.</p>
            <p>Paragraph 2 with more content.</p>
            <a href="#">Link 1</a>
            <a href="#">Link 2</a>
            <img src="image.jpg">
            <script>console.log('test');</script>
        </body>
        </html>
        """

        result = analyzer.analyze(html)

        assert result.content_stats.heading_count == 2
        assert result.content_stats.paragraph_count == 2
        assert result.content_stats.link_count == 2
        assert result.content_stats.image_count == 1
        assert result.content_stats.script_count == 1

    def test_cms_detection(self):
        """CMS 감지 테스트"""
        from src.router.page_analyzer import PageAnalyzer

        analyzer = PageAnalyzer()

        html = """
        <html>
        <head>
            <meta name="generator" content="WordPress 6.0">
        </head>
        <body>
            <div class="wp-content">
                <p>WordPress content with substantial text for analysis.</p>
            </div>
        </body>
        </html>
        """

        result = analyzer.analyze(html)

        assert result.tech_stack.cms == 'WordPress'

    def test_to_dict(self):
        """딕셔너리 변환 테스트"""
        from src.router.page_analyzer import PageAnalyzer

        analyzer = PageAnalyzer()
        html = "<html><body><p>Test content that is long enough.</p></body></html>"

        result = analyzer.analyze(html, "https://example.com")
        data = result.to_dict()

        assert 'url' in data
        assert 'score' in data
        assert 'route' in data
        assert 'metadata' in data

    def test_quick_score(self):
        """빠른 점수 계산 테스트"""
        from src.router.page_analyzer import PageAnalyzer

        analyzer = PageAnalyzer()
        html = "<html><body><article><p>Long content here for testing purposes.</p></article></body></html>"

        score = analyzer.quick_score(html)

        assert isinstance(score, int)
        assert 0 <= score <= 100


class TestAnalyzerStats:
    """분석기 통계 테스트"""

    def test_record_and_calculate(self):
        """기록 및 계산 테스트"""
        from src.router.page_analyzer import PageAnalyzer, AnalyzerStats

        stats = AnalyzerStats()
        analyzer = PageAnalyzer()

        # 정적 페이지 분석
        static_html = """
        <html><body>
            <article><h1>Title</h1>
            <p>Long static content that should score high and be routed to fast path.
            This paragraph contains substantial text for analysis.</p></article>
        </body></html>
        """
        result1 = analyzer.analyze(static_html)
        stats.record(result1)

        # SPA 페이지 분석
        spa_html = "<html><body><div id='root'></div><script src='bundle.js'></script></body></html>"
        result2 = analyzer.analyze(spa_html)
        stats.record(result2)

        assert stats.total_analyzed == 2
        assert stats.fast_routed + stats.rich_routed == 2


class TestSmartRouter:
    """SmartRouter 테스트"""

    @pytest.mark.asyncio
    async def test_process_single(self):
        """단일 처리 테스트"""
        from src.router.smart_router import SmartRouter
        from src.router.scoring import RouteDecision

        router = SmartRouter(score_threshold=80)

        html = """
        <html><body>
            <article><h1>Blog Post</h1>
            <p>This is a static blog post with substantial content for analysis.
            The content needs to be long enough to avoid short content penalties.</p></article>
        </body></html>
        """

        result = await router.process_single(html, "https://example.com/blog")

        assert result.url == "https://example.com/blog"
        assert isinstance(result.score, int)
        assert result.route in [RouteDecision.FAST, RouteDecision.RICH]

    def test_get_stats(self):
        """통계 조회 테스트"""
        from src.router.smart_router import SmartRouter

        router = SmartRouter()
        stats = router.get_stats()

        assert 'consumed' in stats
        assert 'routed_fast' in stats
        assert 'routed_rich' in stats
        assert 'analyzer' in stats


class TestRouterStats:
    """RouterStats 테스트"""

    def test_fast_ratio(self):
        """Fast 비율 계산 테스트"""
        from src.router.smart_router import RouterStats

        stats = RouterStats()
        stats.messages_routed_fast = 70
        stats.messages_routed_rich = 30

        assert stats.fast_ratio == 0.7
        assert stats.total_routed == 100

    def test_empty_stats(self):
        """빈 통계 테스트"""
        from src.router.smart_router import RouterStats

        stats = RouterStats()

        assert stats.fast_ratio == 0
        assert stats.messages_per_second == 0


class TestRoutedMessage:
    """RoutedMessage 테스트"""

    def test_message_creation(self):
        """메시지 생성 테스트"""
        from src.router.smart_router import RoutedMessage
        from src.router.scoring import RouteDecision

        msg = RoutedMessage(
            url="https://example.com",
            html="<html></html>",
            score=85,
            route=RouteDecision.FAST,
            route_reason="High score",
            metadata={},
            analysis={},
            original_timestamp=1000.0,
            router_timestamp=1001.0,
        )

        assert msg.url == "https://example.com"
        assert msg.score == 85
        assert msg.route == RouteDecision.FAST


class TestConvenienceFunctions:
    """편의 함수 테스트"""

    def test_calculate_score(self):
        """calculate_score 함수 테스트"""
        from src.router.scoring import calculate_score

        html = "<html><body><article><p>Content for testing.</p></article></body></html>"
        result = calculate_score(html)

        assert hasattr(result, 'score')
        assert hasattr(result, 'route')

    def test_should_use_browser(self):
        """should_use_browser 함수 테스트"""
        from src.router.scoring import should_use_browser

        # 정적 페이지
        static_html = """
        <html><body>
            <article><p>Long static content for testing purposes.
            This needs to be substantial enough for analysis.</p></article>
        </body></html>
        """
        assert should_use_browser(static_html) == False

        # SPA 페이지
        spa_html = """
        <html><body>
            <div id="root"></div>
            <script src="bundle.js"></script>
            <noscript>Enable JavaScript</noscript>
        </body></html>
        """
        assert should_use_browser(spa_html) == True


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

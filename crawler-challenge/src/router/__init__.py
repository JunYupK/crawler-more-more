"""
Router Module - Layer 2: Intelligent Routing
=============================================

HTML 분석 후 처리 경로 결정
- Score >= 80: process.fast (정적 페이지 → BeautifulSoup)
- Score < 80: process.rich (동적 페이지 → Crawl4AI)

Components:
- smart_router: 지능형 라우터 (Kafka Consumer/Producer)
- page_analyzer: 정적/동적 페이지 판별
- scoring: 점수 계산 로직
"""

from .smart_router import SmartRouter
from .page_analyzer import PageAnalyzer
from .scoring import StaticScoreCalculator

__all__ = [
    "SmartRouter",
    "PageAnalyzer",
    "StaticScoreCalculator",
]

"""
Processor Module - Layer 3: Parallel Processing
================================================

데이터 정제 및 Markdown 변환 워커들

Components:
- fast_worker: BeautifulSoup/lxml 기반 정적 페이지 처리 (70% 예상)
- rich_worker: Crawl4AI/Playwright 기반 동적 페이지 처리 (30% 예상)
- base_worker: 공통 워커 인터페이스
"""

from .base_worker import BaseWorker
from .fast_worker import FastWorker
from .rich_worker import RichWorker

__all__ = [
    "BaseWorker",
    "FastWorker",
    "RichWorker",
]

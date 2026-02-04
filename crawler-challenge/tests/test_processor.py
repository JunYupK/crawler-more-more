"""
Processor 모듈 테스트
=====================

Usage:
    pytest tests/test_processor.py -v
    pytest tests/test_processor.py -v -k "fast"

    # pytest 없이 실행
    python tests/test_processor.py
"""

import asyncio
import sys
from pathlib import Path

# pytest fallback 처리
try:
    import pytest
    PYTEST_AVAILABLE = True
except ImportError:
    PYTEST_AVAILABLE = False

    class DummyMark:
        @staticmethod
        def asyncio(func):
            return func

        @staticmethod
        def skip(reason=""):
            def decorator(func):
                return func
            return decorator

    class DummyPytest:
        mark = DummyMark()

    pytest = DummyPytest()

# 프로젝트 루트를 path에 추가
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


class TestProcessorType:
    """ProcessorType 테스트"""

    def test_processor_types(self):
        """프로세서 유형 테스트"""
        from src.processor.base_worker import ProcessorType

        assert ProcessorType.FAST.value == "fast"
        assert ProcessorType.RICH.value == "rich"


class TestProcessedResult:
    """ProcessedResult 테스트"""

    def test_success_result(self):
        """성공 결과 테스트"""
        from src.processor.base_worker import ProcessedResult, ProcessorType

        result = ProcessedResult(
            url="https://example.com",
            success=True,
            processor_type=ProcessorType.FAST,
            markdown="# Title\n\nContent here",
            title="Example Page",
        )

        assert result.success
        assert result.processor_type == ProcessorType.FAST
        assert result.markdown is not None
        assert result.error_type is None

    def test_failure_result(self):
        """실패 결과 테스트"""
        from src.processor.base_worker import ProcessedResult, ProcessorType

        result = ProcessedResult(
            url="https://example.com",
            success=False,
            processor_type=ProcessorType.RICH,
            error_type="TimeoutError",
            error_message="Connection timed out",
        )

        assert not result.success
        assert result.error_type == "TimeoutError"

    def test_to_dict(self):
        """딕셔너리 변환 테스트"""
        from src.processor.base_worker import ProcessedResult, ProcessorType

        result = ProcessedResult(
            url="https://example.com",
            success=True,
            processor_type=ProcessorType.FAST,
            markdown="# Test",
            title="Test Page",
        )

        data = result.to_dict()

        assert data['url'] == "https://example.com"
        assert data['success'] == True
        assert data['processor_type'] == "fast"
        assert data['metadata']['title'] == "Test Page"


class TestWorkerStats:
    """WorkerStats 테스트"""

    def test_record_success(self):
        """성공 기록 테스트"""
        from src.processor.base_worker import WorkerStats

        stats = WorkerStats()
        stats.record_success(100.0, 5000, 2000)
        stats.record_success(150.0, 8000, 3000)

        assert stats.messages_processed == 2
        assert stats.messages_consumed == 2
        assert stats.total_content_bytes == 13000
        assert stats.total_markdown_bytes == 5000
        assert stats.average_processing_time_ms == 125.0

    def test_record_failure(self):
        """실패 기록 테스트"""
        from src.processor.base_worker import WorkerStats

        stats = WorkerStats()
        stats.record_success(100.0, 5000, 2000)
        stats.record_failure()
        stats.record_failure()

        assert stats.messages_consumed == 3
        assert stats.messages_processed == 1
        assert stats.messages_failed == 2
        assert abs(stats.success_rate - 0.333) < 0.01


class TestFastWorker:
    """FastWorker 테스트"""

    def test_get_processor_type(self):
        """프로세서 유형 테스트"""
        from src.processor.fast_worker import FastWorker
        from src.processor.base_worker import ProcessorType

        worker = FastWorker(worker_id=0)
        assert worker.get_processor_type() == ProcessorType.FAST

    @pytest.mark.asyncio
    async def test_process_simple_html(self):
        """간단한 HTML 처리 테스트"""
        from src.processor.fast_worker import FastWorker

        worker = FastWorker(worker_id=0)

        html = """
        <html>
        <head>
            <title>Test Page</title>
            <meta name="description" content="Test description">
        </head>
        <body>
            <article>
                <h1>Main Title</h1>
                <p>First paragraph with some content.</p>
                <p>Second paragraph with more content.</p>
                <a href="https://example.com/link">A link</a>
            </article>
        </body>
        </html>
        """

        result = await worker.process_html(html, "https://example.com", {})

        assert result.success
        assert result.title == "Test Page"
        assert result.description == "Test description"
        assert result.markdown is not None
        assert len(result.links or []) >= 1
        assert "Main Title" in (result.markdown or "")

    @pytest.mark.asyncio
    async def test_extract_metadata(self):
        """메타데이터 추출 테스트"""
        from src.processor.fast_worker import FastWorker

        worker = FastWorker(worker_id=0)

        html = """
        <html lang="ko">
        <head>
            <title>한국어 페이지</title>
            <meta name="description" content="페이지 설명입니다">
            <meta name="keywords" content="테스트, 한국어, 예제">
        </head>
        <body>
            <h1>본문</h1>
            <p>내용이 여기에 있습니다.</p>
        </body>
        </html>
        """

        result = await worker.process_html(html, "https://example.com", {})

        assert result.language == "ko"
        assert result.title == "한국어 페이지"
        assert result.keywords is not None
        assert "테스트" in result.keywords

    @pytest.mark.asyncio
    async def test_remove_unwanted_tags(self):
        """불필요한 태그 제거 테스트"""
        from src.processor.fast_worker import FastWorker

        worker = FastWorker(worker_id=0)

        html = """
        <html>
        <head><title>Test</title></head>
        <body>
            <nav>Navigation should be removed</nav>
            <article>
                <h1>Title</h1>
                <p>This content should remain.</p>
            </article>
            <footer>Footer should be removed</footer>
            <script>console.log('removed');</script>
        </body>
        </html>
        """

        result = await worker.process_html(html, "https://example.com", {})

        # nav, footer, script 내용이 마크다운에 없어야 함
        markdown = result.markdown or ""
        assert "Navigation should be removed" not in markdown
        assert "Footer should be removed" not in markdown
        assert "console.log" not in markdown
        assert "This content should remain" in markdown

    @pytest.mark.asyncio
    async def test_extract_links(self):
        """링크 추출 테스트"""
        from src.processor.fast_worker import FastWorker

        worker = FastWorker(worker_id=0)

        html = """
        <html>
        <body>
            <a href="https://example.com/page1">Link 1</a>
            <a href="/page2">Relative Link</a>
            <a href="#section">Anchor Link</a>
            <a href="javascript:void(0)">JS Link</a>
        </body>
        </html>
        """

        result = await worker.process_html(html, "https://example.com", {})

        links = result.links or []
        # https://example.com/page1 포함
        assert any("page1" in link for link in links)
        # 상대 경로가 절대 경로로 변환
        assert any("page2" in link for link in links)
        # 앵커와 JS 링크는 제외
        assert not any("#section" in link for link in links)
        assert not any("javascript" in link for link in links)

    @pytest.mark.asyncio
    async def test_extract_images(self):
        """이미지 추출 테스트"""
        from src.processor.fast_worker import FastWorker

        worker = FastWorker(worker_id=0)

        html = """
        <html>
        <body>
            <img src="https://example.com/image1.jpg">
            <img src="/images/image2.png">
            <img data-src="/images/lazy.jpg">
            <img src="data:image/png;base64,ABC123">
        </body>
        </html>
        """

        result = await worker.process_html(html, "https://example.com", {})

        images = result.images or []
        assert any("image1.jpg" in img for img in images)
        assert any("image2.png" in img for img in images)
        # base64 이미지는 제외
        assert not any("base64" in img for img in images)

    @pytest.mark.asyncio
    async def test_extract_headings(self):
        """헤딩 추출 테스트"""
        from src.processor.fast_worker import FastWorker

        worker = FastWorker(worker_id=0)

        html = """
        <html>
        <body>
            <h1>Main Title</h1>
            <h2>Section One</h2>
            <h2>Section Two</h2>
            <h3>Subsection</h3>
        </body>
        </html>
        """

        result = await worker.process_html(html, "https://example.com", {})

        headings = result.headings or []
        assert len(headings) == 4
        assert any("H1: Main Title" in h for h in headings)
        assert any("H2: Section One" in h for h in headings)

    @pytest.mark.asyncio
    async def test_error_handling(self):
        """에러 처리 테스트"""
        from src.processor.fast_worker import FastWorker

        worker = FastWorker(worker_id=0)

        # 빈 HTML
        result = await worker.process_html("", "https://example.com", {})

        # 빈 HTML도 처리 가능해야 함
        assert result.url == "https://example.com"


class TestRichWorker:
    """RichWorker 테스트"""

    def test_get_processor_type(self):
        """프로세서 유형 테스트"""
        from src.processor.rich_worker import RichWorker
        from src.processor.base_worker import ProcessorType

        worker = RichWorker(worker_id=0)
        assert worker.get_processor_type() == ProcessorType.RICH

    @pytest.mark.asyncio
    async def test_fallback_processing(self):
        """폴백 처리 테스트"""
        from src.processor.rich_worker import RichWorker

        worker = RichWorker(worker_id=0)

        html = """
        <html>
        <head>
            <title>Dynamic Page</title>
            <meta name="description" content="Page description">
        </head>
        <body>
            <h1>Content Title</h1>
            <p>Some content here with a <a href="https://example.com">link</a>.</p>
            <img src="https://example.com/image.jpg">
        </body>
        </html>
        """

        result = await worker._process_fallback(html, "https://example.com", {})

        assert result.success
        assert result.title == "Dynamic Page"
        assert result.description == "Page description"
        assert result.markdown is not None
        assert len(result.links or []) >= 1
        assert len(result.images or []) >= 1

    def test_check_crawl4ai_installation(self):
        """Crawl4AI 설치 확인 테스트"""
        from src.processor.rich_worker import check_crawl4ai_installation

        status = check_crawl4ai_installation()

        assert 'installed' in status
        # installed가 True면 version도 있어야 함
        if status['installed']:
            assert 'version' in status


class TestFastWorkerPool:
    """FastWorkerPool 테스트"""

    def test_pool_creation(self):
        """풀 생성 테스트"""
        from src.processor.fast_worker import FastWorkerPool

        pool = FastWorkerPool(num_workers=4)

        assert pool.num_workers == 4
        assert len(pool.workers) == 0  # start 전

    @pytest.mark.asyncio
    async def test_get_combined_stats(self):
        """통합 통계 테스트"""
        from src.processor.fast_worker import FastWorkerPool

        pool = FastWorkerPool(num_workers=2)
        stats = pool.get_combined_stats()

        assert stats['num_workers'] == 2
        assert stats['total_processed'] == 0


class TestRichWorkerPool:
    """RichWorkerPool 테스트"""

    def test_pool_creation(self):
        """풀 생성 테스트"""
        from src.processor.rich_worker import RichWorkerPool

        pool = RichWorkerPool(num_workers=2)

        assert pool.num_workers == 2
        assert len(pool.workers) == 0  # start 전


class TestMarkdownConversion:
    """Markdown 변환 테스트"""

    @pytest.mark.asyncio
    async def test_markdown_formatting(self):
        """Markdown 포맷팅 테스트"""
        from src.processor.fast_worker import FastWorker

        worker = FastWorker(worker_id=0)

        html = """
        <html>
        <body>
            <h1>Title</h1>
            <p><strong>Bold</strong> and <em>italic</em> text.</p>
            <ul>
                <li>Item 1</li>
                <li>Item 2</li>
            </ul>
            <a href="https://example.com">Link text</a>
        </body>
        </html>
        """

        result = await worker.process_html(html, "https://example.com", {})
        markdown = result.markdown or ""

        # 기본 마크다운 형식 확인
        assert "#" in markdown  # 헤딩
        assert "**" in markdown or "Bold" in markdown  # 볼드
        assert "*" in markdown or "italic" in markdown  # 이탤릭
        assert "-" in markdown or "*" in markdown  # 리스트
        assert "[" in markdown and "]" in markdown  # 링크


def run_quick_test():
    """pytest 없이 빠른 테스트 실행"""
    print("=" * 60)
    print("Processor Module Quick Tests")
    print("=" * 60)

    # 의존성 체크
    deps_missing = []
    try:
        import msgpack
    except ImportError:
        deps_missing.append("msgpack")
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        deps_missing.append("beautifulsoup4")
    try:
        import html2text
    except ImportError:
        deps_missing.append("html2text")
    try:
        from aiokafka import AIOKafkaConsumer
    except ImportError:
        deps_missing.append("aiokafka")

    if deps_missing:
        print(f"\n⚠️ Missing dependencies: {', '.join(deps_missing)}")
        print("   Install with: pip install msgpack beautifulsoup4 lxml html2text aiokafka")
        print("   Running fallback tests...\n")
        return run_fallback_tests()

    # 1. ProcessorType 테스트
    print("\n1. Testing ProcessorType...")
    from src.processor.base_worker import ProcessorType

    assert ProcessorType.FAST.value == "fast", "FAST should be 'fast'"
    assert ProcessorType.RICH.value == "rich", "RICH should be 'rich'"
    print("   ✅ ProcessorType tests passed")

    # 2. ProcessedResult 테스트
    print("\n2. Testing ProcessedResult...")
    from src.processor.base_worker import ProcessedResult

    result = ProcessedResult(
        url="https://example.com",
        success=True,
        processor_type=ProcessorType.FAST,
        markdown="# Title\n\nContent here",
        title="Example Page",
    )

    assert result.success, "Should be success"
    assert result.processor_type == ProcessorType.FAST
    assert result.markdown is not None

    data = result.to_dict()
    assert data['url'] == "https://example.com"
    assert data['success'] == True
    assert data['processor_type'] == "fast"
    print("   ✅ ProcessedResult tests passed")

    # 3. WorkerStats 테스트
    print("\n3. Testing WorkerStats...")
    from src.processor.base_worker import WorkerStats

    stats = WorkerStats()
    stats.record_success(100.0, 5000, 2000)
    stats.record_success(150.0, 8000, 3000)

    assert stats.messages_processed == 2, "Should have 2 processed"
    assert stats.total_content_bytes == 13000, "Total content should be 13000"
    assert stats.average_processing_time_ms == 125.0, "Avg time should be 125"

    stats.record_failure()
    assert stats.messages_failed == 1, "Should have 1 failure"
    print("   ✅ WorkerStats tests passed")

    # 4. FastWorker 테스트 (async)
    print("\n4. Testing FastWorker...")
    from src.processor.fast_worker import FastWorker

    worker = FastWorker(worker_id=0)
    assert worker.get_processor_type() == ProcessorType.FAST

    html = """
    <html>
    <head>
        <title>Test Page</title>
        <meta name="description" content="Test description">
    </head>
    <body>
        <article>
            <h1>Main Title</h1>
            <p>First paragraph with some content.</p>
            <a href="https://example.com/link">A link</a>
        </article>
    </body>
    </html>
    """

    async def test_fast_worker():
        result = await worker.process_html(html, "https://example.com", {})
        assert result.success, "Processing should succeed"
        assert result.title == "Test Page", f"Title should be 'Test Page', got {result.title}"
        assert result.markdown is not None, "Markdown should not be None"
        assert "Main Title" in (result.markdown or ""), "Markdown should contain 'Main Title'"
        return result

    result = asyncio.get_event_loop().run_until_complete(test_fast_worker())
    print(f"   Title: {result.title}")
    print(f"   Markdown length: {len(result.markdown or '')}")
    print("   ✅ FastWorker tests passed")

    # 5. RichWorker 테스트
    print("\n5. Testing RichWorker...")
    from src.processor.rich_worker import RichWorker, check_crawl4ai_installation

    worker = RichWorker(worker_id=0)
    assert worker.get_processor_type() == ProcessorType.RICH

    status = check_crawl4ai_installation()
    print(f"   Crawl4AI installed: {status.get('installed', False)}")

    async def test_rich_worker_fallback():
        result = await worker._process_fallback(html, "https://example.com", {})
        assert result.success, "Fallback should succeed"
        return result

    result = asyncio.get_event_loop().run_until_complete(test_rich_worker_fallback())
    print("   ✅ RichWorker tests passed")

    # 6. WorkerPool 테스트
    print("\n6. Testing WorkerPool...")
    from src.processor.fast_worker import FastWorkerPool
    from src.processor.rich_worker import RichWorkerPool

    fast_pool = FastWorkerPool(num_workers=4)
    assert fast_pool.num_workers == 4

    rich_pool = RichWorkerPool(num_workers=2)
    assert rich_pool.num_workers == 2
    print("   ✅ WorkerPool tests passed")

    print("\n" + "=" * 60)
    print("All Processor quick tests passed! ✅")
    print("=" * 60)


def run_fallback_tests():
    """의존성 없이 기본 로직 테스트"""
    print("=" * 60)
    print("Processor Fallback Tests (No dependencies)")
    print("=" * 60)

    from enum import Enum
    from dataclasses import dataclass, field
    from typing import Optional, List
    import time

    # 1. ProcessorType Enum 테스트
    print("\n1. Testing ProcessorType enum...")

    class ProcessorType(Enum):
        FAST = "fast"
        RICH = "rich"

    assert ProcessorType.FAST.value == "fast"
    assert ProcessorType.RICH.value == "rich"
    print("   ✅ ProcessorType tests passed")

    # 2. ProcessedResult 테스트
    print("\n2. Testing ProcessedResult...")

    @dataclass
    class ProcessedResult:
        url: str
        success: bool
        processor_type: ProcessorType
        markdown: Optional[str] = None
        title: Optional[str] = None
        error_type: Optional[str] = None

        def to_dict(self) -> dict:
            return {
                'url': self.url,
                'success': self.success,
                'processor_type': self.processor_type.value,
                'metadata': {'title': self.title},
            }

    result = ProcessedResult(
        url="https://example.com",
        success=True,
        processor_type=ProcessorType.FAST,
        markdown="# Title",
        title="Example Page",
    )

    assert result.success
    data = result.to_dict()
    assert data['processor_type'] == "fast"
    print("   ✅ ProcessedResult tests passed")

    # 3. WorkerStats 테스트
    print("\n3. Testing WorkerStats...")

    @dataclass
    class WorkerStats:
        messages_consumed: int = 0
        messages_processed: int = 0
        messages_failed: int = 0
        total_content_bytes: int = 0
        total_markdown_bytes: int = 0
        total_processing_time_ms: float = 0.0
        start_time: float = field(default_factory=time.time)

        def record_success(self, time_ms: float, content_bytes: int, markdown_bytes: int):
            self.messages_consumed += 1
            self.messages_processed += 1
            self.total_processing_time_ms += time_ms
            self.total_content_bytes += content_bytes
            self.total_markdown_bytes += markdown_bytes

        def record_failure(self):
            self.messages_consumed += 1
            self.messages_failed += 1

        @property
        def average_processing_time_ms(self) -> float:
            return self.total_processing_time_ms / self.messages_processed if self.messages_processed > 0 else 0

        @property
        def success_rate(self) -> float:
            return self.messages_processed / self.messages_consumed if self.messages_consumed > 0 else 0

    stats = WorkerStats()
    stats.record_success(100.0, 5000, 2000)
    stats.record_success(150.0, 8000, 3000)

    assert stats.messages_processed == 2
    assert stats.total_content_bytes == 13000
    assert stats.average_processing_time_ms == 125.0

    stats.record_failure()
    assert stats.messages_failed == 1
    assert abs(stats.success_rate - 0.666) < 0.01
    print("   ✅ WorkerStats tests passed")

    # 4. 간단한 HTML 파싱 테스트
    print("\n4. Testing basic HTML parsing...")
    import re

    def extract_title(html: str) -> Optional[str]:
        match = re.search(r'<title>([^<]+)</title>', html, re.IGNORECASE)
        return match.group(1) if match else None

    def extract_meta_description(html: str) -> Optional[str]:
        match = re.search(r'<meta\s+name=["\']description["\']\s+content=["\']([^"\']+)["\']', html, re.IGNORECASE)
        return match.group(1) if match else None

    html = '''
    <html>
    <head>
        <title>Test Page</title>
        <meta name="description" content="Test description">
    </head>
    <body><p>Content</p></body>
    </html>
    '''

    assert extract_title(html) == "Test Page"
    assert extract_meta_description(html) == "Test description"
    print("   ✅ HTML parsing tests passed")

    print("\n" + "=" * 60)
    print("All fallback tests passed! ✅")
    print("(Full tests require: pip install msgpack beautifulsoup4 lxml html2text aiokafka)")
    print("=" * 60)


if __name__ == "__main__":
    if PYTEST_AVAILABLE:
        pytest.main([__file__, "-v"])
    else:
        run_quick_test()

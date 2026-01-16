"""
Processor 모듈 테스트
=====================

Usage:
    pytest tests/test_processor.py -v
    pytest tests/test_processor.py -v -k "fast"
"""

import pytest
import asyncio
import sys
from pathlib import Path

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


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

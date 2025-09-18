import asyncio
import time
from typing import List, Dict, Any
import aiohttp
from bs4 import BeautifulSoup
from config.settings import MAX_CONCURRENT_REQUESTS, REQUEST_TIMEOUT
from core.heavy_processing import heavy_html_processing


class WebCrawler:
    def __init__(self, metrics_monitor=None, database_manager=None):
        self.semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
        self.session = None
        self.results = []
        self.metrics_monitor = metrics_monitor
        self.database_manager = database_manager
        
    async def __aenter__(self):
        connector = aiohttp.TCPConnector(
            limit=100,  # Total connection limit
            limit_per_host=10,  # Per-host connection limit
            ttl_dns_cache=300,  # DNS cache TTL (5 minutes)
            use_dns_cache=True,
            keepalive_timeout=60,  # Keep connections alive for 60s
            enable_cleanup_closed=True
        )
        
        self.session = aiohttp.ClientSession(
            connector=connector,
            timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT),
            headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate',
                'Connection': 'keep-alive'
            }
        )
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
    
    async def fetch_page(self, url: str) -> Dict[str, Any]:
        if self.metrics_monitor:
            self.metrics_monitor.increment_active_tasks()

        async with self.semaphore:
            try:
                async with self.session.get(url) as response:
                    content = await response.text()

                    # Heavy CPU processing to trigger GIL bottleneck
                    processing_result = heavy_html_processing(content, url)

                    # DB에 저장 (배치로 처리됨)
                    if self.database_manager:
                        self.database_manager.add_to_batch(url, content)

                    if self.metrics_monitor:
                        self.metrics_monitor.increment_pages()

                    return {
                        'url': url,
                        'status': response.status,
                        'title': processing_result['title'],
                        'content_length': processing_result['content_length'],
                        'word_count': processing_result['word_count'],
                        'unique_words': processing_result['unique_words'],
                        'emails_found': processing_result['emails_found'],
                        'links_found': processing_result['links_found'],
                        'content_hash': processing_result['content_hash'][:8],  # First 8 chars
                        'complexity_score': processing_result['processing_complexity_score'],
                        'success': True
                    }
            except Exception as e:
                return {
                    'url': url,
                    'status': None,
                    'title': None,
                    'content_length': 0,
                    'error': str(e),
                    'success': False
                }
            finally:
                if self.metrics_monitor:
                    self.metrics_monitor.decrement_active_tasks()
    
    async def crawl_urls(self, urls: List[str]) -> List[Dict[str, Any]]:
        start_time = time.time()
        
        tasks = [self.fetch_page(url) for url in urls]
        results = await asyncio.gather(*tasks)
        
        end_time = time.time()
        duration = end_time - start_time
        pages_per_second = len(urls) / duration if duration > 0 else 0
        
        print(f"Crawled {len(urls)} pages in {duration:.2f}s ({pages_per_second:.2f} pages/sec)")
        
        return results


def get_test_urls() -> List[str]:
    return [
        "https://jsonplaceholder.typicode.com/posts/1",
        "https://jsonplaceholder.typicode.com/posts/2",
        "https://jsonplaceholder.typicode.com/users/1",
        "https://jsonplaceholder.typicode.com/users/2",
        "https://jsonplaceholder.typicode.com/albums/1"
    ]
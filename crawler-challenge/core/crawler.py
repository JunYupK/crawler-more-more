import asyncio
import time
from typing import List, Dict, Any
import aiohttp
from bs4 import BeautifulSoup
from config.settings import MAX_CONCURRENT_REQUESTS, REQUEST_TIMEOUT


class WebCrawler:
    def __init__(self, metrics_monitor=None):
        self.semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
        self.session = None
        self.results = []
        self.metrics_monitor = metrics_monitor
        
    async def __aenter__(self):
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
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
                    soup = BeautifulSoup(content, 'html.parser')
                    
                    title = soup.find('title')
                    title_text = title.get_text().strip() if title else "No title"
                    
                    if self.metrics_monitor:
                        self.metrics_monitor.increment_pages()
                    
                    return {
                        'url': url,
                        'status': response.status,
                        'title': title_text,
                        'content_length': len(content),
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
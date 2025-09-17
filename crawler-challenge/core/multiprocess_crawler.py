import asyncio
import time
from typing import List, Dict, Any
from concurrent.futures import ProcessPoolExecutor
import aiohttp
from config.settings import MAX_CONCURRENT_REQUESTS, REQUEST_TIMEOUT
from core.heavy_processing import heavy_html_processing


def process_html_worker(url: str, content: str) -> Dict[str, Any]:
    """
    Worker function for multiprocessing HTML processing
    This runs in a separate process to bypass GIL
    """
    try:
        result = heavy_html_processing(content, url)
        result['success'] = True
        return result
    except Exception as e:
        return {
            'url': url,
            'status': None,
            'title': None,
            'content_length': 0,
            'error': str(e),
            'success': False
        }


class MultiprocessWebCrawler:
    def __init__(self, metrics_monitor=None, max_workers=None):
        self.semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
        self.session = None
        self.results = []
        self.metrics_monitor = metrics_monitor
        self.max_workers = max_workers or 4  # Default to 4 processes
        self.executor = None
        
    async def __aenter__(self):
        # Setup HTTP session for extreme scale
        connector = aiohttp.TCPConnector(
            limit=500,  # Increased for extreme scale
            limit_per_host=50,  # More connections per host for scale
            ttl_dns_cache=600,  # Longer DNS cache
            use_dns_cache=True,
            keepalive_timeout=120,  # Longer keep-alive
            enable_cleanup_closed=True,
            force_close=False  # Reuse connections
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
        
        # Setup process pool
        self.executor = ProcessPoolExecutor(max_workers=self.max_workers)
        
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
        if self.executor:
            self.executor.shutdown(wait=True)
    
    async def fetch_page(self, url: str) -> Dict[str, Any]:
        if self.metrics_monitor:
            self.metrics_monitor.increment_active_tasks()
        
        async with self.semaphore:
            try:
                # Phase 1: Network I/O (main process, async)
                async with self.session.get(url) as response:
                    content = await response.text()
                    status = response.status
                
                # Phase 2: CPU processing (separate process, sync)
                loop = asyncio.get_event_loop()
                processing_result = await loop.run_in_executor(
                    self.executor,
                    process_html_worker,
                    url,
                    content
                )
                
                # Combine network and processing results
                if processing_result['success']:
                    processing_result['status'] = status
                    
                    if self.metrics_monitor:
                        self.metrics_monitor.increment_pages()
                    
                    return {
                        'url': url,
                        'status': status,
                        'title': processing_result['title'],
                        'content_length': processing_result['content_length'],
                        'word_count': processing_result['word_count'],
                        'unique_words': processing_result['unique_words'],
                        'emails_found': processing_result['emails_found'],
                        'links_found': processing_result['links_found'],
                        'content_hash': processing_result['content_hash'][:8],
                        'complexity_score': processing_result['processing_complexity_score'],
                        'success': True
                    }
                else:
                    if self.metrics_monitor:
                        self.metrics_monitor.increment_errors()
                    return processing_result
                    
            except Exception as e:
                if self.metrics_monitor:
                    self.metrics_monitor.increment_errors()
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
        
        print(f"Starting multiprocess crawling with {self.max_workers} worker processes...")
        
        tasks = [self.fetch_page(url) for url in urls]
        results = await asyncio.gather(*tasks)
        
        end_time = time.time()
        duration = end_time - start_time
        pages_per_second = len(urls) / duration if duration > 0 else 0
        
        print(f"Crawled {len(urls)} pages in {duration:.2f}s ({pages_per_second:.2f} pages/sec)")
        print(f"Used {self.max_workers} worker processes for CPU-intensive tasks")
        
        return results
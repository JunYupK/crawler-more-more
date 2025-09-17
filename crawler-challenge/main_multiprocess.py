import asyncio
from core.multiprocess_crawler import MultiprocessWebCrawler
from core.tranco import get_tranco_urls
from core.crawler import get_test_urls
from monitoring.metrics import MetricsMonitor


async def main():
    # Test with Tranco Top 50 URLs
    try:
        urls = await get_tranco_urls()
        print(f"Testing multiprocess crawler with {len(urls)} Tranco URLs...")
    except Exception as e:
        print(f"Failed to load Tranco URLs: {e}")
        print("Falling back to test URLs...")
        urls = get_test_urls() * 10  # 50 URLs for testing
        print(f"Testing multiprocess crawler with {len(urls)} URLs...")
    
    metrics = MetricsMonitor()
    await metrics.start_monitoring()
    
    try:
        # Test with different worker counts
        worker_counts = [2, 4]  # Test with 2 and 4 worker processes
        
        for workers in worker_counts:
            print(f"\n{'='*60}")
            print(f"Testing with {workers} worker processes")
            print(f"{'='*60}")
            
            async with MultiprocessWebCrawler(metrics, max_workers=workers) as crawler:
                results = await crawler.crawl_urls(urls)
                
                print(f"\nResults with {workers} workers:")
                successful = sum(1 for r in results if r['success'])
                print(f"SUCCESS: {successful}/{len(results)} pages crawled successfully")
                
                # Show some sample results
                for result in results[:3]:
                    if result['success']:
                        print(f"  {result['url']} - {result['status']} - {result['title'][:30]}...")
                        print(f"    Words: {result.get('word_count', 0)}, Links: {result.get('links_found', 0)}, "
                              f"Hash: {result.get('content_hash', 'N/A')}")
                    else:
                        print(f"  {result['url']} - Error: {result['error']}")
                
                if len(results) > 3:
                    print(f"  ... and {len(results) - 3} more")
            
            # Reset metrics for next test
            metrics.total_pages = 0
            metrics.start_time = time.time()
            
            if workers < max(worker_counts):
                print("\nWaiting 3 seconds before next test...")
                await asyncio.sleep(3)
    
    finally:
        await metrics.stop_monitoring()


if __name__ == "__main__":
    import time
    asyncio.run(main())
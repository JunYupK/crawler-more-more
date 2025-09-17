import asyncio
from core.crawler import WebCrawler, get_test_urls
from core.tranco import get_tranco_urls
from monitoring.metrics import MetricsMonitor


async def main():
    # Test with Tranco Top 50 URLs
    try:
        urls = await get_tranco_urls()
        print(f"Testing crawler with {len(urls)} Tranco URLs...")
    except Exception as e:
        print(f"Failed to load Tranco URLs: {e}")
        print("Falling back to test URLs...")
        urls = get_test_urls() * 10  # 50 URLs for testing
        print(f"Testing crawler with {len(urls)} URLs...")
    
    metrics = MetricsMonitor()
    await metrics.start_monitoring()
    
    try:
        async with WebCrawler(metrics) as crawler:
            results = await crawler.crawl_urls(urls)
            
            print("\nResults:")
            successful = sum(1 for r in results if r['success'])
            print(f"SUCCESS: {successful}/{len(results)} pages crawled successfully")
            
            # Show some sample results
            for result in results[:5]:
                if result['success']:
                    print(f"  {result['url']} - {result['status']} - {result['title'][:50]}...")
                else:
                    print(f"  {result['url']} - Error: {result['error']}")
            
            if len(results) > 5:
                print(f"  ... and {len(results) - 5} more")
    
    finally:
        await metrics.stop_monitoring()


if __name__ == "__main__":
    asyncio.run(main())
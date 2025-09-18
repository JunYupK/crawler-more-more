import asyncio
from core.crawler import WebCrawler, get_test_urls
from core.tranco import get_tranco_urls
from monitoring.metrics import MetricsMonitor
from database import DatabaseManager


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

    # Database setup
    db_manager = DatabaseManager()

    try:
        async with WebCrawler(metrics, db_manager) as crawler:
            results = await crawler.crawl_urls(urls)

            print("\nResults:")
            successful = sum(1 for r in results if r['success'])
            print(f"SUCCESS: {successful}/{len(results)} pages crawled successfully")

            # Show some sample results with CPU processing info
            for result in results[:5]:
                if result['success']:
                    print(f"  {result['url']} - {result['status']} - {result['title'][:30]}...")
                    print(f"    Words: {result.get('word_count', 0)}, Unique: {result.get('unique_words', 0)}, "
                          f"Links: {result.get('links_found', 0)}, Hash: {result.get('content_hash', 'N/A')}")
                else:
                    print(f"  {result['url']} - Error: {result['error']}")

            if len(results) > 5:
                print(f"  ... and {len(results) - 5} more")

            # DB 통계 출력
            print(f"\nDatabase Statistics:")
            print(f"Total pages in DB: {db_manager.get_crawled_count()}")

            domain_stats = db_manager.get_domain_stats()
            if domain_stats:
                print("Domain breakdown:")
                for stat in domain_stats[:5]:
                    print(f"  {stat['domain']}: {stat['count']} pages (avg content: {stat['avg_content_length']:.0f} chars)")

    finally:
        await metrics.stop_monitoring()
        # DB 연결 정리
        db_manager.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
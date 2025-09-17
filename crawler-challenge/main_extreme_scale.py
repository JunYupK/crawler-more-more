import asyncio
import time
import random
from core.multiprocess_crawler import MultiprocessWebCrawler
from core.tranco import get_tranco_urls
from monitoring.metrics import MetricsMonitor


async def sustained_crawling_test(urls, duration_minutes=10):
    """
    Sustained crawling test for extreme scale
    """
    print(f"Starting {duration_minutes}-minute sustained crawling test...")
    print(f"Target: Continuous crawling of {len(urls)} URLs with 200 concurrent requests")
    
    metrics = MetricsMonitor()
    await metrics.start_monitoring()
    
    start_time = time.time()
    end_time = start_time + (duration_minutes * 60)
    
    round_count = 0
    total_pages_crawled = 0
    total_errors = 0
    
    try:
        async with MultiprocessWebCrawler(metrics, max_workers=8) as crawler:
            while time.time() < end_time:
                round_count += 1
                round_start = time.time()
                
                # Shuffle URLs for variety in each round
                shuffled_urls = random.sample(urls, min(100, len(urls)))  # 100 URLs per round
                
                print(f"\n--- Round {round_count} ---")
                print(f"Crawling {len(shuffled_urls)} URLs...")
                
                results = await crawler.crawl_urls(shuffled_urls)
                
                # Count results
                successful = sum(1 for r in results if r['success'])
                errors = len(results) - successful
                
                total_pages_crawled += successful
                total_errors += errors
                
                round_time = time.time() - round_start
                round_pps = len(results) / round_time if round_time > 0 else 0
                
                print(f"Round {round_count} completed: {successful}/{len(results)} success, "
                      f"{round_pps:.2f} pages/sec")
                
                # Brief pause between rounds to avoid overwhelming
                await asyncio.sleep(2)
                
                remaining_time = end_time - time.time()
                if remaining_time <= 0:
                    break
                    
                print(f"Remaining time: {remaining_time/60:.1f} minutes")
    
    except Exception as e:
        print(f"Test interrupted: {e}")
    
    finally:
        await metrics.stop_monitoring()
        
        # Final statistics
        total_time = time.time() - start_time
        overall_pps = total_pages_crawled / total_time if total_time > 0 else 0
        error_rate = (total_errors / (total_pages_crawled + total_errors) * 100) if (total_pages_crawled + total_errors) > 0 else 0
        
        print(f"\n{'='*80}")
        print(f"EXTREME SCALE TEST COMPLETE")
        print(f"{'='*80}")
        print(f"Duration: {total_time/60:.1f} minutes")
        print(f"Rounds completed: {round_count}")
        print(f"Total pages crawled: {total_pages_crawled}")
        print(f"Total errors: {total_errors}")
        print(f"Overall pages/sec: {overall_pps:.2f}")
        print(f"Error rate: {error_rate:.2f}%")
        print(f"Memory usage: {metrics.get_memory_usage()}")
        
        return {
            'duration_minutes': total_time / 60,
            'rounds': round_count,
            'total_pages': total_pages_crawled,
            'total_errors': total_errors,
            'overall_pps': overall_pps,
            'error_rate': error_rate
        }


async def main():
    print("EXTREME SCALE CRAWLER TEST")
    print("="*50)
    
    # Load 500 URLs
    try:
        urls = await get_tranco_urls(500)
        print(f"Loaded {len(urls)} URLs for extreme scale test")
    except Exception as e:
        print(f"Failed to load Tranco URLs: {e}")
        print("This test requires a large URL dataset")
        return
    
    # Show system info
    import psutil
    memory = psutil.virtual_memory()
    cpu_count = psutil.cpu_count()
    
    print(f"System specs:")
    print(f"- CPU cores: {cpu_count}")
    print(f"- RAM: {memory.total / (1024**3):.1f} GB")
    print(f"- Concurrent requests: 200")
    print(f"- Worker processes: 8")
    
    # Run sustained test (shortened for stability)
    results = await sustained_crawling_test(urls, duration_minutes=3)
    
    # Analyze Python limits
    print(f"\nPYTHON SCALABILITY ANALYSIS:")
    print(f"- Peak performance: {results['overall_pps']:.2f} pages/sec")
    print(f"- Stability: {100-results['error_rate']:.1f}% success rate")
    print(f"- Resource efficiency: {results['total_pages']/results['duration_minutes']:.0f} pages/minute")
    
    if results['overall_pps'] < 28:
        print(f"- GIL impact: Network I/O remains primary bottleneck")
        print(f"- Recommendation: Consider async/await optimizations or alternative languages")
    else:
        print(f"- SUCCESS: Python achieved target performance!")


if __name__ == "__main__":
    asyncio.run(main())
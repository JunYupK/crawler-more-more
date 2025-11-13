TARGET_PAGES_PER_SECOND = 28
MAX_CONCURRENT_REQUESTS = 200  # Aggressive optimization
REDIS_HOST = "localhost"
REQUEST_TIMEOUT = 10  # Reduced from 30s for faster failures

# Aggressive Optimization Settings
GLOBAL_SEMAPHORE_LIMIT = 200  # Increased from 20
TCP_CONNECTOR_LIMIT = 300  # Increased from 100
TCP_CONNECTOR_LIMIT_PER_HOST = 20  # Increased from 5
DEFAULT_CRAWL_DELAY = 0.5  # Reduced from 2s
WORKER_THREADS = 16  # Increased from 4
BATCH_SIZE = 100  # Increased from 25
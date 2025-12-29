TARGET_PAGES_PER_SECOND = 28
MAX_CONCURRENT_REQUESTS = 300  # More aggressive optimization
REDIS_HOST = "localhost"
REQUEST_TIMEOUT = 15  # Increased to 15s for more tolerance

# More Aggressive Optimization Settings for Random Sharding
GLOBAL_SEMAPHORE_LIMIT = 300  # Increased from 200
TCP_CONNECTOR_LIMIT = 500  # Increased from 300
TCP_CONNECTOR_LIMIT_PER_HOST = 30  # Increased from 20
DEFAULT_CRAWL_DELAY = 0.0  # Removed delay (not used in random sharding)
WORKER_THREADS = 16  # Increased from 4
BATCH_SIZE = 100  # Increased from 25
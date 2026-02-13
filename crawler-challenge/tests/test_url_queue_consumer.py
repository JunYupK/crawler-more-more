import sys
import types


# aiokafka가 테스트 환경에 없을 수 있어 import stub 제공
if 'aiokafka' not in sys.modules:
    sys.modules['aiokafka'] = types.SimpleNamespace(AIOKafkaConsumer=object)

from src.managers.url_queue_consumer import URLQueueConsumer


class FakeRedisClient:
    def __init__(self):
        self.sets = {}
        self.hashes = {}
        self.zadd_calls = []
        self.hincr_calls = []

    def sismember(self, key, value):
        return value in self.sets.get(key, set())

    def hget(self, key, field):
        return self.hashes.get(key, {}).get(field)

    def zadd(self, key, mapping):
        self.zadd_calls.append((key, mapping))

    def hincrby(self, key, field, amount):
        self.hincr_calls.append((key, field, amount))


class FakeQueueManager:
    def __init__(self):
        self.num_shards = 1
        self.redis_clients = [FakeRedisClient()]
        self.completed_template = 'completed:{shard}'
        self.queue_templates = {'priority_low': 'queue:low:{shard}'}
        self._pending = False
        self.pending_check_calls = 0

    def _get_url_hash(self, url):
        return f'hash:{url}'

    def get_shard_for_url(self, url):
        return 0

    def is_url_hash_pending(self, url_hash):
        self.pending_check_calls += 1
        return self._pending

    def get_queue_stats(self):
        return {'total_pending': 0}


def _build_consumer():
    consumer = URLQueueConsumer.__new__(URLQueueConsumer)
    consumer._queue_manager = FakeQueueManager()
    consumer._stats = {
        'messages_consumed': 0,
        'urls_received': 0,
        'urls_added': 0,
        'urls_skipped_dedup': 0,
        'urls_skipped_dedup_completed': 0,
        'urls_skipped_dedup_pending': 0,
        'urls_skipped_domain_limit': 0,
        'urls_skipped_queue_limit': 0,
        'urls_skipped_invalid': 0,
    }
    return consumer


def test_try_add_url_dedup_pending():
    consumer = _build_consumer()
    consumer._queue_manager._pending = True

    result = consumer._try_add_url('https://example.com/a', 'https://example.com')

    assert result == 'dedup_pending'
    client = consumer._queue_manager.redis_clients[0]
    assert client.zadd_calls == []
    assert client.hincr_calls == []


def test_process_message_tracks_pending_dedup_stats(monkeypatch):
    consumer = _build_consumer()

    monkeypatch.setattr(
        'src.managers.url_queue_consumer.URLExtractor.filter_and_normalize',
        lambda urls: urls,
    )
    consumer._queue_manager._pending = True

    import asyncio

    asyncio.run(consumer._process_message(
        {
            'source_url': 'https://source.example',
            'urls': ['https://example.com/a'],
        }
    ))

    assert consumer._stats['urls_skipped_dedup'] == 1
    assert consumer._stats['urls_skipped_dedup_pending'] == 1
    assert consumer._stats['urls_skipped_dedup_completed'] == 0

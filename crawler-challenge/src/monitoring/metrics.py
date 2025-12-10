import time
import asyncio
import psutil
from typing import Optional, List, Dict
from prometheus_client import start_http_server, Gauge, Counter, Histogram

class MetricsManager:
    def __init__(self, port=8000):
        self.port = port
        self.server_started = False
        
        # 1. 큐 상태 (Gauge: Redis 상태 반영)
        self.queue_pending = Gauge('crawler_queue_pending', 'Total URLs waiting in queue')
        self.queue_processing = Gauge('crawler_queue_processing', 'Total URLs currently being processed')
        
        # 2. 작업 결과 (Counter로 변경 권장, 하지만 마스터가 Redis 값을 덮어쓰는 구조라면 Gauge 유지)
        self.tasks_completed = Gauge('crawler_tasks_completed_total', 'Total successfully completed tasks')
        self.tasks_failed = Gauge('crawler_tasks_failed_total', 'Total failed tasks')

        # [NEW] 3. 상세 에러 카운트 (Counter + Label)
        # 예: metrics.inc_error('connection_error')
        self.error_details = Counter('crawler_error_details', 'Detailed error counts by type', ['error_type'])

        # [NEW] 4. 페이지 처리 시간 분포 (Histogram)
        # 예: with metrics.measure_latency(): process_page()
        self.process_latency = Histogram('crawler_process_latency_seconds', 'Time spent processing a page', buckets=[0.1, 0.5, 1.0, 2.0, 5.0, 10.0])

        # 5. 샤드별 상태 (Labeled Gauge)
        self.shard_pending = Gauge('crawler_shard_pending', 'Pending tasks per shard', ['shard_id'])

        # [NEW] 6. 시스템 리소스 (Gauge)
        self.system_cpu = Gauge('crawler_system_cpu_percent', 'CPU usage percent')
        self.system_memory = Gauge('crawler_system_memory_percent', 'Memory usage percent')

    def start_server(self):
        """Prometheus Exporter 서버 시작"""
        if not self.server_started:
            try:
                start_http_server(self.port)
                self.server_started = True
                print(f"[Metrics] Prometheus server started on port {self.port}")
            except Exception as e:
                print(f"[Metrics] Failed to start server: {e}")

    def update_queue_stats(self, stats: dict):
        """Master가 조회한 queue_stats 딕셔너리를 받아 메트릭 갱신"""
        self.queue_pending.set(stats.get('total_pending', 0))
        self.queue_processing.set(stats.get('processing', 0))
        self.tasks_completed.set(stats.get('completed', 0))
        self.tasks_failed.set(stats.get('failed', 0))

        if 'shard_details' in stats:
            for shard in stats['shard_details']:
                shard_id = str(shard.get('shard_id', 'unknown'))
                pending = shard.get('total_pending', 0)
                self.shard_pending.labels(shard_id=shard_id).set(pending)
        
        # 시스템 리소스도 같이 업데이트
        self.update_system_metrics()

    def update_system_metrics(self):
        """CPU 및 메모리 사용량 갱신"""
        self.system_cpu.set(psutil.cpu_percent())
        self.system_memory.set(psutil.virtual_memory().percent)

    def inc_error(self, error_type='unknown'):
        """특정 에러 타입 카운트 증가"""
        self.error_details.labels(error_type=error_type).inc()
                
class MetricsMonitor:
    def __init__(self):
        self.start_time = time.time()
        self.total_pages = 0
        self.active_tasks = 0
        self.monitoring = False
        self.monitor_task: Optional[asyncio.Task] = None
        self.total_errors = 0
        self.error_rate_history = []
    
    def increment_pages(self):
        self.total_pages += 1
    
    def increment_active_tasks(self):
        self.active_tasks += 1
    
    def decrement_active_tasks(self):
        self.active_tasks = max(0, self.active_tasks - 1)
    
    def get_pages_per_second(self) -> float:
        elapsed = time.time() - self.start_time
        return self.total_pages / elapsed if elapsed > 0 else 0
    
    def get_cpu_usage(self) -> float:
        return psutil.cpu_percent()
    
    def get_per_core_cpu_usage(self) -> List[float]:
        return psutil.cpu_percent(percpu=True)
    
    def get_process_count(self) -> int:
        return len(psutil.pids())
    
    def get_memory_usage(self) -> Dict[str, float]:
        memory = psutil.virtual_memory()
        return {
            'total_gb': memory.total / (1024**3),
            'available_gb': memory.available / (1024**3),
            'used_gb': memory.used / (1024**3),
            'percent': memory.percent
        }
    
    def get_active_tasks(self) -> int:
        return self.active_tasks
    
    def increment_errors(self):
        self.total_errors += 1
    
    def get_error_rate(self) -> float:
        total_requests = self.total_pages + self.total_errors
        return (self.total_errors / total_requests * 100) if total_requests > 0 else 0
    
    async def start_monitoring(self):
        self.monitoring = True
        self.monitor_task = asyncio.create_task(self._monitor_loop())
    
    async def stop_monitoring(self):
        self.monitoring = False
        if self.monitor_task:
            self.monitor_task.cancel()
            try:
                await self.monitor_task
            except asyncio.CancelledError:
                pass
    
    async def _monitor_loop(self):
        try:
            while self.monitoring:
                pages_per_sec = self.get_pages_per_second()
                cpu_usage = self.get_cpu_usage()
                active_tasks = self.get_active_tasks()
                
                # Get per-core CPU usage and memory stats
                per_core_cpu = self.get_per_core_cpu_usage()
                memory = self.get_memory_usage()
                error_rate = self.get_error_rate()
                
                # Simplified core display for extreme scale
                active_cores = sum(1 for cpu in per_core_cpu if cpu > 5)
                avg_core_usage = sum(per_core_cpu) / len(per_core_cpu)
                
                print(f"[METRICS] PPS: {pages_per_sec:.2f} | CPU: {cpu_usage:.1f}% ({active_cores}/{len(per_core_cpu)} cores) | "
                      f"RAM: {memory['used_gb']:.1f}/{memory['total_gb']:.1f}GB | Active: {active_tasks} | "
                      f"Errors: {error_rate:.1f}%")
                
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            pass
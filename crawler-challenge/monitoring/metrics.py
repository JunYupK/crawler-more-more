import asyncio
import time
import psutil
from typing import Optional, List, Dict


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
import asyncio
import time
import psutil
from typing import Optional


class MetricsMonitor:
    def __init__(self):
        self.start_time = time.time()
        self.total_pages = 0
        self.active_tasks = 0
        self.monitoring = False
        self.monitor_task: Optional[asyncio.Task] = None
    
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
    
    def get_active_tasks(self) -> int:
        return self.active_tasks
    
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
                
                print(f"[METRICS] PPS: {pages_per_sec:.2f} | CPU: {cpu_usage:.1f}% | Active: {active_tasks}")
                
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            pass
#!/usr/bin/env python3
"""
크롤링 제어 API 서버
- 크롤링 시작/중지/상태조회
- worker 수, URL 수 동적 지정
"""

from fastapi import FastAPI, BackgroundTasks, HTTPException
from pydantic import BaseModel
import asyncio
from typing import Optional
import subprocess
import time
import os

app = FastAPI(title="Crawler Control API", version="1.0.0")


# ============================================
# 상태 관리
# ============================================
class CrawlerState:
    def __init__(self):
        self.is_running = False
        self.workers = 0
        self.target_urls = 0
        self.processed_urls = 0
        self.success_count = 0
        self.fail_count = 0
        self.start_time = None
        self.process = None

crawler_state = CrawlerState()


# ============================================
# 요청/응답 모델
# ============================================
class CrawlRequest(BaseModel):
    workers: int = 4
    url_count: int = 10000

class CrawlStatus(BaseModel):
    is_running: bool
    workers: int
    target_urls: int
    processed_urls: int
    success_count: int
    fail_count: int
    progress_percent: float
    elapsed_seconds: Optional[float]
    pages_per_second: Optional[float]


# ============================================
# 엔드포인트
# ============================================
@app.get("/")
def root():
    return {
        "status": "ready",
        "message": "Crawler Control API",
        "endpoints": {
            "start": "POST /crawl/start",
            "status": "GET /crawl/status",
            "stop": "POST /crawl/stop"
        }
    }


@app.get("/health")
def health_check():
    return {"status": "healthy", "timestamp": time.time()}


@app.post("/crawl/start")
async def start_crawl(request: CrawlRequest, background_tasks: BackgroundTasks):
    """크롤링 시작"""
    if crawler_state.is_running:
        raise HTTPException(
            status_code=400,
            detail="크롤링이 이미 실행 중입니다. 먼저 /crawl/stop을 호출하세요."
        )

    background_tasks.add_task(
        run_crawler,
        request.workers,
        request.url_count
    )

    return {
        "status": "started",
        "config": {
            "workers": request.workers,
            "url_count": request.url_count
        }
    }


@app.get("/crawl/status")
def get_status() -> CrawlStatus:
    """크롤링 상태 조회"""
    elapsed = None
    pps = None

    if crawler_state.start_time:
        elapsed = time.time() - crawler_state.start_time
        if elapsed > 0 and crawler_state.processed_urls > 0:
            pps = round(crawler_state.processed_urls / elapsed, 2)

    progress = 0.0
    if crawler_state.target_urls > 0:
        progress = (crawler_state.processed_urls / crawler_state.target_urls) * 100

    return CrawlStatus(
        is_running=crawler_state.is_running,
        workers=crawler_state.workers,
        target_urls=crawler_state.target_urls,
        processed_urls=crawler_state.processed_urls,
        success_count=crawler_state.success_count,
        fail_count=crawler_state.fail_count,
        progress_percent=round(progress, 2),
        elapsed_seconds=round(elapsed, 1) if elapsed else None,
        pages_per_second=pps
    )


@app.post("/crawl/stop")
async def stop_crawl():
    """크롤링 중지"""
    if not crawler_state.is_running:
        raise HTTPException(
            status_code=400,
            detail="실행 중인 크롤링이 없습니다."
        )

    if crawler_state.process:
        crawler_state.process.terminate()
        crawler_state.process.wait(timeout=10)

    crawler_state.is_running = False

    return {
        "status": "stopped",
        "summary": {
            "processed": crawler_state.processed_urls,
            "success": crawler_state.success_count,
            "failed": crawler_state.fail_count
        }
    }


# ============================================
# 크롤링 실행 로직
# ============================================
async def run_crawler(workers: int, url_count: int):
    """백그라운드에서 크롤링 실행"""

    # 상태 초기화
    crawler_state.is_running = True
    crawler_state.workers = workers
    crawler_state.target_urls = url_count
    crawler_state.processed_urls = 0
    crawler_state.success_count = 0
    crawler_state.fail_count = 0
    crawler_state.start_time = time.time()

    try:
        # sharded_master.py 실행
        cmd = [
            "python", "-u", "runners/sharded_master.py",
            "--count", str(url_count),
            "--workers", str(workers)
        ]

        print(f"[API] 크롤링 시작: {' '.join(cmd)}")

        crawler_state.process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1
        )

        # 출력 모니터링 (진행 상황 파싱)
        for line in crawler_state.process.stdout:
            print(f"[Crawler] {line.strip()}")
            # TODO: 진행 상황 파싱해서 crawler_state 업데이트
            # 예: "Processed: 100/10000" 같은 형식 파싱

        crawler_state.process.wait()

        print(f"[API] 크롤링 완료, 종료 코드: {crawler_state.process.returncode}")

    except Exception as e:
        print(f"[API] 크롤링 에러: {e}")
    finally:
        crawler_state.is_running = False
        crawler_state.process = None

        # 리포트 자동 생성
        await generate_report()


async def generate_report():
    """크롤링 완료 후 리포트 자동 생성"""
    try:
        elapsed = time.time() - crawler_state.start_time if crawler_state.start_time else 0

        print(f"[API] 리포트 생성 중...")
        print(f"      처리: {crawler_state.processed_urls}")
        print(f"      성공: {crawler_state.success_count}")
        print(f"      실패: {crawler_state.fail_count}")
        print(f"      소요: {elapsed:.1f}초")

        # work_logger가 있다면 실행
        if os.path.exists("src/utils/work_logger.py"):
            subprocess.run([
                "python", "src/utils/work_logger.py",
                "--title", f"크롤링 완료: {crawler_state.target_urls} URLs",
                "--processed", str(crawler_state.processed_urls),
                "--elapsed", str(int(elapsed))
            ], check=False)

    except Exception as e:
        print(f"[API] 리포트 생성 에러: {e}")


# ============================================
# 메인 실행
# ============================================
if __name__ == "__main__":
    import uvicorn

    print("=" * 50)
    print("🚀 Crawler Control API 서버 시작")
    print("=" * 50)
    print("Endpoints:")
    print("  GET  /          - API 정보")
    print("  GET  /health    - 헬스체크")
    print("  POST /crawl/start - 크롤링 시작")
    print("  GET  /crawl/status - 상태 조회")
    print("  POST /crawl/stop  - 크롤링 중지")
    print("=" * 50)

    uvicorn.run(app, host="0.0.0.0", port=8080)

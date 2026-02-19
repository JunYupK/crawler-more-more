#!/usr/bin/env python3
"""
RAG Search API 서버 실행 스크립트

Usage:
    python desktop/run_api.py                     # 기본 (0.0.0.0:8600)
    python desktop/run_api.py --port 8601
    python desktop/run_api.py --host 127.0.0.1

Swagger UI: http://localhost:8600/docs
"""

import argparse
import logging
import os
import sys

# 프로젝트 루트를 sys.path에 추가
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

def main():
    parser = argparse.ArgumentParser(description="RAG Search API server")
    parser.add_argument("--host", default="0.0.0.0", help="Bind host (default: 0.0.0.0)")
    parser.add_argument("--port", type=int, default=8600, help="Bind port (default: 8600)")
    parser.add_argument("--reload", action="store_true", help="Auto-reload on code change (dev only)")
    args = parser.parse_args()

    try:
        import uvicorn
    except ImportError:
        print("ERROR: uvicorn is required. Install with: pip install uvicorn[standard]")
        sys.exit(1)

    print(f"Starting RAG Search API on http://{args.host}:{args.port}")
    print(f"Swagger UI: http://localhost:{args.port}/docs")
    print(f"Embed backend: {os.getenv('EMBED_BACKEND', 'local')}")

    uvicorn.run(
        "src.api.search_api:app",
        host=args.host,
        port=args.port,
        reload=args.reload,
        log_level="info",
    )


if __name__ == "__main__":
    main()

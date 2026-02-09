#!/usr/bin/env python3
"""
Mac Ingestor Entry Point
========================

맥북에서 간편하게 크롤러를 실행하는 진입점.
runners/ingestor_runner.py의 간소화된 래퍼.

Usage:
    # 프로젝트 루트에서 실행
    python mac/run.py
    python mac/run.py --test --limit 100
    python mac/run.py --kafka-servers desktop:9092
"""

import sys
from pathlib import Path

# 프로젝트 루트를 path에 추가
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# ingestor_runner의 main을 그대로 사용
from runners.ingestor_runner import main
import asyncio

if __name__ == '__main__':
    asyncio.run(main())

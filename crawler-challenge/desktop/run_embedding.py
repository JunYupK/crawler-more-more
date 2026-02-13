#!/usr/bin/env python3
"""
Desktop Embedding Entry Point
==============================

processed.final Kafka 토픽을 소비하여 pgvector에 벡터 임베딩을 저장합니다.

Usage:
    python desktop/run_embedding.py
    python desktop/run_embedding.py --max-messages 100
    python desktop/run_embedding.py --test-connection
    python desktop/run_embedding.py --stats
    python desktop/run_embedding.py --search "검색할 내용"
"""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from runners.embedding_runner import main
import asyncio

if __name__ == '__main__':
    asyncio.run(main())

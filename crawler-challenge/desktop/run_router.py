#!/usr/bin/env python3
"""
Desktop Router Entry Point
==========================

Usage:
    python desktop/run_router.py
    python desktop/run_router.py --test --max-messages 100
"""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from runners.router_runner import main
import asyncio

if __name__ == '__main__':
    asyncio.run(main())

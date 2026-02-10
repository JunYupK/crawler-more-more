#!/usr/bin/env python3
"""
Desktop Rich Processor Entry Point
====================================

Usage:
    python desktop/run_rich_processor.py
    python desktop/run_rich_processor.py --workers 2
"""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from runners.rich_processor_runner import main
import asyncio

if __name__ == '__main__':
    asyncio.run(main())

#!/usr/bin/env python3
"""
Desktop Fast Processor Entry Point
===================================

Usage:
    python desktop/run_fast_processor.py
    python desktop/run_fast_processor.py --workers 4
"""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from runners.fast_processor_runner import main
import asyncio

if __name__ == '__main__':
    asyncio.run(main())

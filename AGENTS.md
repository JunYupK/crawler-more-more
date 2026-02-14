# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## AI Guidance

* Ignore GEMINI.md and GEMINI-*.md files
* To save main context space, for code searches, inspections, troubleshooting or analysis, use code-searcher subagent where appropriate - giving the subagent full context background for the task(s) you assign it.
* After receiving tool results, carefully reflect on their quality and determine optimal next steps before proceeding. Use your thinking to plan and iterate based on this new information, and then take the best next action.
* For maximum efficiency, whenever you need to perform multiple independent operations, invoke all relevant tools simultaneously rather than sequentially.
* Before you finish, please verify your solution
* Do what has been asked; nothing more, nothing less.
* NEVER create files unless they're absolutely necessary for achieving your goal.
* ALWAYS prefer editing an existing file to creating a new one.
* NEVER proactively create documentation files (*.md) or README files. Only create documentation files if explicitly requested by the User.
* When you update or modify core context files, also update markdown documentation and memory bank
* When asked to commit changes, exclude CLAUDE.md and CLAUDE-*.md referenced memory bank system files from any commits. Never delete these files.
* Communicate in Korean unless instructed otherwise.

## Memory Bank System

This project uses a structured memory bank system with specialized context files. Always check these files for relevant information before starting work:

### Core Context Files

* **CLAUDE-activeContext.md** - Current session state, goals, and progress (if exists)
* **CLAUDE-patterns.md** - Established code patterns and conventions (if exists)
* **CLAUDE-decisions.md** - Architecture decisions and rationale (if exists)
* **CLAUDE-troubleshooting.md** - Common issues and proven solutions (if exists)
* **CLAUDE-config-variables.md** - Configuration variables reference (if exists)
* **CLAUDE-temp.md** - Temporary scratch pad (only read when referenced)

**Important:** Always reference the active context file first to understand what's currently being worked on and maintain session continuity.

### Memory Bank System Backups

When asked to backup Memory Bank System files, you will copy the core context files above and @.claude settings directory to directory @/path/to/backup-directory. If files already exist in the backup directory, you will overwrite them.

## Claude Code Official Documentation

When working on Claude Code features (hooks, skills, subagents, MCP servers, etc.), use the `claude-docs-consultant` skill to selectively fetch official documentation from docs.claude.com.

## Project Overview

### 프로젝트 설명
고처리량 분산 웹 크롤링 시스템. Mac(크롤링) + Desktop(처리/저장) 분산 아키텍처를 사용하며, 한정된 컴퓨팅 자원(M2 맥북 에어)에서 최대 크롤링 처리량을 목표로 합니다.

### 아키텍처 (두 가지 모드 공존)

**1. Stream Pipeline (신규, 권장)**
```
Mac (Layer 1)                          Desktop (Layer 2-4)
┌──────────────┐                       ┌──────────────────────────────────────┐
│   Ingestor   │                       │  Router → Processor → Storage       │
│  HTTP Crawl  │──── Kafka ──────────> │                                      │
│  + Zstd 압축  │   (raw.page)         │  ┌─ process.fast → FastProcessor    │
└──────────────┘                       │  └─ process.rich → RichProcessor    │
                                       │          ↓                           │
                                       │  processed.final → HybridStorage    │
                                       │    (MinIO + PostgreSQL)              │
                                       └──────────────────────────────────────┘
```

**2. Sharded Architecture (레거시)**
- `sharded_master`: 작업 분배 + Redis 큐 관리
- `sharded_worker`: 실제 크롤링 수행 (Polite Crawling)

### 기술 스택
* 언어: Python 3.10+ (AsyncIO, Multiprocessing)
* 컨테이너: Docker, Docker Compose
* 데이터베이스: PostgreSQL (결과 저장), Redis (큐 관리), Kafka (스트림 파이프라인)
* 오브젝트 스토리지: MinIO
* 모니터링: Prometheus, Grafana
* 네트워크: Tailscale (Windows ↔ Mac 연결)

### 주요 디렉토리 구조
```
crawler-challenge/
├── src/
│   ├── common/          # 공유 모듈 (compression, kafka_config)
│   ├── core/            # 핵심 크롤링 로직 (polite_crawler.py, database.py)
│   ├── managers/        # 큐/작업 관리자 (redis_queue_manager.py, sharded_queue_manager.py)
│   ├── ingestor/        # Layer 1: HTTP 크롤링 + Kafka produce
│   ├── router/          # Layer 2: 콘텐츠 분석 + 라우팅
│   ├── processor/       # Layer 3: HTML → Markdown 변환
│   ├── storage/         # Layer 4: MinIO + PostgreSQL 저장
│   ├── monitoring/      # Prometheus 메트릭
│   └── utils/           # 유틸리티 (로깅, 리포트 생성)
├── runners/             # 전체 실행 스크립트 (entry points)
├── mac/                 # Mac 전용 간소화 실행기 (Ingestor)
├── desktop/             # Desktop 전용 실행기 (Router/Processor/Storage)
├── docker/              # Docker 설정, SQL 스크립트
├── scripts/             # 자동화 스크립트 (AI 리포트 등)
├── tests/               # 테스트 (pytest)
├── docs/reports/        # AI 생성 성능 리포트
├── docker-compose.yml   # Sharded 아키텍처용
└── pyproject.toml       # 패키지 설정
```

### Kafka Topics (Stream Pipeline)
| 토픽 | 설명 |
|------|------|
| `raw.page` | Layer 1 → 2: 크롤링된 원본 페이지 (Zstd 압축) |
| `process.fast` | Layer 2 → 3: 정적 페이지 (BeautifulSoup 처리) |
| `process.rich` | Layer 2 → 3: 동적 페이지 (Crawl4AI 처리) |
| `processed.final` | Layer 3 → 4: 처리 완료 결과 |
| `*.dlq` | 각 레이어의 Dead Letter Queue |

### 코드 패턴
**공유 모듈 import (권장 방식)**:
```python
from src.common.compression import compress, decompress_to_str
from src.common.kafka_config import get_config, reset_config
```
기존 경로(`config/kafka_config.py`, `src/ingestor/compression.py`)는 하위호환 shim으로 유지됨.

**모든 작업 완료 시:**
* 작업 중 잘못 하였거나 실수한 내용들은 2번 다시 같은 실수가 일어나지 않도록 이 CLAUDE.md 파일에 명시해둘 것
* 개발을 진행하면서 주요 디렉토리 구조가 바뀌었다면 CALAUDE.md 파일도 변경할 것
* **테스트 후 부산물 자동 정리**: 테스트 실행 후 생성된 부산물들을 자동으로 정리하고 git status를 clean 상태로 유지할 것
  - 변경된 테스트 데이터 파일 복원 (예: `git restore data/tranco_urls.txt`)
  - 임시 파일 삭제 (예: `nul`, `*.log`, `*.pyc` 등)
  - 작업 완료 전 `git status`로 clean 상태 확인

**이 규칙은 모든 세션에서 자동 적용**

## Development Commands

### Environment Setup
```bash
# Working directory: crawler-challenge/

# Mac (Ingestor만 실행)
pip install -e ".[mac]"

# Desktop (Router/Processor/Storage 실행)
pip install -e ".[desktop]"

# 개발 환경 (테스트 포함)
pip install -e ".[mac,desktop,dev]"

# 또는 requirements.txt 사용
pip install -r requirements.txt
```

### Testing
```bash
# Working directory: crawler-challenge/

# 전체 테스트 실행
python -m pytest tests/ -v

# 특정 레이어 테스트
python -m pytest tests/test_ingestor.py -v
python -m pytest tests/test_router.py -v
python -m pytest tests/test_processor.py -v
python -m pytest tests/test_storage.py -v

# JSON 리포트 포함
python -m pytest --json-report --json-report-file=test_report.json -v
```

### Stream Pipeline 실행 (신규 아키텍처)

**인프라 시작**:
```bash
docker compose -f docker/docker-compose.stream.yml up -d
```

**Mac (Ingestor)**:
```bash
python mac/run.py                              # 기본 실행
python mac/run.py --test --limit 100          # 테스트 모드
python mac/run.py --kafka-servers 192.168.x.x:9092  # Kafka 서버 지정
```

**Desktop (각 터미널에서)**:
```bash
python desktop/run_router.py                   # Router
python desktop/run_fast_processor.py --workers 4   # Fast Processor
python desktop/run_rich_processor.py --workers 2   # Rich Processor (Crawl4AI)
python desktop/run_storage.py                  # Storage
```

**또는 runners/ 사용**:
```bash
python runners/ingestor_runner.py
python runners/router_runner.py
python runners/fast_processor_runner.py
python runners/rich_processor_runner.py
python runners/storage_runner.py
```

### Sharded Architecture 실행 (레거시)

```bash
# 인프라
docker compose up -d redis postgres

# 로컬 실행
python runners/sharded_master.py --count 10000 --workers 4
python runners/sharded_worker.py --worker-id 1

# Docker 실행
docker compose build && docker compose up -d
docker compose logs -f crawler-master
```

### Monitoring & Analysis
```bash
python scripts/generate_ai_report.py --time-range 60
python scripts/analyze_failures.py
```

### Service Ports
| 서비스 | 포트 | 용도 |
|--------|------|------|
| Master | 8000 | Prometheus 메트릭 |
| Worker 1-8 | 8001-8008 | 각 Worker 메트릭 |
| PostgreSQL | 5432 | 데이터베이스 |
| Redis | 6379 | 큐 관리 |
| Kafka UI | 8080 | Kafka 모니터링 |
| MinIO Console | 9001 | Object Storage 관리 |
| Prometheus | 9090 | 메트릭 수집 |
| Grafana | 3000 | 대시보드 |

### Environment Variables (Stream Pipeline)
| 변수 | 기본값 | 설명 |
|------|--------|------|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka 브로커 |
| `MINIO_ENDPOINT` | `localhost:9000` | MinIO 엔드포인트 |
| `INGESTOR_MAX_CONCURRENT` | `500` | 최대 동시 요청 |
| `ROUTER_STATIC_THRESHOLD` | `80` | 정적 페이지 판정 점수 |
| `PROCESSOR_FAST_WORKERS` | `6` | Fast Worker 수 |
| `PROCESSOR_RICH_WORKERS` | `2` | Rich Worker 수 |

## File Search & Navigation Commands

**IMPORTANT**: `fd` may not be installed in all environments. Use `rg` (ripgrep) as the primary tool.

### Quick Reference

**Task: "List all files"**
```bash
rg --files              # RECOMMENDED: Lists all files (respects .gitignore)
ls -la                  # Current directory only
```

**Task: "Search for content in files"**
```bash
rg "search_term"        # Search everywhere (FASTEST)
```

**Task: "Find files by name"**
```bash
rg --files | rg "filename"    # Find by name pattern
```

### Directory/File Exploration

```bash
# List all files recursively:
rg --files              # All files (respects .gitignore) - RECOMMENDED
rg --files -t py        # Only Python files
ls -la                  # Current directory only

# If fd is available (optional):
# fd . -t f             # All files
# fd . -t d             # All directories
```

### BANNED - Never Use These Slow Tools

* ❌ `tree` - NOT INSTALLED, use `rg --files` or Glob tool
* ❌ `find` - use `rg --files` or Glob tool
* ❌ `grep` or `grep -r` - use `rg` or Grep tool instead
* ❌ `ls -R` - use `rg --files`
* ❌ `cat file | grep` - use `rg pattern file`

### Recommended Tools

```bash
# ripgrep (rg) - content search
rg "search_term"                # Search in all files
rg -i "case_insensitive"        # Case-insensitive
rg "pattern" -t py              # Only Python files
rg "pattern" -g "*.md"          # Only Markdown
rg -l "pattern"                 # Filenames with matches
rg -c "pattern"                 # Count matches per file
rg -n "pattern"                 # Show line numbers
rg -A 3 -B 3 "error"            # Context lines
rg "(TODO|FIXME|HACK)"          # Multiple patterns

# ripgrep (rg) - file listing
rg --files                      # List all files (respects .gitignore)
rg --files | rg "pattern"       # Find files by name
rg --files -t md                # Only Markdown files
```

### Search Strategy

1. Start broad, then narrow: `rg "partial" | rg "specific"`
2. Filter by type early: `rg -t python "def function_name"`
3. Batch patterns: `rg "(pattern1|pattern2|pattern3)"`
4. Limit scope: `rg "pattern" src/`

### INSTANT DECISION TREE

```
User asks to "list/show/summarize/explore files"?
  → USE: rg --files  (RECOMMENDED, respects .gitignore)
  → OR: Glob tool for pattern matching
  → OR: ls -la (for single directory)

User asks to "search/grep/find text content"?
  → USE: rg "pattern"  (NOT grep!)
  → OR: Grep tool for complex searches

User asks to "find file/directory by name"?
  → USE: rg --files | rg "name"
  → OR: Glob tool with pattern

User asks for "directory structure/tree"?
  → USE: Task tool with Explore subagent
  → NEVER: tree (not installed!)

Need just current directory?
  → USE: ls -la  (OK for single dir)
```
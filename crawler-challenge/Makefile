# =============================================================================
# Crawler Pipeline — 운영 명령 통합 Makefile
# =============================================================================
#
# 사용법:
#   make test-crawl                     # Mac 크롤링 테스트 (100개 URL, 워커 1개)
#   make test-crawl CRAWL_LIMIT=500     # URL 개수 지정
#   make test-crawl WORKERS=2           # 워커 수 지정
#   make test-crawl-full                # 전체 1M URL 크롤링 (워커 4개)
#   make status                         # Desktop 파이프라인 상태 확인
#   make restart-desktop                # Desktop 서비스 재시작
#   make stop-desktop                   # Desktop 서비스 중지
#   make logs SVC=router                # 특정 서비스 로그 확인
#
# Mac 크롤링 구조:
#   sharded_master  — Redis 샤드 큐에 URL 로딩 + 진행 모니터링
#   sharded_worker  — Redis 큐에서 URL 소비 → HTTP 크롤링 → Kafka produce
#
# =============================================================================

# ── 설정 변수 (필요시 override 가능) ──────────────────────────────────────
CRAWL_LIMIT    ?= 100
WORKERS        ?= 1
VENV_PATH      ?= $(HOME)/venv
PYTHON         ?= python3
SVC            ?=

# 프로젝트 경로
MAKEFILE_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

.PHONY: test-crawl test-crawl-full status restart-desktop stop-desktop logs help

# ── 테스트 크롤링 (Mac) ──────────────────────────────────────────────────
# 1. sharded_master: CRAWL_LIMIT 개 URL을 Redis 샤드 큐에 로딩 후 모니터링
# 2. sharded_worker(s): Redis 큐에서 URL 소비하여 실제 크롤링 수행
test-crawl:
	@echo "==> 크롤링 테스트 시작 ($(CRAWL_LIMIT)개 URL, 워커 $(WORKERS)개)"
	@bash -c '\
		if [ -f "$(VENV_PATH)/bin/activate" ]; then \
			source "$(VENV_PATH)/bin/activate"; \
		fi; \
		cd "$(MAKEFILE_DIR)"; \
		$(PYTHON) runners/sharded_master.py --count $(CRAWL_LIMIT) --auto-start & \
		MASTER_PID=$$!; \
		trap "kill $$MASTER_PID 2>/dev/null || true; exit" INT TERM EXIT; \
		sleep 3; \
		WORKER_PIDS=; \
		for i in $$(seq 1 $(WORKERS)); do \
			$(PYTHON) runners/sharded_worker.py --worker-id $$i & \
			WORKER_PIDS="$$WORKER_PIDS $$!"; \
		done; \
		wait $$WORKER_PIDS; \
		kill $$MASTER_PID 2>/dev/null || true'

# 전체 크롤링 (Tranco Top 1M, 워커 4개)
test-crawl-full:
	@echo "==> 전체 크롤링 시작 (1M URLs, 워커 4개)"
	@bash -c '\
		if [ -f "$(VENV_PATH)/bin/activate" ]; then \
			source "$(VENV_PATH)/bin/activate"; \
		fi; \
		cd "$(MAKEFILE_DIR)"; \
		$(PYTHON) runners/sharded_master.py --count 1000000 --auto-start & \
		MASTER_PID=$$!; \
		trap "kill $$MASTER_PID 2>/dev/null || true; exit" INT TERM EXIT; \
		sleep 5; \
		WORKER_PIDS=; \
		for i in 1 2 3 4; do \
			$(PYTHON) runners/sharded_worker.py --worker-id $$i & \
			WORKER_PIDS="$$WORKER_PIDS $$!"; \
		done; \
		wait $$WORKER_PIDS; \
		kill $$MASTER_PID 2>/dev/null || true'

# ── Desktop 파이프라인 관리 ──────────────────────────────────────────────
status:
	@cd "$(MAKEFILE_DIR)" && ./desktop/start.sh status

restart-desktop:
	@echo "==> Desktop 파이프라인 재시작"
	@cd "$(MAKEFILE_DIR)" && ./desktop/start.sh stop || true
	@cd "$(MAKEFILE_DIR)" && ./desktop/start.sh

stop-desktop:
	@cd "$(MAKEFILE_DIR)" && ./desktop/start.sh stop

logs:
ifndef SVC
	@echo "사용법: make logs SVC=서비스명"
	@echo "  예시: make logs SVC=router"
	@echo ""
	@cd "$(MAKEFILE_DIR)" && ls desktop/logs/*.log 2>/dev/null | xargs -I{} basename {} .log | sed 's/^/  /'
else
	@cd "$(MAKEFILE_DIR)" && ./desktop/start.sh logs $(SVC)
endif

# ── 도움말 ───────────────────────────────────────────────────────────────
help:
	@echo ""
	@echo "  Crawler Pipeline 운영 명령"
	@echo "  ─────────────────────────────────────────────────────"
	@echo ""
	@echo "  테스트:"
	@echo "    make test-crawl                   100개 URL 테스트 크롤링 (워커 1개)"
	@echo "    make test-crawl CRAWL_LIMIT=500   URL 개수 지정"
	@echo "    make test-crawl WORKERS=2          워커 수 지정"
	@echo "    make test-crawl-full              전체 1M URL 크롤링 (워커 4개)"
	@echo ""
	@echo "  Desktop 관리:"
	@echo "    make status                       파이프라인 상태 확인"
	@echo "    make restart-desktop              서비스 재시작"
	@echo "    make stop-desktop                 서비스 중지"
	@echo "    make logs SVC=router              로그 확인"
	@echo ""
	@echo "  설정 변수 (override 가능):"
	@echo "    CRAWL_LIMIT     테스트 URL 수    (기본: $(CRAWL_LIMIT))"
	@echo "    WORKERS         워커 프로세스 수 (기본: $(WORKERS))"
	@echo "    VENV_PATH       venv 경로        (기본: $(VENV_PATH))"
	@echo ""

# =============================================================================
# Crawler Pipeline — 운영 명령 통합 Makefile
# =============================================================================
#
# 사용법:
#   make test-crawl                    # Mac 크롤링 테스트 (100개 URL)
#   make test-crawl CRAWL_LIMIT=500    # URL 개수 지정
#   make test-crawl-full               # 전체 1M URL 크롤링
#   make status                        # Desktop 파이프라인 상태 확인
#   make restart-desktop               # Desktop 서비스 재시작
#   make stop-desktop                  # Desktop 서비스 중지
#   make logs SVC=router               # 특정 서비스 로그 확인
#
# =============================================================================

# ── 설정 변수 (필요시 override 가능) ──────────────────────────────────────
KAFKA_SERVERS  ?= 100.105.22.101:9092
CRAWL_LIMIT    ?= 100
MAX_CONCURRENT ?= 500
VENV_PATH      ?= $(HOME)/venv
PYTHON         ?= python3
SVC            ?=

# 프로젝트 경로
MAKEFILE_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

.PHONY: test-crawl test-crawl-full status restart-desktop stop-desktop logs help

# ── 테스트 크롤링 (Mac) ──────────────────────────────────────────────────
# venv를 자동 활성화하고 인제스터를 실행합니다.
# 기존에 수동으로 하던 것: source ~/venv/bin/activate && python mac/run.py --test
test-crawl:
	@echo "==> 크롤링 테스트 시작 ($(CRAWL_LIMIT)개 URL, Kafka: $(KAFKA_SERVERS))"
	@bash -c '\
		if [ -f "$(VENV_PATH)/bin/activate" ]; then \
			source "$(VENV_PATH)/bin/activate"; \
		fi; \
		cd "$(MAKEFILE_DIR)" && \
		$(PYTHON) mac/run.py --test --limit $(CRAWL_LIMIT) \
			--max-concurrent $(MAX_CONCURRENT) \
			--kafka-servers $(KAFKA_SERVERS)'

# 전체 크롤링 (Tranco Top 1M)
test-crawl-full:
	@echo "==> 전체 크롤링 시작 (1M URLs, Kafka: $(KAFKA_SERVERS))"
	@bash -c '\
		if [ -f "$(VENV_PATH)/bin/activate" ]; then \
			source "$(VENV_PATH)/bin/activate"; \
		fi; \
		cd "$(MAKEFILE_DIR)" && \
		$(PYTHON) mac/run.py \
			--max-concurrent $(MAX_CONCURRENT) \
			--kafka-servers $(KAFKA_SERVERS)'

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
	@echo "    make test-crawl                   100개 URL 테스트 크롤링"
	@echo "    make test-crawl CRAWL_LIMIT=500   URL 개수 지정"
	@echo "    make test-crawl-full              전체 1M URL 크롤링"
	@echo ""
	@echo "  Desktop 관리:"
	@echo "    make status                       파이프라인 상태 확인"
	@echo "    make restart-desktop              서비스 재시작"
	@echo "    make stop-desktop                 서비스 중지"
	@echo "    make logs SVC=router              로그 확인"
	@echo ""
	@echo "  설정 변수 (override 가능):"
	@echo "    KAFKA_SERVERS   Kafka 주소       (기본: $(KAFKA_SERVERS))"
	@echo "    CRAWL_LIMIT     테스트 URL 수    (기본: $(CRAWL_LIMIT))"
	@echo "    MAX_CONCURRENT  동시 요청 수     (기본: $(MAX_CONCURRENT))"
	@echo "    VENV_PATH       venv 경로        (기본: $(VENV_PATH))"
	@echo ""

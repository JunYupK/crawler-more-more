#!/usr/bin/env bash
# =============================================================================
# Desktop 스트림 파이프라인 실행 스크립트
# =============================================================================
#
# 실행하는 서비스:
#   [필수]
#   1. Router          raw.page → process.fast / process.rich
#   2. Fast Processor  process.fast → processed.final (BeautifulSoup)
#   3. Rich Processor  process.rich → processed.final (Crawl4AI)
#   4. Storage         processed.final → MinIO + PostgreSQL
#   [선택]
#   5. URL Queue       discovered.urls → Redis 크롤러 큐  (--with-url-queue)
#   6. Embedding       processed.final → pgvector         (--with-embedding)
#
# Usage:
#   ./desktop/start.sh                      # 필수 서비스만 실행
#   ./desktop/start.sh --with-url-queue     # + URL 자동 재공급
#   ./desktop/start.sh --with-embedding     # + 벡터 임베딩 (무거움)
#   ./desktop/start.sh --all                # 전체 실행
#   ./desktop/start.sh stop                 # 전체 중지
#   ./desktop/start.sh status               # 실행 상태 확인
#   ./desktop/start.sh logs [서비스명]      # 로그 확인
#
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
LOG_DIR="$SCRIPT_DIR/logs"
PID_DIR="$SCRIPT_DIR/.pids"
PYTHON="${PYTHON:-python3}"

# 색상 출력
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
CYAN='\033[0;36m'
RESET='\033[0m'

log_info()  { echo -e "${GREEN}[INFO]${RESET}  $*"; }
log_warn()  { echo -e "${YELLOW}[WARN]${RESET}  $*"; }
log_error() { echo -e "${RED}[ERROR]${RESET} $*"; }
log_step()  { echo -e "${CYAN}[>>>>]${RESET}  $*"; }

# ----------------------------------------------------------------------------
# 파라미터 파싱
# ----------------------------------------------------------------------------
WITH_URL_QUEUE=false
WITH_EMBEDDING=false
COMMAND="start"

for arg in "$@"; do
    case $arg in
        --with-url-queue) WITH_URL_QUEUE=true ;;
        --with-embedding) WITH_EMBEDDING=true ;;
        --all)            WITH_URL_QUEUE=true; WITH_EMBEDDING=true ;;
        stop)             COMMAND="stop" ;;
        status)           COMMAND="status" ;;
        logs)             COMMAND="logs" ;;
        -h|--help)        COMMAND="help" ;;
    esac
done

# 로그 인수 (logs [서비스명])
LOGS_TARGET="${2:-}"

# ----------------------------------------------------------------------------
# 헬퍼 함수
# ----------------------------------------------------------------------------
start_service() {
    local name="$1"
    local script="$2"
    shift 2
    local extra_args=("$@")

    local log_file="$LOG_DIR/${name}.log"
    local pid_file="$PID_DIR/${name}.pid"

    # 이미 실행 중이면 스킵
    if [[ -f "$pid_file" ]]; then
        local old_pid
        old_pid=$(cat "$pid_file")
        if kill -0 "$old_pid" 2>/dev/null; then
            log_warn "$name 이미 실행 중 (PID $old_pid) — 스킵"
            return
        fi
    fi

    cd "$PROJECT_ROOT"
    $PYTHON "$script" "${extra_args[@]}" >> "$log_file" 2>&1 &
    local pid=$!
    echo "$pid" > "$pid_file"
    log_info "$(printf '%-18s' "$name") 시작 | PID %-6s | 로그: desktop/logs/${name}.log" "$pid"
}

stop_service() {
    local name="$1"
    local pid_file="$PID_DIR/${name}.pid"

    if [[ ! -f "$pid_file" ]]; then
        return
    fi

    local pid
    pid=$(cat "$pid_file")
    if kill -0 "$pid" 2>/dev/null; then
        kill -TERM "$pid" 2>/dev/null || true
        # 최대 5초 대기
        local i=0
        while kill -0 "$pid" 2>/dev/null && [[ $i -lt 10 ]]; do
            sleep 0.5; ((i++))
        done
        if kill -0 "$pid" 2>/dev/null; then
            kill -KILL "$pid" 2>/dev/null || true
            log_warn "$name 강제 종료 (SIGKILL)"
        else
            log_info "$name 정상 종료"
        fi
    fi
    rm -f "$pid_file"
}

show_status() {
    local services=("router" "fast_processor" "rich_processor" "storage" "url_queue" "embedding")
    echo ""
    echo -e "  ${CYAN}서비스             PID       상태${RESET}"
    echo    "  ─────────────────────────────────────"
    for name in "${services[@]}"; do
        local pid_file="$PID_DIR/${name}.pid"
        if [[ -f "$pid_file" ]]; then
            local pid
            pid=$(cat "$pid_file")
            if kill -0 "$pid" 2>/dev/null; then
                printf "  %-18s %-8s ${GREEN}실행중${RESET}\n" "$name" "$pid"
            else
                printf "  %-18s %-8s ${RED}중지됨${RESET} (PID 파일 남아있음)\n" "$name" "$pid"
            fi
        else
            printf "  %-18s %-8s ${YELLOW}미실행${RESET}\n" "$name" "-"
        fi
    done
    echo ""
}

# ----------------------------------------------------------------------------
# 명령 실행
# ----------------------------------------------------------------------------
case "$COMMAND" in

    # ── HELP ────────────────────────────────────────────────────────────────
    help)
        grep '^#' "$0" | grep -v '^#!/' | sed 's/^# \?//'
        exit 0
        ;;

    # ── STOP ────────────────────────────────────────────────────────────────
    stop)
        log_step "Desktop 파이프라인 중지 중..."
        for name in embedding url_queue storage rich_processor fast_processor router; do
            stop_service "$name"
        done
        log_info "모든 서비스 중지 완료"
        exit 0
        ;;

    # ── STATUS ──────────────────────────────────────────────────────────────
    status)
        show_status
        exit 0
        ;;

    # ── LOGS ────────────────────────────────────────────────────────────────
    logs)
        if [[ -n "$LOGS_TARGET" ]]; then
            tail -f "$LOG_DIR/${LOGS_TARGET}.log"
        else
            echo "사용 가능한 로그 파일:"
            ls "$LOG_DIR"/*.log 2>/dev/null | xargs -I{} basename {} .log | sed 's/^/  /'
            echo ""
            echo "사용법: ./desktop/start.sh logs [서비스명]"
            echo "  예시: ./desktop/start.sh logs router"
        fi
        exit 0
        ;;

    # ── START ────────────────────────────────────────────────────────────────
    start)
        mkdir -p "$LOG_DIR" "$PID_DIR"

        echo ""
        echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}"
        echo -e "${CYAN}  Desktop 스트림 파이프라인 시작${RESET}"
        echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}"
        echo ""

        # ── 필수 서비스 ─────────────────────────────────────────────────────
        log_step "필수 서비스 시작..."
        start_service "router"         "desktop/run_router.py"
        sleep 0.5
        start_service "fast_processor" "desktop/run_fast_processor.py" --workers 4
        start_service "rich_processor" "desktop/run_rich_processor.py" --workers 2
        start_service "storage"        "desktop/run_storage.py"

        # ── 선택 서비스 ─────────────────────────────────────────────────────
        if $WITH_URL_QUEUE; then
            log_step "URL Queue 서비스 시작..."
            start_service "url_queue" "desktop/run_url_queue.py"
        fi

        if $WITH_EMBEDDING; then
            log_step "Embedding 서비스 시작..."
            start_service "embedding" "desktop/run_embedding.py"
        fi

        echo ""
        show_status

        # ── 종료 트랩 ───────────────────────────────────────────────────────
        cleanup() {
            echo ""
            log_step "Ctrl+C 감지 — 모든 서비스 종료 중..."
            for name in embedding url_queue storage rich_processor fast_processor router; do
                stop_service "$name"
            done
            log_info "정상 종료 완료"
            exit 0
        }
        trap cleanup INT TERM

        log_info "모든 서비스가 백그라운드에서 실행 중입니다."
        log_info "Ctrl+C 또는 './desktop/start.sh stop' 으로 중지할 수 있습니다."
        echo ""

        # 프로세스가 살아있는지 모니터링
        while true; do
            sleep 10
            DEAD=false
            for name in router fast_processor rich_processor storage; do
                pid_file="$PID_DIR/${name}.pid"
                if [[ -f "$pid_file" ]]; then
                    pid=$(cat "$pid_file")
                    if ! kill -0 "$pid" 2>/dev/null; then
                        log_warn "$name (PID $pid) 이 예기치 않게 종료되었습니다. 로그를 확인하세요: desktop/logs/${name}.log"
                        DEAD=true
                    fi
                fi
            done
        done
        ;;
esac

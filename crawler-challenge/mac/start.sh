#!/usr/bin/env bash
# =============================================================================
# Mac 인제스터 실행 스크립트
# =============================================================================
#
# Tranco Top 1M URL을 고속 크롤링하여 Kafka의 raw.page 토픽으로 전송합니다.
#
# Usage:
#   ./mac/start.sh                              # 기본 실행 (1M URLs, 500 동시)
#   ./mac/start.sh --test                       # 테스트 모드 (100개만)
#   ./mac/start.sh --limit 50000                # URL 수 지정
#   ./mac/start.sh --max-concurrent 200         # 동시 요청 수 조정
#   ./mac/start.sh --kafka-servers 192.168.x.x:9092  # Desktop Kafka 주소 지정
#
# 환경변수 (선택):
#   KAFKA_SERVERS   Kafka 브로커 주소 (기본: localhost:9092)
#   PYTHON          Python 실행 파일 (기본: python3)
#
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
LOG_DIR="$SCRIPT_DIR/logs"
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

mkdir -p "$LOG_DIR"

# ----------------------------------------------------------------------------
# Kafka 연결 확인
# ----------------------------------------------------------------------------
KAFKA_SERVERS="${KAFKA_SERVERS:-localhost:9092}"
KAFKA_HOST="${KAFKA_SERVERS%%:*}"
KAFKA_PORT="${KAFKA_SERVERS##*:}"

log_step "Kafka 연결 확인 중... ($KAFKA_SERVERS)"
if ! nc -z -w3 "$KAFKA_HOST" "$KAFKA_PORT" 2>/dev/null; then
    log_error "Kafka에 연결할 수 없습니다: $KAFKA_SERVERS"
    log_error "Desktop 서버에서 docker-compose가 실행 중인지 확인하세요."
    exit 1
fi
log_info "Kafka 연결 확인 완료"

# ----------------------------------------------------------------------------
# Python 의존성 확인
# ----------------------------------------------------------------------------
log_step "Python 패키지 확인 중..."
MISSING=""
for pkg in httpx aiokafka msgpack zstandard tranco; do
    if ! $PYTHON -c "import ${pkg//-/_}" 2>/dev/null; then
        MISSING="$MISSING $pkg"
    fi
done

if [[ -n "$MISSING" ]]; then
    log_warn "다음 패키지가 없습니다:$MISSING"
    log_warn "설치하려면: pip install$MISSING"
    read -rp "계속하시겠습니까? (y/N) " confirm
    [[ "$confirm" =~ ^[Yy]$ ]] || exit 1
fi

# ----------------------------------------------------------------------------
# 인제스터 실행
# ----------------------------------------------------------------------------
echo ""
echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}"
echo -e "${CYAN}  Mac 인제스터 시작${RESET}"
echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}"
echo ""
log_info "Kafka:     $KAFKA_SERVERS"
log_info "Log:       mac/logs/ingestor.log"
echo ""

cd "$PROJECT_ROOT"

# 인자를 그대로 mac/run.py에 전달 (--kafka-servers 기본값 주입)
# 사용자가 --kafka-servers를 직접 지정하지 않은 경우 환경변수 값 사용
ARGS=("$@")
HAS_KAFKA=false
for arg in "${ARGS[@]:-}"; do
    [[ "$arg" == "--kafka-servers" ]] && HAS_KAFKA=true
done

if ! $HAS_KAFKA && [[ "$KAFKA_SERVERS" != "localhost:9092" ]]; then
    ARGS+=("--kafka-servers" "$KAFKA_SERVERS")
fi

log_step "python mac/run.py ${ARGS[*]:-}"
echo ""

# 포그라운드 실행 (로그를 화면 + 파일에 동시 출력)
$PYTHON mac/run.py "${ARGS[@]:-}" 2>&1 | tee -a "$LOG_DIR/ingestor.log"

#!/bin/bash
#
# 통합 테스트 러너 (Unified Test Runner)
# CI/CD 파이프라인 점검 + pytest 실행 + 리포트 자동 생성
#
# Usage:
#   ./run_tests.sh              # 전체 실행 (quick + pytest + report)
#   ./run_tests.sh --quick      # 빠른 검증만 (import, config, files)
#   ./run_tests.sh --pytest     # pytest만 실행
#   ./run_tests.sh --report     # 기존 JSON으로 리포트만 생성
#   ./run_tests.sh -k "test_db" # pytest에 옵션 전달
#

set -o pipefail

# ============================================
# 색상 및 전역 변수
# ============================================
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# 테스트 카운터
TOTAL_TESTS=0
PASSED_TESTS=0
FAILED_TESTS=0

# 프로젝트 루트로 이동
cd "$(dirname "$0")" || exit 1

# ============================================
# 유틸리티 함수
# ============================================

print_header() {
    echo ""
    echo -e "${BLUE}======================================${NC}"
    echo -e "${BLUE}  $1${NC}"
    echo -e "${BLUE}======================================${NC}"
    echo ""
}

print_phase() {
    echo ""
    echo -e "${CYAN}=== $1 ===${NC}"
}

run_test() {
    local test_name=$1
    local test_command=$2

    TOTAL_TESTS=$((TOTAL_TESTS + 1))

    if eval "$test_command" > /dev/null 2>&1; then
        echo -e "  ${GREEN}✅ PASS${NC}: $test_name"
        PASSED_TESTS=$((PASSED_TESTS + 1))
        return 0
    else
        echo -e "  ${RED}❌ FAIL${NC}: $test_name"
        FAILED_TESTS=$((FAILED_TESTS + 1))
        return 1
    fi
}

check_dependency() {
    local name=$1
    local install_cmd=$2
    
    if ! python -c "import $name" 2>/dev/null; then
        echo -e "${YELLOW}[WARN] $name이 설치되어 있지 않습니다.${NC}"
        read -p "       설치하시겠습니까? (y/n): " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            pip install $install_cmd
        else
            return 1
        fi
    fi
    return 0
}

# ============================================
# Phase 1: Python 문법 검사
# ============================================
run_syntax_check() {
    print_phase "Phase 1: Python Syntax Check"
    
    local py_files=$(find . -name "*.py" -not -path "./venv/*" -not -path "./.git/*" 2>/dev/null | head -20)
    
    if [ -z "$py_files" ]; then
        echo -e "  ${YELLOW}⚠️ SKIP${NC}: Python 파일을 찾을 수 없습니다."
        return 0
    fi
    
    local syntax_ok=true
    for file in $py_files; do
        if ! python -m py_compile "$file" 2>/dev/null; then
            echo -e "  ${RED}❌ FAIL${NC}: $file"
            syntax_ok=false
            FAILED_TESTS=$((FAILED_TESTS + 1))
        fi
        TOTAL_TESTS=$((TOTAL_TESTS + 1))
    done
    
    if $syntax_ok; then
        echo -e "  ${GREEN}✅ PASS${NC}: 모든 Python 파일 문법 검사 통과"
        PASSED_TESTS=$((PASSED_TESTS + 1))
    fi
}

# ============================================
# Phase 2: Import 테스트
# ============================================
run_import_tests() {
    print_phase "Phase 2: Import Tests"
    
    # 핵심 모듈 import 테스트
    run_test "Import config.settings" "python -c 'from config import settings'"
    run_test "Import src.core.polite_crawler" "python -c 'from src.core import polite_crawler'"
    run_test "Import src.core.database" "python -c 'from src.core import database'"
    run_test "Import src.managers.sharded_queue_manager" "python -c 'from src.managers import sharded_queue_manager'"
    run_test "Import src.monitoring.metrics" "python -c 'from src.monitoring import metrics'"
}

# ============================================
# Phase 3: 설정값 검증
# ============================================
run_config_tests() {
    print_phase "Phase 3: Configuration Tests"
    
    run_test "GLOBAL_SEMAPHORE_LIMIT 설정" \
        "python -c 'from config.settings import GLOBAL_SEMAPHORE_LIMIT; assert GLOBAL_SEMAPHORE_LIMIT > 0'"
    run_test "TCP_CONNECTOR_LIMIT 설정" \
        "python -c 'from config.settings import TCP_CONNECTOR_LIMIT; assert TCP_CONNECTOR_LIMIT > 0'"
    run_test "WORKER_THREADS 설정" \
        "python -c 'from config.settings import WORKER_THREADS; assert WORKER_THREADS > 0'"
    run_test "BATCH_SIZE 설정" \
        "python -c 'from config.settings import BATCH_SIZE; assert BATCH_SIZE > 0'"
    run_test "REQUEST_TIMEOUT 설정" \
        "python -c 'from config.settings import REQUEST_TIMEOUT; assert REQUEST_TIMEOUT > 0'"
}

# ============================================
# Phase 4: 파일 존재 확인
# ============================================
run_file_tests() {
    print_phase "Phase 4: File Existence Tests"
    
    # 핵심 파일
    run_test "Dockerfile 존재" "test -f Dockerfile"
    run_test "requirements.txt 존재" "test -f requirements.txt"
    run_test "docker-compose.yml 존재" "test -f docker-compose.yml"
    
    # K8s 매니페스트
    run_test "K8s namespace.yaml 존재" "test -f k8s/base/namespace.yaml"
    run_test "K8s deployment.yaml 존재" "test -f k8s/base/crawler-deployment.yaml"
    
    # 모니터링
    run_test "Prometheus config 존재" "test -f k8s/monitoring/prometheus/prometheus-configmap.yaml"
    run_test "Grafana config 존재" "test -f k8s/monitoring/grafana/grafana-deployment.yaml"
}

# ============================================
# Phase 5: CI/CD 워크플로우 확인
# ============================================
run_cicd_tests() {
    print_phase "Phase 5: CI/CD Workflow Tests"
    
    local workflow_dir=".github/workflows"
    
    if [ -d "$workflow_dir" ]; then
        run_test "CI workflow 존재" "test -f $workflow_dir/ci.yml"
        run_test "Docker build workflow 존재" "test -f $workflow_dir/docker-build.yml"
        run_test "Deploy workflow 존재" "test -f $workflow_dir/deploy-k8s.yml"
    else
        echo -e "  ${YELLOW}⚠️ SKIP${NC}: .github/workflows 디렉토리가 없습니다."
    fi
}

# ============================================
# Phase 6: pytest 실행
# ============================================
run_pytest() {
    local pytest_args=("$@")
    
    print_phase "Phase 6: pytest Execution"
    
    # pytest-json-report 확인
    if ! check_dependency "pytest_json_report" "pytest-json-report"; then
        echo -e "${RED}[Error] pytest-json-report 없이는 계속할 수 없습니다.${NC}"
        return 1
    fi
    
    # pytest 옵션
    local PYTEST_OPTS=(
        "--json-report"
        "--json-report-file=test_report.json"
        "--json-report-indent=2"
        "-v"
        "--tb=short"
    )
    
    # tests 디렉토리 확인
    if [ ! -d "tests" ]; then
        echo -e "  ${YELLOW}⚠️ SKIP${NC}: tests/ 디렉토리가 없습니다."
        return 0
    fi
    
    echo -e "  pytest 실행 중..."
    echo ""
    
    # 추가 인자가 있으면 전달
    if [ ${#pytest_args[@]} -gt 0 ]; then
        pytest "${PYTEST_OPTS[@]}" "${pytest_args[@]}"
    else
        pytest "${PYTEST_OPTS[@]}" tests/
    fi
    
    return $?
}

# ============================================
# Phase 7: 리포트 생성
# ============================================
run_report_generator() {
    print_phase "Phase 7: Report Generation"
    
    if [ ! -f "test_report.json" ]; then
        echo -e "  ${YELLOW}⚠️ SKIP${NC}: test_report.json이 없습니다. pytest를 먼저 실행하세요."
        return 1
    fi
    
    # 리포트 생성 스크립트 확인
    local report_script="src/utils/report_generator.py"
    
    if [ -f "$report_script" ]; then
        python "$report_script" --json test_report.json --output docs/TEST_REPORT.md
        return $?
    else
        echo -e "  ${YELLOW}⚠️ SKIP${NC}: $report_script가 없습니다."
        return 1
    fi
}

# ============================================
# 결과 요약
# ============================================
print_summary() {
    local pytest_exit=$1
    
    print_header "📊 Test Summary"
    
    # Quick check 결과
    if [ $TOTAL_TESTS -gt 0 ]; then
        echo -e "Quick Check Results:"
        echo -e "  Total:  $TOTAL_TESTS"
        echo -e "  Passed: ${GREEN}$PASSED_TESTS${NC}"
        echo -e "  Failed: ${RED}$FAILED_TESTS${NC}"
        
        if [ $TOTAL_TESTS -gt 0 ]; then
            local success_rate=$((PASSED_TESTS * 100 / TOTAL_TESTS))
            echo -e "  Rate:   ${success_rate}%"
        fi
        echo ""
    fi
    
    # pytest 결과 (JSON에서 추출)
    if [ -f "test_report.json" ]; then
        echo -e "pytest Results:"
        if command -v jq &> /dev/null; then
            local total=$(jq -r '.summary.total // 0' test_report.json)
            local passed=$(jq -r '.summary.passed // 0' test_report.json)
            local failed=$(jq -r '.summary.failed // 0' test_report.json)
            local skipped=$(jq -r '.summary.skipped // 0' test_report.json)
            local duration=$(jq -r '.duration // 0' test_report.json)
            
            echo -e "  Total:    ${total}개"
            echo -e "  Passed:   ${GREEN}${passed}개${NC}"
            echo -e "  Failed:   ${RED}${failed}개${NC}"
            echo -e "  Skipped:  ${YELLOW}${skipped}개${NC}"
            printf "  Duration: %.2f초\n" "$duration"
        else
            python3 -c "
import json
with open('test_report.json') as f:
    data = json.load(f)
    s = data.get('summary', {})
    print(f\"  Total:    {s.get('total', 0)}개\")
    print(f\"  Passed:   {s.get('passed', 0)}개\")
    print(f\"  Failed:   {s.get('failed', 0)}개\")
    print(f\"  Skipped:  {s.get('skipped', 0)}개\")
    print(f\"  Duration: {data.get('duration', 0):.2f}초\")
"
        fi
        echo ""
    fi
    
    # 최종 결과
    echo -e "${BLUE}======================================${NC}"
    
    if [ $FAILED_TESTS -eq 0 ] && [ "${pytest_exit:-0}" -eq 0 ]; then
        echo -e "${GREEN}✅ 모든 테스트 통과!${NC}"
        echo -e "   리포트: docs/TEST_REPORT.md"
        return 0
    else
        echo -e "${RED}❌ 일부 테스트 실패${NC}"
        echo -e "   상세 내용은 docs/TEST_REPORT.md를 확인하세요."
        return 1
    fi
}

# ============================================
# 메인 로직
# ============================================

print_header "🚀 Unified Test Runner"

# 명령어 파싱
MODE="full"
PYTEST_ARGS=()

while [[ $# -gt 0 ]]; do
    case $1 in
        --quick)
            MODE="quick"
            shift
            ;;
        --pytest)
            MODE="pytest"
            shift
            ;;
        --report)
            MODE="report"
            shift
            ;;
        --help|-h)
            echo "Usage: ./run_tests.sh [OPTIONS] [PYTEST_ARGS]"
            echo ""
            echo "Options:"
            echo "  --quick     빠른 검증만 (import, config, files)"
            echo "  --pytest    pytest만 실행"
            echo "  --report    기존 JSON으로 리포트만 생성"
            echo "  --help      이 도움말 표시"
            echo ""
            echo "Examples:"
            echo "  ./run_tests.sh                    # 전체 실행"
            echo "  ./run_tests.sh --quick            # 빠른 검증"
            echo "  ./run_tests.sh -k 'test_config'   # 특정 테스트만"
            echo "  ./run_tests.sh tests/test_db.py   # 특정 파일만"
            exit 0
            ;;
        *)
            PYTEST_ARGS+=("$1")
            shift
            ;;
    esac
done

PYTEST_EXIT=0

case $MODE in
    quick)
        run_syntax_check
        run_import_tests
        run_config_tests
        run_file_tests
        run_cicd_tests
        print_summary
        ;;
    pytest)
        run_pytest "${PYTEST_ARGS[@]}"
        PYTEST_EXIT=$?
        run_report_generator
        print_summary $PYTEST_EXIT
        ;;
    report)
        run_report_generator
        ;;
    full)
        # Phase 1-5: Quick checks
        run_syntax_check
        run_import_tests
        run_config_tests
        run_file_tests
        run_cicd_tests
        
        # Phase 6-7: pytest + report
        run_pytest "${PYTEST_ARGS[@]}"
        PYTEST_EXIT=$?
        run_report_generator
        
        print_summary $PYTEST_EXIT
        ;;
esac

# 종료 코드 결정
if [ $FAILED_TESTS -gt 0 ] || [ $PYTEST_EXIT -ne 0 ]; then
    exit 1
fi
exit 0
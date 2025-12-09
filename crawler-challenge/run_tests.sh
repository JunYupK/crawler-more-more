#!/bin/bash
# CI/CD Pipeline Test Runner
# Phase 1-10 전체 기능 점검 스크립트

set -e  # Exit on error

echo "======================================"
echo "🚀 CI/CD Test Runner"
echo "======================================"
echo ""

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Test counters
TOTAL_TESTS=0
PASSED_TESTS=0
FAILED_TESTS=0

# Function to run a test
run_test() {
    local test_name=$1
    local test_command=$2

    echo "Running: $test_name"
    TOTAL_TESTS=$((TOTAL_TESTS + 1))

    if eval "$test_command" > /dev/null 2>&1; then
        echo -e "${GREEN}✅ PASS${NC}: $test_name"
        PASSED_TESTS=$((PASSED_TESTS + 1))
        return 0
    else
        echo -e "${RED}❌ FAIL${NC}: $test_name"
        FAILED_TESTS=$((FAILED_TESTS + 1))
        return 1
    fi
}

# 1. Python Syntax Check
echo ""
echo "=== Phase 1: Python Syntax Check ==="
run_test "Python syntax check" "python -m py_compile *.py"

# 2. Import Tests
echo ""
echo "=== Phase 2: Import Tests ==="
run_test "Import polite_crawler" "python -c 'import polite_crawler'"
run_test "Import multithreaded_crawler" "python -c 'import multithreaded_crawler'"
run_test "Import distributed_crawler" "python -c 'import distributed_crawler'"
run_test "Import config.settings" "python -c 'from config import settings'"
run_test "Import monitoring.metrics" "python -c 'from monitoring import metrics'"

# 3. Configuration Tests
echo ""
echo "=== Phase 3: Configuration Tests ==="
run_test "Check SEMAPHORE_LIMIT=200" "python -c 'from config.settings import GLOBAL_SEMAPHORE_LIMIT; assert GLOBAL_SEMAPHORE_LIMIT == 200'"
run_test "Check TCP_CONNECTOR_LIMIT=300" "python -c 'from config.settings import TCP_CONNECTOR_LIMIT; assert TCP_CONNECTOR_LIMIT == 300'"
run_test "Check WORKER_THREADS=16" "python -c 'from config.settings import WORKER_THREADS; assert WORKER_THREADS == 16'"

# 4. File Existence Tests
echo ""
echo "=== Phase 4: File Existence Tests ==="
run_test "Dockerfile exists" "test -f Dockerfile"
run_test "requirements.txt exists" "test -f requirements.txt"
run_test "K8s namespace.yaml exists" "test -f k8s/base/namespace.yaml"
run_test "K8s deployment.yaml exists" "test -f k8s/base/crawler-deployment.yaml"
run_test "KEDA scaledobject.yaml exists" "test -f k8s/autoscaling/keda-scaledobject.yaml"
run_test "Prometheus config exists" "test -f k8s/monitoring/prometheus/prometheus-configmap.yaml"
run_test "Grafana config exists" "test -f k8s/monitoring/grafana/grafana-deployment.yaml"

# 5. CI/CD Workflow Tests
echo ""
echo "=== Phase 5: CI/CD Workflow Tests ==="
run_test "CI workflow exists" "test -f ../.github/workflows/ci.yml"
run_test "Docker build workflow exists" "test -f ../.github/workflows/docker-build.yml"
run_test "Deploy workflow exists" "test -f ../.github/workflows/deploy-k8s.yml"
run_test "PR automation workflow exists" "test -f ../.github/workflows/pr-automation.yml"
run_test "Release workflow exists" "test -f ../.github/workflows/release.yml"

# 6. Comprehensive Test Suite
echo ""
echo "=== Phase 6: Comprehensive Test Suite ==="
if [ -f "tests/test_all_phases.py" ]; then
    python tests/test_all_phases.py
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✅ PASS${NC}: Comprehensive test suite"
        PASSED_TESTS=$((PASSED_TESTS + 1))
    else
        echo -e "${YELLOW}⚠️ WARN${NC}: Some comprehensive tests failed"
    fi
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
fi

# Summary
echo ""
echo "======================================"
echo "📊 Test Summary"
echo "======================================"
echo "Total Tests: $TOTAL_TESTS"
echo -e "Passed: ${GREEN}$PASSED_TESTS${NC}"
echo -e "Failed: ${RED}$FAILED_TESTS${NC}"

SUCCESS_RATE=$((PASSED_TESTS * 100 / TOTAL_TESTS))
echo "Success Rate: $SUCCESS_RATE%"
echo "======================================"

# Exit code based on success rate
if [ $SUCCESS_RATE -ge 90 ]; then
    echo -e "${GREEN}✅ Tests PASSED (>= 90%)${NC}"
    exit 0
elif [ $SUCCESS_RATE -ge 70 ]; then
    echo -e "${YELLOW}⚠️ Tests WARNING (70-89%)${NC}"
    exit 1
else
    echo -e "${RED}❌ Tests FAILED (< 70%)${NC}"
    exit 2
fi

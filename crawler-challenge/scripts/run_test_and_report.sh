#!/bin/bash
# 크롤링 테스트 실행 후 AI 리포트 자동 생성 스크립트

set -e  # 에러 발생 시 즉시 중단

echo "=================================================="
echo "크롤링 테스트 및 AI 리포트 생성"
echo "=================================================="
echo ""

# 1. 현재 위치 확인 및 이동
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_DIR"

echo "📂 작업 디렉토리: $PROJECT_DIR"
echo ""

# 2. 환경 변수 확인
if [ ! -f ".env" ]; then
    echo "⚠️  경고: .env 파일이 없습니다!"
    echo "   GEMINI_API_KEY를 설정하세요."
    exit 1
fi

if ! grep -q "GEMINI_API_KEY" .env; then
    echo "⚠️  경고: .env에 GEMINI_API_KEY가 없습니다!"
    exit 1
fi

echo "✅ 환경 변수 확인 완료"
echo ""

# 3. pytest 테스트 실행
echo "=================================================="
echo "1단계: pytest 테스트 실행"
echo "=================================================="
echo ""

python -m pytest --json-report --json-report-file=test_report.json -v

if [ $? -eq 0 ]; then
    echo ""
    echo "✅ 테스트 실행 완료 (test_report.json 생성)"
else
    echo ""
    echo "⚠️  테스트 실행 중 일부 실패가 있었지만 리포트 생성을 계속합니다."
fi
echo ""

# 4. AI 리포트 생성
echo "=================================================="
echo "2단계: AI 리포트 생성 (Gemini API)"
echo "=================================================="
echo ""

# 시간 범위 파라미터 (기본값: 60분)
TIME_RANGE=${1:-60}

python scripts/generate_ai_report.py --time-range $TIME_RANGE

if [ $? -eq 0 ]; then
    echo ""
    echo "=================================================="
    echo "✅ 모든 작업 완료!"
    echo "=================================================="
    echo ""
    echo "📊 생성된 리포트 확인:"
    echo "   $ ls -lt docs/reports/"
    echo ""
    echo "📁 최신 리포트:"
    LATEST_REPORT=$(ls -t docs/reports/report_*.md 2>/dev/null | head -1)
    if [ -n "$LATEST_REPORT" ]; then
        echo "   $LATEST_REPORT"
        echo ""
        echo "📖 리포트 보기:"
        echo "   $ cat $LATEST_REPORT"
    fi
else
    echo ""
    echo "❌ AI 리포트 생성 실패"
    exit 1
fi

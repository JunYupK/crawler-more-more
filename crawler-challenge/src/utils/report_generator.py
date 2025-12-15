#!/usr/bin/env python3
"""
테스트 결과 JSON을 읽어서 docs/TEST_REPORT.md 파일을 생성하는 스크립트.
pytest-json-report 플러그인과 함께 사용합니다.

Usage:
    python src/utils/report_generator.py
    python src/utils/report_generator.py --json test_report.json --output docs/TEST_REPORT.md
"""

import json
import os
import sys
import platform
import argparse
import shutil
from datetime import datetime
from pathlib import Path


def get_environment_info() -> dict:
    """실행 환경 정보를 수집합니다."""
    return {
        'python_version': platform.python_version(),
        'os': f"{platform.system()} {platform.release()}",
        'machine': platform.machine(),
        'cwd': os.getcwd(),
    }


def extract_error_message(test: dict) -> str:
    """테스트 결과에서 에러 메시지를 추출합니다."""
    # call 단계에서 실패한 경우
    if 'call' in test:
        call = test['call']
        if 'crash' in call:
            return call['crash'].get('message', 'No error message')
        if 'longrepr' in call:
            return call['longrepr'][:500] + '...' if len(call.get('longrepr', '')) > 500 else call.get('longrepr', '')
    
    # setup 단계에서 실패한 경우
    if 'setup' in test:
        setup = test['setup']
        if 'crash' in setup:
            return setup['crash'].get('message', 'Setup failed')
        if 'longrepr' in setup:
            return setup['longrepr'][:500] + '...' if len(setup.get('longrepr', '')) > 500 else setup.get('longrepr', '')
    
    # teardown 단계에서 실패한 경우
    if 'teardown' in test:
        teardown = test['teardown']
        if 'crash' in teardown:
            return teardown['crash'].get('message', 'Teardown failed')
    
    return 'No error message available'


def get_test_duration(test: dict) -> float:
    """테스트의 총 소요 시간을 계산합니다."""
    duration = 0.0
    for phase in ['setup', 'call', 'teardown']:
        if phase in test and 'duration' in test[phase]:
            duration += test[phase]['duration']
    return duration


def backup_existing_report(output_path: str) -> None:
    """기존 리포트가 있으면 백업합니다."""
    if os.path.exists(output_path):
        backup_dir = os.path.join(os.path.dirname(output_path), 'backup')
        os.makedirs(backup_dir, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = os.path.basename(output_path)
        name, ext = os.path.splitext(filename)
        backup_path = os.path.join(backup_dir, f"{name}_{timestamp}{ext}")
        
        shutil.copy2(output_path, backup_path)
        print(f"[Backup] 기존 리포트 백업: {backup_path}")


def generate_report(json_path: str = 'test_report.json', 
                   output_path: str = 'docs/TEST_REPORT.md',
                   backup: bool = True) -> bool:
    """
    테스트 결과 JSON을 읽어 Markdown 리포트를 생성합니다.
    
    Args:
        json_path: pytest-json-report가 생성한 JSON 파일 경로
        output_path: 생성할 Markdown 리포트 경로
        backup: 기존 리포트 백업 여부
    
    Returns:
        성공 시 True, 실패 시 False
    """
    
    # 1. JSON 파일 확인 및 읽기
    if not os.path.exists(json_path):
        print(f"[Error] '{json_path}' 파일을 찾을 수 없습니다.")
        print("        pytest --json-report --json-report-file=test_report.json 명령어로 테스트를 먼저 실행해주세요.")
        return False

    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        print(f"[Error] JSON 파싱 실패: {e}")
        return False
    except Exception as e:
        print(f"[Error] 파일 읽기 실패: {e}")
        return False

    # 2. 기존 리포트 백업
    if backup:
        backup_existing_report(output_path)

    # 3. 데이터 파싱 및 통계 계산
    summary = data.get('summary', {})
    passed = summary.get('passed', 0)
    failed = summary.get('failed', 0)
    skipped = summary.get('skipped', 0)
    error = summary.get('error', 0)
    total = summary.get('total', passed + failed + skipped + error)
    duration = data.get('duration', 0.0)
    
    success_rate = (passed / total * 100) if total > 0 else 0.0
    test_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # 환경 정보
    env_info = get_environment_info()
    
    # 테스트 분류
    tests = data.get('tests', [])
    passed_tests = [t for t in tests if t.get('outcome') == 'passed']
    failed_tests = [t for t in tests if t.get('outcome') == 'failed']
    skipped_tests = [t for t in tests if t.get('outcome') == 'skipped']
    error_tests = [t for t in tests if t.get('outcome') == 'error']

    # 4. Markdown 콘텐츠 작성
    md_content = f"""# 🧪 자동 생성된 테스트 보고서

> 이 문서는 `pytest-json-report`와 `report_generator.py`에 의해 자동 생성되었습니다.

## 📊 테스트 요약

| 항목 | 결과 |
|------|------|
| **실행 일시** | {test_date} |
| **총 소요 시간** | {duration:.2f}초 |
| **총 테스트** | {total}개 |
| **성공** | {passed}개 ✅ |
| **실패** | {failed}개 ❌ |
| **에러** | {error}개 💥 |
| **스킵** | {skipped}개 ⚠️ |
| **성공률** | **{success_rate:.1f}%** |

## 🖥️ 실행 환경

| 항목 | 값 |
|------|-----|
| Python | {env_info['python_version']} |
| OS | {env_info['os']} |
| Architecture | {env_info['machine']} |
| Working Directory | `{env_info['cwd']}` |

---

"""

    # 실패한 테스트 상세
    if failed_tests or error_tests:
        md_content += "## ❌ 실패 항목 분석\n\n"
        
        for test in failed_tests + error_tests:
            nodeid = test.get('nodeid', 'unknown')
            outcome = test.get('outcome', 'unknown')
            test_duration = get_test_duration(test)
            error_msg = extract_error_message(test)
            
            icon = "❌" if outcome == 'failed' else "💥"
            md_content += f"""### {icon} `{nodeid}`

- **결과**: {outcome}
- **소요 시간**: {test_duration:.3f}초

```
{error_msg}
```

"""
    
    # 스킵된 테스트
    if skipped_tests:
        md_content += "## ⚠️ 스킵된 테스트\n\n"
        md_content += "| 테스트 | 사유 |\n"
        md_content += "|--------|------|\n"
        
        for test in skipped_tests:
            nodeid = test.get('nodeid', 'unknown')
            # 스킵 사유 추출
            reason = 'N/A'
            if 'setup' in test and 'longrepr' in test['setup']:
                reason = test['setup']['longrepr'][:100]
            md_content += f"| `{nodeid}` | {reason} |\n"
        
        md_content += "\n"

    # 성공한 테스트 요약
    if passed_tests:
        md_content += "## ✅ 성공한 테스트\n\n"
        md_content += "<details>\n<summary>성공한 테스트 목록 (클릭하여 펼치기)</summary>\n\n"
        md_content += "| 테스트 | 소요 시간 |\n"
        md_content += "|--------|----------|\n"
        
        # 소요 시간 기준 정렬 (느린 순)
        passed_tests_sorted = sorted(passed_tests, key=get_test_duration, reverse=True)
        
        for test in passed_tests_sorted:
            nodeid = test.get('nodeid', 'unknown')
            test_duration = get_test_duration(test)
            md_content += f"| `{nodeid}` | {test_duration:.3f}s |\n"
        
        md_content += "\n</details>\n\n"

    # 결과 요약
    md_content += "---\n\n"
    
    if total == 0:
        md_content += "⚠️ **실행된 테스트가 없습니다.**\n"
    elif failed == 0 and error == 0:
        md_content += "🎉 **모든 테스트가 성공했습니다!**\n"
    else:
        md_content += f"⚠️ **{failed + error}개의 테스트가 실패했습니다.** 위의 상세 내용을 확인해주세요.\n"

    # 5. 파일 저장
    try:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(md_content)
        print(f"[OK] 리포트 생성 완료: {output_path}")
        return True
    except Exception as e:
        print(f"[Error] 리포트 저장 실패: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description='pytest-json-report 결과를 Markdown 리포트로 변환합니다.'
    )
    parser.add_argument(
        '--json', '-j',
        default='test_report.json',
        help='입력 JSON 파일 경로 (기본값: test_report.json)'
    )
    parser.add_argument(
        '--output', '-o',
        default='docs/TEST_REPORT.md',
        help='출력 Markdown 파일 경로 (기본값: docs/TEST_REPORT.md)'
    )
    parser.add_argument(
        '--no-backup',
        action='store_true',
        help='기존 리포트 백업 안 함'
    )
    
    args = parser.parse_args()
    
    success = generate_report(
        json_path=args.json,
        output_path=args.output,
        backup=not args.no_backup
    )
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
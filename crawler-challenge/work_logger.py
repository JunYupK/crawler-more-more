#!/usr/bin/env python3
"""
자동 작업 로깅 및 Git 커밋 시스템
- README 자동 업데이트
- Git 커밋 자동화
- 작업 진행상황 추적
"""

import os
import subprocess
import datetime
from typing import List, Dict, Any
import json


class WorkLogger:
    def __init__(self, project_dir: str = "."):
        self.project_dir = project_dir
        self.readme_path = os.path.join(project_dir, "README.md")
        self.log_file = os.path.join(project_dir, "work_progress.json")

    def log_work(self, title: str, description: str, details: Dict[str, Any] = None):
        """작업 내용을 로깅하고 README에 추가"""
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # 작업 로그 저장
        work_entry = {
            "timestamp": timestamp,
            "title": title,
            "description": description,
            "details": details or {}
        }

        self._save_to_log_file(work_entry)
        self._update_readme(work_entry)

        print(f"[OK] [{timestamp}] {title}")
        print(f"   {description}")

    def _save_to_log_file(self, entry: Dict[str, Any]):
        """JSON 로그 파일에 작업 내용 저장"""
        try:
            if os.path.exists(self.log_file):
                with open(self.log_file, 'r', encoding='utf-8') as f:
                    logs = json.load(f)
            else:
                logs = []

            logs.append(entry)

            with open(self.log_file, 'w', encoding='utf-8') as f:
                json.dump(logs, f, indent=2, ensure_ascii=False)

        except Exception as e:
            print(f"[WARN] 로그 파일 저장 실패: {e}")

    def _update_readme(self, entry: Dict[str, Any]):
        """README.md에 작업 내용 추가"""
        try:
            # README 읽기
            if os.path.exists(self.readme_path):
                with open(self.readme_path, 'r', encoding='utf-8') as f:
                    content = f.read()
            else:
                content = "# Crawler Challenge\n\n"

            # 새 섹션 추가
            new_section = f"""
## {entry['title']} ({entry['timestamp']})

{entry['description']}

"""

            # 세부 정보가 있으면 추가
            if entry['details']:
                new_section += "**세부 정보:**\n"
                for key, value in entry['details'].items():
                    new_section += f"- {key}: {value}\n"
                new_section += "\n"

            # README 끝에 추가
            content += new_section

            # README 저장
            with open(self.readme_path, 'w', encoding='utf-8') as f:
                f.write(content)

        except Exception as e:
            print(f"[WARN] README 업데이트 실패: {e}")

    def auto_commit(self, message: str = None):
        """변경사항을 자동으로 Git에 커밋"""
        try:
            os.chdir(self.project_dir)

            # Git 상태 확인
            result = subprocess.run(['git', 'status', '--porcelain'],
                                  capture_output=True, text=True)

            if not result.stdout.strip():
                print("[INFO] 커밋할 변경사항이 없습니다.")
                return False

            # 모든 변경사항 스테이징
            subprocess.run(['git', 'add', '.'], check=True)

            # 커밋 메시지 생성
            if not message:
                timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
                message = f"Auto-commit: Work progress update ({timestamp})"

            # 커밋 실행
            subprocess.run(['git', 'commit', '-m', message], check=True)

            print(f"[OK] Git 커밋 완료: {message}")
            return True

        except subprocess.CalledProcessError as e:
            print(f"[WARN] Git 커밋 실패: {e}")
            return False
        except Exception as e:
            print(f"[WARN] 예상치 못한 오류: {e}")
            return False

    def log_and_commit(self, title: str, description: str,
                      details: Dict[str, Any] = None,
                      commit_message: str = None):
        """작업 로깅과 Git 커밋을 한번에 실행"""
        self.log_work(title, description, details)

        if not commit_message:
            commit_message = f"{title}: {description}"

        self.auto_commit(commit_message)

    def get_work_history(self) -> List[Dict[str, Any]]:
        """작업 히스토리 조회"""
        try:
            if os.path.exists(self.log_file):
                with open(self.log_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            return []
        except Exception as e:
            print(f"[WARN] 작업 히스토리 조회 실패: {e}")
            return []


def main():
    """테스트 및 시연"""
    logger = WorkLogger(".")

    # 시스템 초기화 로깅
    logger.log_and_commit(
        title="자동 작업 로깅 시스템 구축",
        description="README 자동 업데이트와 Git 커밋 자동화 시스템을 구현했습니다.",
        details={
            "기능": "작업 로깅, README 업데이트, Git 자동 커밋",
            "파일": "work_logger.py, work_progress.json",
            "다음 단계": "Tranco Top 1M 다운로드 시스템 구현"
        }
    )

    print("\n[READY] 자동 작업 로깅 시스템이 준비되었습니다!")
    print("이제 모든 작업이 자동으로 README에 기록되고 Git에 커밋됩니다.")


if __name__ == "__main__":
    main()
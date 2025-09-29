from work_logger import WorkLogger

# The script is inside crawler-challenge, so the project_dir is '.'
logger = WorkLogger(project_dir='.') 

logger.log_and_commit(
    title='Fix: start_crawler.bat 경로 문제 해결', 
    description='start_crawler.bat 파일이 어디서 실행되든 항상 올바른 경로에서 실행되도록 스크립트 맨 위에 `cd /d %~dp0` 명령어를 추가했습니다.', 
    details={
        '수정 파일': 'start_crawler.bat', 
        '수정 내용': '스크립트 최상단에 `cd /d %~dp0` 추가', 
        '기대 효과': '경로 문제 없이 크롤러 정상 실행'
    }, 
    commit_message='Fix(script): `start_crawler.bat` 실행 경로 문제 해결'
)
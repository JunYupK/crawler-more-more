@echo off
cd /d %~dp0

echo Starting 24-Hour Resilient Crawler...
echo ====================================

REM Docker services 시작
echo Starting Docker services...
docker-compose up -d

REM 잠시 대기 (서비스 시작 완료 대기)
echo Waiting for services to start...
timeout /t 10

REM Python 의존성 확인
echo Checking Python dependencies...
pip install -r requirements.txt

REM 크롤러 실행
echo Starting resilient crawler...
python resilient_runner.py

echo Crawler stopped.
pause
# AI 보고서 생성 가이드

이 문서는 크롤링 완료 후 **윈도우 데스크탑**에서 AI 보고서를 생성하는 방법을 설명합니다.

---

## 📋 목차

- [사전 준비](#사전-준비)
- [보고서 생성 방법](#보고서-생성-방법)
- [고급 옵션](#고급-옵션)
- [문제 해결](#문제-해결)

---

## 🔧 사전 준비

### 1. Python 환경

Python 3.10 이상이 설치되어 있어야 합니다.

```bash
python --version
```

### 2. 필요한 패키지 설치

```bash
pip install google-generativeai prometheus-api-client python-dotenv
```

### 3. 환경 변수 설정

프로젝트 루트에 `.env` 파일을 생성하고 다음 내용을 추가합니다:

```env
# Gemini API 키 (필수)
GEMINI_API_KEY=your_api_key_here

# Prometheus URL (선택, 기본값: localhost:9090)
PROMETHEUS_URL=http://localhost:9090
```

**Gemini API 키 발급 방법:**
1. [Google AI Studio](https://aistudio.google.com/app/apikey)에 접속
2. "Get API key" 클릭
3. 생성된 키를 복사하여 `.env` 파일에 추가

---

## 📊 보고서 생성 방법

### 기본 사용 (자동 시간 범위)

크롤링이 완료된 후, 윈도우 명령 프롬프트나 PowerShell에서 다음 명령을 실행합니다:

```bash
cd crawler-challenge
python scripts/generate_ai_report.py
```

**동작:**
- Prometheus에서 크롤링 시작/종료 시간을 자동으로 조회합니다
- 실제 크롤링 시간만큼의 메트릭을 수집합니다
- AI 분석 보고서를 생성합니다

### 수동 시간 범위 지정

특정 시간 범위의 메트릭을 조회하고 싶다면:

```bash
python scripts/generate_ai_report.py --time-range 120
```

위 명령은 **최근 120분**간의 메트릭을 수집합니다.

---

## 🎛️ 고급 옵션

### 모든 옵션 확인

```bash
python scripts/generate_ai_report.py --help
```

### 주요 옵션

| 옵션 | 기본값 | 설명 |
|------|--------|------|
| `--prometheus-url` | `localhost:9090` | Prometheus 서버 URL |
| `--time-range` | 자동 계산 | 메트릭 수집 시간 범위 (분) |
| `--model` | `gemini-2.5-flash` | 사용할 Gemini 모델 |
| `--test-report` | `test_report.json` | Pytest 결과 JSON 파일 경로 |
| `--output-dir` | `docs/reports` | 보고서 출력 디렉토리 |

### 사용 예시

```bash
# Prometheus URL 지정
python scripts/generate_ai_report.py --prometheus-url http://192.168.1.100:9090

# 다른 모델 사용
python scripts/generate_ai_report.py --model gemini-1.5-pro

# 모든 옵션 조합
python scripts/generate_ai_report.py \
  --prometheus-url http://192.168.1.100:9090 \
  --time-range 180 \
  --model gemini-2.5-flash \
  --output-dir ./my_reports
```

---

## 📁 생성된 보고서 확인

보고서는 다음 위치에 저장됩니다:

```
crawler-challenge/docs/reports/report_YYYYMMDD_HHMM.md
```

**파일명 형식:**
- `report_20251230_1530.md` → 2025년 12월 30일 15:30 생성

**보고서 내용:**
- 성능 요약 (Performance Summary)
- 변화 분석 (Change Analysis) - 이전 보고서가 있는 경우
- 병목 구간 분석 (Bottleneck Analysis)
- 에러 분석 (Error Analysis)
- 개선 제안 (Recommendations)

---

## 🔍 문제 해결

### 1. Prometheus 연결 실패

**증상:**
```
❌ Prometheus 연결 실패. 보고서를 생성할 수 없습니다.
```

**해결 방법:**
1. Prometheus가 실행 중인지 확인:
   - 웹 브라우저에서 `http://localhost:9090` 접속

2. 방화벽 확인:
   - Windows Defender 방화벽에서 포트 9090 허용

3. URL 확인:
   - Tailscale 사용 시: `--prometheus-url http://100.105.22.101:9090`

### 2. GEMINI_API_KEY 오류

**증상:**
```
GEMINI_API_KEY 환경 변수가 설정되지 않았습니다.
```

**해결 방법:**
1. `.env` 파일이 올바른 위치에 있는지 확인 (`crawler-challenge/.env`)
2. `.env` 파일에 `GEMINI_API_KEY=실제키` 형식으로 작성되었는지 확인
3. API 키가 유효한지 확인 ([Google AI Studio](https://aistudio.google.com/app/apikey)에서 확인)

### 3. 메트릭 데이터 없음

**증상:**
```
⚠️ 세션 시간 조회 실패, 기본값 사용: 60분
```

**해결 방법:**
1. Mac의 master가 실행 중인지 확인:
   ```bash
   # Mac에서 확인
   docker ps | grep crawler-master
   ```

2. 크롤링이 완료되었는지 확인:
   - master 로그에서 "크롤링 세션 종료 시간 기록" 메시지 확인

3. 수동으로 시간 범위 지정:
   ```bash
   python scripts/generate_ai_report.py --time-range 180
   ```

### 4. 보고서가 잘림

**증상:**
보고서 내용이 중간에 끊겨 있음

**해결 방법:**
1. 로그에서 경고 확인:
   ```
   ⚠️ 응답이 max_output_tokens에 도달하여 잘렸을 수 있습니다.
   ```

2. 현재 `max_output_tokens=8192`로 설정되어 있으며, 필요시 코드 수정 가능

### 5. Rate limit 초과

**증상:**
```
Rate limit 초과. 30초 후 재시도...
```

**해결 방법:**
- 자동으로 재시도됩니다 (최대 3회)
- Gemini API 무료 할당량을 확인하세요
- 필요시 유료 플랜으로 업그레이드

---

## 💡 팁

### 여러 보고서 생성

크롤링 완료 후 언제든지 보고서를 **여러 번** 생성할 수 있습니다:

```bash
# 첫 번째 보고서
python scripts/generate_ai_report.py

# 30분 후 두 번째 보고서 (비교 분석 포함)
python scripts/generate_ai_report.py

# 특정 시간대만 분석
python scripts/generate_ai_report.py --time-range 60
```

### 이전 보고서와 비교

두 번째 보고서부터는 **이전 보고서와 자동으로 비교 분석**됩니다:
- 성능 개선/악화 지표
- 변화 추이 분석
- 화살표 (↑, ↓, →)로 변화 표시

### Mac 크롤러 유지

Mac의 master는 크롤링 완료 후에도 **대기 모드**로 유지됩니다:
- 메트릭 서버 계속 실행 (port 8000)
- Prometheus가 실시간 메트릭 수집 가능
- 종료하려면 `Ctrl+C` 입력

---

## 📚 참고

- Prometheus 메트릭은 기본적으로 **30일간** 보존됩니다
- 오래된 크롤링 세션의 보고서도 생성 가능 (데이터가 남아있다면)
- 보고서는 Markdown 형식이므로 GitHub, Notion 등에서 바로 읽기 가능

---

**문제가 해결되지 않으면:**
- GitHub Issues에 문의
- 로그 파일 확인: `crawler-challenge/logs/`

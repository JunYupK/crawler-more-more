# 작업 이력

## 2024-12-28

### AI 기반 크롤링 리포트 자동화 시스템 개선

**작업 내용:**
- `scripts/generate_ai_report.py`에 Mac 환경 자동 감지 기능 추가
- Mac에서 실행 시 Prometheus URL을 자동으로 `localhost:9090`으로 설정
- 기타 환경에서는 `.env`의 `PROMETHEUS_URL` 사용 (Tailscale)
- `platform` 모듈을 활용한 플랫폼 감지 로직 구현
- `get_prometheus_url()` 함수 추가하여 실행 환경에 따른 URL 자동 결정
- 테스트 실행 및 리포트 생성을 한 번에 처리하는 `run_test_and_report.sh` 스크립트 작성

**결과:**
- Mac과 Windows 환경에서 각각 적절한 Prometheus URL을 자동으로 사용
- pytest 테스트 결과를 Gemini API로 분석하여 `docs/reports/` 디렉토리에 Markdown 리포트 자동 생성
- Docker 환경에서 크롤링 실행, 리포트는 Mac 로컬에 저장되도록 구성 완료

### Prometheus 메트릭 쿼리 개선 및 이전 리포트 비교 분석 기능 추가

**작업 내용:**
- Prometheus에서 실제 수집 중인 크롤러 메트릭 확인 (23개 메트릭 발견)
- `generate_ai_report.py`의 메트릭 쿼리를 실제 메트릭 이름에 맞게 수정
  - 크롤러 작업 메트릭: `crawler_tasks_completed_total`, `crawler_tasks_failed_total` 등
  - 크롤러 큐 메트릭: `crawler_queue_pending`, `crawler_queue_processing` 등
  - 크롤러 시스템 리소스: `crawler_system_cpu_percent`, `crawler_system_memory_percent`
  - 처리 지연 시간: `crawler_process_latency_seconds_*` (히스토그램)
  - PostgreSQL 통계: `pg_crawler_throughput_pages_per_second`, `pg_crawler_stats_*` 등
- postgres-exporter의 커스텀 메트릭 활용 추가
- 이전 리포트 참고 기능 구현
  - `ReportSaver.get_latest_report()` 메서드 추가: 가장 최근 리포트 자동 로드
  - `GeminiReportGenerator`에서 이전 리포트와 비교 분석하도록 프롬프트 수정
  - 첫 리포트인 경우 일반 분석, 이후부터는 변화 분석 수행

**결과:**
- 메트릭 수집 개수: 4개 → 23개로 증가
- AI 리포트 품질 대폭 개선: 실제 크롤링 성능 지표 기반 분석 가능
- 리포트 간 비교 분석으로 성능 변화 추이 자동 파악
- 개선/악화 지표를 화살표(↑, ↓, →)로 시각화하여 한눈에 파악 가능

---

## 상세 작업 내역 (2024-12-28)

### 1단계: 프로젝트 초기 분석 및 구조 파악
- `.claude/CLAUDE.md` 메모리 뱅크 최적화 요청 받음
- 프로젝트 구조 확인: `crawler-challenge/` 디렉토리 구조 분석
- 기존 `scripts/generate_ai_report.py` 파일 발견 및 분석

### 2단계: AI 리포트 자동화 시스템 구축
**목표**: pytest 테스트 결과와 Prometheus 메트릭을 Gemini AI로 분석하여 자동 리포트 생성

**구현 사항:**
1. **플랫폼 자동 감지 로직**
   - `platform.system()` 사용하여 Mac(Darwin) 환경 감지
   - Mac: `localhost:9090` (로컬 Docker Prometheus)
   - 기타: `.env`의 `PROMETHEUS_URL` (Tailscale 원격 접속)
   - `get_prometheus_url()` 함수 구현

2. **환경 변수 관리**
   - `.env` 파일에서 `GEMINI_API_KEY`, `PROMETHEUS_URL` 로드
   - `python-dotenv` 활용한 안전한 API 키 관리

3. **자동화 스크립트**
   - `run_test_and_report.sh`: pytest 실행 → AI 리포트 생성을 원클릭으로 처리
   - 환경 변수 검증, 에러 핸들링 포함

### 3단계: Prometheus 메트릭 문제 발견 및 해결
**문제**: AI 리포트에 "크롤러 성능 지표가 없음"이라는 내용이 계속 생성됨

**원인 분석:**
- `generate_ai_report.py`가 존재하지 않는 메트릭 이름 쿼리
- 실제 Prometheus에는 23개의 크롤러 메트릭이 수집 중이었음

**해결 과정:**
1. Prometheus API 호출하여 실제 메트릭 목록 확인
   ```bash
   curl "http://100.105.22.101:9090/api/v1/label/__name__/values"
   ```

2. 발견된 실제 메트릭:
   - `crawler_tasks_completed_total`, `crawler_tasks_failed_total`
   - `crawler_queue_pending`, `crawler_queue_processing`
   - `crawler_system_cpu_percent`, `crawler_system_memory_percent`
   - `crawler_process_latency_seconds_*` (히스토그램)
   - `pg_crawler_throughput_pages_per_second`
   - `pg_crawler_stats_*` (PostgreSQL 통계)
   - `pg_crawler_dlq_*` (Dead Letter Queue)

3. 메트릭 쿼리 수정
   - 23개의 실제 메트릭에 맞게 쿼리 재작성
   - PromQL 쿼리 최적화 (rate, histogram_quantile 등)

**결과:**
- 수집 메트릭: 4개 → 23개
- 리포트 품질 대폭 향상: 실제 크롤링 성능, 큐 상태, 처리량 등 구체적 분석 가능

### 4단계: 이전 리포트 비교 분석 기능 추가
**목표**: 시간대별 독립 리포트가 아닌, 이전 리포트와 비교하여 변화 추이 분석

**구현 사항:**
1. **최근 리포트 자동 탐지**
   - `ReportSaver.get_latest_report()` 메서드 추가
   - `docs/reports/report_*.md` 패턴 파일 검색
   - 수정 시간 기준 정렬하여 최신 파일 자동 선택

2. **비교 분석 프롬프트 구현**
   - 첫 리포트: "첫 번째 리포트입니다. 현재 상태만 분석합니다."
   - 2번째 이후: "이전 리포트와 비교하여 변화 사항을 분석해주세요."
   - Gemini에 이전 리포트 전문을 전달하여 컨텍스트 제공

3. **변화 분석 요청사항**
   - 성능 요약에 이전 값 vs 현재 값 비교 표 생성
   - 개선/악화 지표를 화살표(↑, ↓, →)로 표시
   - 변화 원인 추정 및 새로운 병목 구간 감지

**코드 변경:**
```python
# GeminiReportGenerator.generate_report()
def generate_report(self, metrics, test_results, previous_report=None):
    prompt = self._build_prompt(metrics, test_results, previous_report)
    # ...

# main()
saver = ReportSaver(args.output_dir)
previous_report = saver.get_latest_report()  # 이전 리포트 로드
report_content = generator.generate_report(metrics, test_summary, previous_report)
```

**실행 로그 예시:**
```
[3/5] 이전 리포트 확인 중...
이전 리포트 발견: report_20251228_1854.md
  - 이전 리포트를 참고하여 비교 분석 수행
```

### 5단계: 문서화 및 CLAUDE.md 업데이트
**작업 내용:**
1. `.claude/CLAUDE.md` 업데이트
   - "주요 디렉토리 구조"에 `scripts/`, `docs/reports/` 추가
   - "AI 기반 성능 분석 시스템" 섹션 추가
   - 자동 리포트 생성, 플랫폼 자동 감지, 비교 분석 기능 설명

2. `docs/REPORT.md` 업데이트
   - 오늘 수행한 모든 작업 상세 기록
   - 문제 발견 → 원인 분석 → 해결 과정 문서화

### 최종 결과물

**생성된 파일:**
- `scripts/generate_ai_report.py` (수정)
- `scripts/run_test_and_report.sh` (신규)
- `docs/reports/report_*.md` (AI 생성 리포트, 타임스탬프별)

**주요 개선 사항:**
1. ✅ Mac/Windows 환경 자동 감지 및 Prometheus URL 자동 설정
2. ✅ 실제 메트릭 23개 수집 (이전 4개)
3. ✅ postgres-exporter 커스텀 메트릭 활용
4. ✅ 이전 리포트 자동 비교 분석 기능
5. ✅ 성능 변화 추이 시각화 (↑, ↓, →)

**실행 방법:**
```bash
# 간단 실행
cd crawler-challenge
python scripts/generate_ai_report.py

# 전체 자동화 (pytest + AI 리포트)
./scripts/run_test_and_report.sh
```

**수집되는 메트릭 (23개):**
- 크롤러 작업: 완료/실패 수, 성공률
- 큐 상태: 대기/처리 중인 작업 수
- 시스템 리소스: CPU/메모리 사용률
- 처리 지연 시간: 평균, P95, P99
- PostgreSQL 통계: 처리량, 총 페이지, 고유 도메인
- DLQ: 에러 통계

**AI 리포트 구성:**
1. 성능 요약 (이전 대비 비교 포함)
2. 변화 분석 (2번째 리포트부터)
3. 병목 구간 분석
4. 개선 제안

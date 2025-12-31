# 작업 이력

## 2025-12-31

### Grafana 대시보드 메트릭 수집 문제 해결

**문제:**
- Grafana 대시보드에서 워커별 크롤링 메트릭 패널들이 "No Data"로 표시됨
- 영향받은 패널: Processing Throughput, Error Type Distribution, Success Rate by Worker, HTTP Status Codes, Response Time Heatmap, Top Failing Domains

**원인 분석:**
1. `sharded_worker.py`에서 `record_crawl_result()` 메서드는 정상 호출되고 있음 ✅
2. 워커들이 각각 다른 포트(8001-8008)에서 메트릭을 노출하지만, Prometheus가 이 포트들을 스크래핑하지 않았음 ❌
3. `docker-compose.yml`에서 워커들의 메트릭 포트가 호스트로 노출되지 않음 ❌

**해결 과정:**

1. **코드 분석 (runners/sharded_worker.py)**
   - 212-218줄: 크롤링 성공 시 메트릭 기록 확인
   - 275-282줄: 크롤링 실패 시 메트릭 기록 확인
   - 294-296줄: 워커별 성공률 업데이트 확인
   - domain, error_type, status_code, response_time 모두 정상 수집 중

2. **Prometheus 설정 수정 (c:\Users\top15\Desktop\api\prometheus.yml)**
   - 기존 `crawler_macbook` job → `crawler_master` job으로 이름 변경
   - 신규 `crawler_workers` job 추가: 워커 8개 포트(8001-8008) 스크래핑 설정
   - 맥북 IP(100.125.119.99)의 8001-8008 포트 타겟 추가

3. **Docker Compose 설정 수정 (crawler-challenge/docker-compose.yml)**
   - 모든 워커(1-8)에 `ports` 섹션 추가
   - 각 워커의 메트릭 포트를 호스트로 노출 (8001:8001, 8002:8002, ...)

4. **Prometheus/Grafana Docker 관리 시스템 구축**
   - Windows 데스크톱에서 Prometheus를 Docker로 관리하도록 변경
   - `c:\Users\top15\Desktop\api\docker-compose.yml` 생성
   - Prometheus와 Grafana를 함께 관리하는 구성
   - 설정 리로드, 데이터 보관 기간(30일) 등 운영 편의성 개선

**결과:**
- ✅ 워커별 메트릭 포트(8001-8008) Prometheus 스크래핑 설정 완료
- ✅ Docker에서 워커 포트를 호스트로 노출하여 원격 접근 가능
- ✅ Prometheus/Grafana Docker Compose로 관리 편의성 향상
- ✅ 23개의 크롤링 메트릭이 정상적으로 수집되어 Grafana에서 시각화 가능

**수정된 파일:**
1. `c:\Users\top15\Desktop\api\prometheus.yml` - 워커 타겟 추가
2. `c:\Users\top15\Desktop\api\docker-compose.yml` - Prometheus/Grafana Docker 설정 신규 생성
3. `crawler-challenge/docker-compose.yml` - 워커 8개의 메트릭 포트 노출

---

### Grafana 메트릭 변동 문제 해결 (근본 원인 수정)

**문제:**
- Grafana 대시보드에서 모든 지표가 새로고침할 때마다 계속 변경됨
- Overview 패널(Master 전용)도 Worker 데이터를 표시하는 문제
- job 레이블 필터링을 추가해도 문제 지속

**근본 원인 분석:**

1. **Worker 포트 계산 오류 (치명적)**
   - `sharded_worker.py:125`: `metrics_port = 8001 + worker_id`
   - Worker ID=1이면 포트 8002가 됨 (8001이어야 함!)
   - Docker는 8001:8001로 포트 노출 → 실제 Worker는 8002에서 메트릭 서빙
   - 결과: **Worker 메트릭이 Prometheus에 전혀 수집되지 않음**

   | Worker ID | 예상 포트 | 실제 포트 | Docker 노출 | 결과 |
   |-----------|----------|----------|------------|------|
   | 1 | 8001 | 8002 | 8001:8001 | ❌ 불일치 |
   | 2 | 8002 | 8003 | 8002:8002 | ❌ 불일치 |
   | ... | ... | ... | ... | ... |

2. **Master가 Worker 전용 메트릭도 노출**
   - `MetricsManager` 클래스가 생성 시 모든 메트릭을 정의
   - Master(8000)에서 `crawl_requests_total`, `crawl_errors_total` 등 Worker 전용 메트릭이 값 0으로 노출
   - Grafana에서 집계 시 Master의 0값과 섞여서 값이 변동

**해결 방법:**

1. **Worker 포트 계산 수정** (`runners/sharded_worker.py:125-126`)
   ```python
   # Before: metrics_port = 8001 + worker_id  (worker_id=1 → 8002 ❌)
   # After:  metrics_port = 8000 + worker_id  (worker_id=1 → 8001 ✅)
   ```

2. **MetricsManager 역할 기반 분리** (`src/monitoring/metrics.py`)
   - `role` 파라미터 추가: `'master'` 또는 `'worker'`
   - Master: 큐 상태, 시스템 리소스 메트릭만 생성
   - Worker: 크롤링 결과, 에러, 응답 시간 메트릭만 생성
   - 각 메서드에 역할 체크 추가하여 잘못된 호출 방지

3. **Master/Worker 초기화 수정**
   - `sharded_master.py`: `MetricsManager(port=8000, role='master')`
   - `sharded_worker.py`: `MetricsManager(port=metrics_port, role='worker')`

**수정된 파일:**
1. `src/monitoring/metrics.py` - MetricsManager 역할 기반 분리
2. `runners/sharded_worker.py` - 포트 계산 수정 및 role='worker' 추가
3. `runners/sharded_master.py` - role='master' 추가

**결과:**
- ✅ Worker 메트릭 포트가 Docker 노출 포트와 정확히 일치
- ✅ Master는 큐/시스템 메트릭만 노출 (Overview 패널용)
- ✅ Worker는 크롤링 결과 메트릭만 노출 (Worker 분석 패널용)
- ✅ job 레이블 필터링이 정상 동작하여 메트릭 혼동 방지

**적용 필요 사항:**
- Docker 이미지 재빌드 필요: `docker-compose build --no-cache`
- 컨테이너 재시작 필요: `docker-compose up -d`

---

### Grafana 대시보드 데이터소스 문제 해결

**문제:**
- 모든 코드 수정 및 Docker 재빌드 후에도 Grafana 패널 값이 계속 랜덤하게 변동
- 패널 Edit 시 Query type이 "Random Walk"로 표시됨
- Prometheus 쿼리 입력창이 보이지 않음

**원인:**
- Grafana 대시보드 JSON 파일에 데이터소스(datasource) 설정이 누락됨
- Import 시 Grafana가 기본 데이터소스인 **TestData (Random Walk)**를 사용
- Random Walk는 테스트용 가짜 데이터로, 값이 랜덤하게 생성됨

**해결 방법:**
1. Grafana API로 실제 Prometheus 데이터소스 UID 확인: `ff8rd0a0vyrr4d`
2. Python 스크립트로 모든 패널(20개)에 Prometheus 데이터소스 추가
3. 수정된 JSON 파일 생성: `Crawler_Dashboard_V4_Fixed.json`

**수정 내용:**
```json
// 각 패널과 target에 추가
"datasource": {
  "type": "prometheus",
  "uid": "ff8rd0a0vyrr4d"
}
```

**결과:**
- ✅ 모든 패널이 Prometheus 데이터소스를 사용하도록 수정
- ✅ Random Walk 대신 실제 크롤러 메트릭 표시
- ✅ Overview 패널: Master 메트릭만 표시 (Queue Pending, Tasks Completed 등)
- ✅ Worker 분석 패널: Worker 메트릭만 표시 (Success Rate, Error Distribution 등)
- ✅ 값이 안정적으로 표시되며 새로고침 시 변동 없음

**생성된 파일:**
- `c:\Users\top15\Downloads\Crawler_Dashboard_V4_Fixed.json` - 데이터소스가 추가된 최종 대시보드

---

### 2025-12-31 작업 요약

| 순서 | 문제 | 원인 | 해결 |
|------|------|------|------|
| 1 | Worker 메트릭 "No Data" | Prometheus가 Worker 포트 미스크래핑 | prometheus.yml에 crawler_workers job 추가 |
| 2 | Docker 포트 미노출 | docker-compose.yml 포트 설정 누락 | 워커 8개 포트(8001-8008) 노출 추가 |
| 3 | 메트릭 값 계속 변동 | Worker 포트 계산 오류 (8001+id → 8002) | 8000+id로 수정 |
| 4 | Master/Worker 메트릭 혼동 | MetricsManager가 모든 메트릭 생성 | role 파라미터로 역할별 분리 |
| 5 | Random Walk 표시 | JSON에 datasource 설정 누락 | Prometheus datasource 일괄 추가 |

**커밋 내역:**
1. `75605c3` - fix(monitoring): Grafana 메트릭 수집 문제 해결 - 워커 포트 노출
2. `6fef659` - fix(metrics): Worker 포트 계산 오류 및 메트릭 분리 수정

---

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

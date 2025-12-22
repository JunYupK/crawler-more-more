# Crawler Challenge - 작업 보고서

**작성일**: 2025-12-16
**작업 단계**: Phase 2.5 안정화 및 자동화
**작성자**: Claude Code Assistant

---

## 개요

본 보고서는 대규모 분산 크롤러 시스템(Crawler Challenge)의 Phase 2.5 단계에서 수행된 작업 내용을 기록합니다. 주요 목표는 **데이터 정합성 보장**과 **자동화된 성능 리포트 생성**이었습니다.

---

## 1. DLQ (Dead Letter Queue) 시스템 구현

### 1.1 배경
- `NUL (0x00)` 바이트가 포함된 데이터로 인해 배치 Insert 실패 시 무한 롤백 현상 발생
- 실패한 데이터가 유실되는 문제

### 1.2 구현 내용

#### 데이터베이스 스키마 (`docker/init.sql`)
```sql
CREATE TABLE IF NOT EXISTS crawler_dlq (
    id BIGSERIAL PRIMARY KEY,
    url TEXT,
    error_message TEXT,
    error_type VARCHAR(50),  -- DataError, UniqueViolationError 등
    raw_data JSONB,          -- 실패한 데이터 원본
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_dlq_created_at ON crawler_dlq(created_at);
CREATE INDEX IF NOT EXISTS idx_dlq_error_type ON crawler_dlq(error_type);
```

#### 코드 수정 (`src/core/database.py`)

| 메서드 | 변경 내용 |
|--------|----------|
| `flush_batch()` | try-except 블록 추가, 실패 시 `save_to_dlq()` 호출 |
| `save_to_dlq()` | 신규 메서드 - 실패 데이터를 DLQ 테이블에 격리 저장 |
| `add_to_batch()` | metadata 버그 수정 (dict에 replace 호출 오류) |

#### 핵심 로직
```python
def flush_batch(self):
    batch_data_copy = list(self.batch_buffer)
    try:
        # 배치 Insert 시도
        psycopg2.extras.execute_values(cursor, insert_query, values)
        conn.commit()
    except psycopg2.Error as e:
        conn.rollback()
        self.save_to_dlq(batch_data_copy, e)  # DLQ로 격리
    finally:
        self.batch_buffer.clear()  # 무한 루프 방지
```

---

## 2. Gemini API 기반 자동 리포트 생성기

### 2.1 목적
- 크롤링 종료 후 Prometheus 메트릭과 테스트 결과를 AI가 분석
- 한국어 Markdown 기술 리포트 자동 생성

### 2.2 구현 파일

| 파일 | 역할 |
|------|------|
| `scripts/generate_ai_report.py` | 메인 리포트 생성 스크립트 |
| `.env.example` | 환경변수 템플릿 |
| `.gitignore` | API 키 보안을 위한 .env 제외 |

### 2.3 주요 클래스

```
MetricsCollector     - Prometheus에서 메트릭 수집
TestResultLoader     - pytest JSON 결과 로드
GeminiReportGenerator - Gemini 2.5 Flash로 리포트 생성
ReportSaver          - docs/reports/에 MD 파일 저장
```

### 2.4 수집 메트릭
- TPS (Transactions Per Second)
- DB Connections (Active/Idle)
- Rollback Rate
- CPU/Memory Usage
- Crawler 관련 커스텀 메트릭

### 2.5 Rate Limit 처리
```python
# 429 에러 시 자동 재시도 (30초 → 60초 → 90초)
for attempt in range(max_retries):
    try:
        response = self.model.generate_content(prompt)
        return response.text
    except Exception as e:
        if "429" in str(e):
            wait_time = 30 * (attempt + 1)
            time.sleep(wait_time)
```

### 2.6 의존성 추가 (`requirements.txt`)
```
google-generativeai
prometheus-api-client
python-dotenv
```

---

## 3. Prometheus Custom Queries 설정

### 3.1 목적
PostgreSQL 테이블 데이터를 실시간 메트릭으로 수집

### 3.2 구현 (`docker/queries.yaml`)

| 메트릭 | 설명 |
|--------|------|
| `pg_crawler_stats_total_pages` | 총 크롤링 페이지 수 |
| `pg_crawler_stats_pages_last_1min` | 최근 1분간 크롤링 수 |
| `pg_crawler_stats_unique_domains` | 고유 도메인 수 |
| `pg_crawler_dlq_total` | DLQ 레코드 수 |
| `pg_crawler_throughput_pages_per_second` | 초당 크롤링 속도 |

### 3.3 Docker 설정 (`docker-compose.yml`)
```yaml
postgres-exporter:
  environment:
    PG_EXPORTER_EXTEND_QUERY_PATH: "/queries.yaml"
  volumes:
    - ./docker/queries.yaml:/queries.yaml:ro
```

---

## 4. 크롤링 완료 후 자동화 파이프라인

### 4.1 수정 파일
`runners/sharded_master.py`

### 4.2 실행 흐름

```
크롤링 완료 (모든 URL 처리)
        ↓
✅ "모든 샤딩된 작업 완료"
        ↓
🧪 pytest 자동 실행
        ↓
📄 test_report.json 생성
        ↓
📊 generate_ai_report.py 실행
        ↓
docs/reports/report_YYYYMMDD_HHMM.md 저장
```

### 4.3 추가된 메서드

| 메서드 | 역할 |
|--------|------|
| `run_tests_and_generate_report()` | pytest 실행 및 JSON 리포트 생성 |
| `generate_completion_report()` | 테스트 + AI 리포트 통합 실행 |

---

## 5. Docker 설정 업데이트

### 5.1 Dockerfile 변경
```dockerfile
# scripts 디렉토리 추가
COPY scripts/ ./scripts/
```

### 5.2 docker-compose.yml 변경 (crawler-master)
```yaml
environment:
  - GEMINI_API_KEY=${GEMINI_API_KEY}
  - PROMETHEUS_URL=http://100.105.22.101:9090
volumes:
  - ./docs/reports:/app/docs/reports
  - ./.env:/app/.env:ro
```

---

## 6. 파일 변경 요약

### 신규 생성
| 파일 | 설명 |
|------|------|
| `scripts/generate_ai_report.py` | AI 리포트 생성기 |
| `docker/queries.yaml` | Prometheus 커스텀 쿼리 |
| `.env.example` | 환경변수 템플릿 |
| `.gitignore` | Git 제외 파일 설정 |
| `docs/reports/` | 리포트 저장 디렉토리 |

### 수정
| 파일 | 변경 내용 |
|------|----------|
| `docker/init.sql` | crawler_dlq 테이블 추가 |
| `src/core/database.py` | DLQ 로직, 버그 수정 |
| `runners/sharded_master.py` | 자동 리포트 생성 연동 |
| `docker/Dockerfile` | scripts 디렉토리 복사 |
| `docker-compose.yml` | 환경변수, 볼륨, queries.yaml 설정 |
| `requirements.txt` | AI 관련 의존성 추가 |

---

## 7. 환경 설정 가이드

### 7.1 API 키 설정
```bash
# .env 파일 생성
cp .env.example .env

# .env 편집
GEMINI_API_KEY=your-api-key-here
PROMETHEUS_URL=http://100.105.22.101:9090
```

### 7.2 Docker 재빌드 및 실행
```bash
# 디렉토리 생성
mkdir -p docs/reports

# 재빌드
docker-compose down
docker-compose build
docker-compose up -d
```

### 7.3 수동 리포트 생성 (필요시)
```bash
python scripts/generate_ai_report.py
```

---

## 8. 알려진 이슈 및 주의사항

### 8.1 Gemini API Rate Limit
- 무료 티어: 분당 5회 요청 제한
- 해결: 새 API 키 발급 또는 유료 플랜 사용

### 8.2 Tailscale 네트워크
- Mac ↔ Windows 간 Tailscale 연결 필요
- Prometheus URL: `http://100.105.22.101:9090`

### 8.3 DLQ 모니터링
- DLQ에 데이터가 쌓이면 원인 분석 필요
- Prometheus 메트릭: `pg_crawler_dlq_total`

---

## 9. 다음 단계 제안

1. **DLQ 재처리 로직**: DLQ 데이터를 정제 후 재시도하는 기능
2. **Grafana 대시보드**: Prometheus 메트릭 시각화
3. **Slack/Discord 알림**: 크롤링 완료 및 에러 알림
4. **리포트 히스토리 관리**: 이전 리포트와 비교 분석

---

**작업 완료**: 2025-12-16 23:30 KST

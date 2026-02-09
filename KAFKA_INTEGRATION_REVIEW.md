# Kafka Integration PR #6 - Code Review

> **PR:** #6 "Plan implementation of previous discussion"
> **Branch:** `claude/plan-implementation-iCg4R` -> `master`
> **Merged:** 2026-02-04
> **Reviewer:** Claude Code Review

---

## 1. Architecture Overview

4-Layer Kafka 스트리밍 파이프라인으로, 기존 Redis+PostgreSQL 직접 저장 방식에서 Kafka 스트림 기반으로 전환한 대규모 리팩토링입니다.

```
Layer 1 (Ingestor) -> raw.page -> Layer 2 (Router) -> process.fast/rich
-> Layer 3 (Processor) -> processed.final -> Layer 4 (Storage) -> MinIO + PostgreSQL
```

**전체 평가: 아키텍처 설계는 우수하나, 프로덕션 배포 전 해결해야 할 버그와 개선점이 있음**

---

## 2. Critical Issues (Must Fix)

### 2.1 `sys.path.insert(0, ...)` 하드코딩 경로
- **파일:** `kafka_producer.py:22-23`, `httpx_crawler.py:26-27`, `base_worker.py:23-24`, `smart_router.py:26-27`, `fast_worker.py:22-23`, `rich_worker.py:19-20`, `minio_writer.py:24-25`, `postgres_writer.py:23-24`, `hybrid_storage.py:21-22`
- **문제:** `sys.path.insert(0, '/home/user/crawler-more-more/crawler-challenge')` 절대 경로 하드코딩
- **영향:** 다른 환경(Docker, CI/CD, 다른 개발자 PC)에서 import 실패
- **권장:** 상대 경로 import 또는 `pyproject.toml`/`setup.py`로 패키지 설치 구조 전환

### 2.2 KafkaProducer - 이중 직렬화 오버헤드
- **파일:** `kafka_producer.py:280-282`
- **문제:** `_send()` 메서드에서 `send_and_wait()` 호출 후 통계를 위해 `self._serialize(value)`를 다시 호출하여 같은 데이터를 두 번 직렬화
- **영향:** CPU 낭비, 특히 대용량 HTML 압축 데이터에서 성능 저하
- **권장:** 직렬화를 한 번만 수행하고, 직렬화된 바이트 크기를 캐시하거나 추정값 사용

```python
# Before
self.stats.bytes_sent += len(self._serialize(value))

# After - 직렬화 전에 크기 계산하거나 send 결과에서 가져오기
serialized = self._serialize(value)
self.stats.bytes_sent += len(serialized)
```

### 2.3 Router - 압축 해제된 HTML을 Kafka에 재전송
- **파일:** `smart_router.py:346-356`
- **문제:** `_route_message()`에서 압축 해제된 전체 HTML 문자열을 `process.fast`/`process.rich` 토픽에 그대로 전송
- **영향:** Kafka 대역폭 폭증. raw.page에서는 Zstd 압축된 HTML을 전송하나, 라우팅 후에는 비압축 HTML이 전달됨
- **권장:** 압축된 상태로 전달하거나, 라우터에서도 재압축 적용

### 2.4 Consumer 오프셋 커밋 전략 위험
- **파일:** `smart_router.py:242`, `base_worker.py:322`, `hybrid_storage.py:224`
- **문제:** 메시지 한 건 처리마다 `await self._consumer.commit()` 호출 (동기적 커밋)
- **영향:** 높은 처리량에서 Kafka 브로커 부하 증가, 처리 속도 저하
- **권장:** 배치 단위 커밋 또는 주기적 커밋으로 전환 (예: 100건마다 또는 5초마다)

### 2.5 MinIO Writer - `asyncio.get_event_loop()` 사용
- **파일:** `minio_writer.py:163`, `minio_writer.py:297`, `minio_writer.py:384` 등
- **문제:** `asyncio.get_event_loop()`은 Python 3.10+에서 deprecated, 3.12에서는 RuntimeError 발생 가능
- **권장:** `asyncio.get_running_loop()` 사용 또는 `aioboto3`/`aiohttp` 기반 비동기 S3 클라이언트로 전환

### 2.6 Producer `acks="1"` - 데이터 유실 가능
- **파일:** `kafka_config.py:80-82`
- **문제:** Producer acks가 `"1"` (리더만 확인)으로 설정되어 있어 리더 장애 시 데이터 유실 가능
- **참고:** `replication_factor=1`이므로 현재는 영향 없으나, 멀티 브로커 환경에서는 `acks="all"` 권장

---

## 3. High Priority Issues

### 3.1 Compression - `compress_stream()` 메서드 버그
- **파일:** `compression.py:116-122`
- **문제:** `ZstdCompressionWriter`를 `chunks.append`로 직접 생성하는 방식은 `zstandard` 라이브러리 API 사용법이 잘못됨
- **영향:** 런타임 에러 발생 가능
- **권장:** 일반 `compress()` 메서드 사용하거나 올바른 스트리밍 API 사용

### 3.2 ZstdCompressor 스레드 안전성
- **파일:** `compression.py:40-44`
- **문제:** 문서에는 "스레드 세이프" 라고 적혀있으나, `zstandard.ZstdCompressor`와 `ZstdDecompressor` 인스턴스가 실제로 스레드 세이프하지 않음 (asyncio 환경에서는 문제 없으나, 멀티스레드 executor 사용 시 위험)
- **권장:** 문서 수정 또는 스레드별 인스턴스 생성

### 3.3 Default 패스워드 하드코딩
- **파일:** `kafka_config.py:140-142`, `kafka_config.py:169-171`
- **문제:** MinIO 패스워드 `minioadmin123`, PostgreSQL 패스워드 `crawler123`이 소스코드에 기본값으로 하드코딩
- **영향:** 환경변수 설정 없이 배포 시 기본 패스워드로 운영됨
- **권장:** 기본값 없이 환경변수 필수화하거나, 런타임에 warning 로그 출력

### 3.4 Singleton Config 인스턴스 문제
- **파일:** `kafka_config.py:272`, `scoring.py:392-400`, `compression.py:192-200`
- **문제:** 모듈 레벨에서 singleton 인스턴스가 import 시점에 생성됨. 테스트에서 config를 변경하기 어려움
- **권장:** Lazy initialization 또는 dependency injection 패턴 사용

### 3.5 RichWorker - 중복 크롤링
- **파일:** `rich_worker.py:100-118`
- **문제:** `process_html()`에서 이미 Ingestor가 가져온 HTML을 받지만, Crawl4AI 사용 시 URL을 **다시 방문**함
- **영향:** 동일 URL을 2번 요청 → 대상 서버 부하 증가, 처리 시간 2배
- **권장:** 가능하면 이미 받은 HTML에 JavaScript를 주입하는 방식을 고려하거나, 이 trade-off를 문서화

---

## 4. Medium Priority Issues

### 4.1 Docker Compose - 단일 Kafka 브로커
- **파일:** `docker-compose.stream.yml:49-50`
- **문제:** `KAFKA_DEFAULT_REPLICATION_FACTOR: 1`, 단일 브로커 구성
- **영향:** 브로커 장애 시 전체 파이프라인 중단
- **권장:** 프로덕션에서는 최소 3 브로커 구성 필요 (개발용으로는 OK)

### 4.2 Kafka UI 인증 없음
- **파일:** `docker-compose.stream.yml:78-85`
- **문제:** Kafka UI가 인증 없이 port 8080에 노출
- **영향:** 누구나 토픽 데이터 조회/수정 가능
- **권장:** 프로덕션에서는 인증 추가 또는 내부 네트워크로 제한

### 4.3 MinIO processed-markdown 버킷 public 접근
- **파일:** `docker-compose.stream.yml:124`
- **문제:** `mc anonymous set download myminio/processed-markdown` → 공개 다운로드 설정
- **영향:** MinIO에 저장된 모든 마크다운이 인증 없이 접근 가능
- **권장:** 프로덕션에서는 제거 필요

### 4.4 PostgreSQL 배치 UPSERT가 실제로는 개별 실행
- **파일:** `postgres_writer.py:288-320`
- **문제:** `save_batch()`가 레코드를 하나씩 `fetchrow()`로 실행. 진정한 배치가 아님
- **영향:** PostgreSQL 왕복 횟수가 배치 크기만큼 발생
- **권장:** `executemany()` 또는 `COPY` 명령, 또는 트랜잭션 내 배치 처리

### 4.5 PageAnalyzer - `soup.decompose()` 후 원본 데이터 변질
- **파일:** `page_analyzer.py:290-291`
- **문제:** `_analyze_content()`에서 `tag.decompose()`를 호출하면 BeautifulSoup 객체가 변경됨. 이후 다른 분석에서 script/style 태그가 사라진 상태로 동작
- **영향:** 콘텐츠 통계가 일부 부정확할 수 있음 (analyze 호출 순서에 따라)
- **완화:** 현재 코드에서는 `_analyze_content()`가 마지막에 호출되어 큰 문제는 아니나, 순서 변경 시 버그 발생

### 4.6 `asyncio.import` 위치 문제
- **파일:** `fast_worker.py:401`
- **문제:** 파일 맨 아래에 `import asyncio`가 있음. 파일 상단에 없어서 FastWorkerPool이 asyncio를 사용할 때까지 import되지 않음
- **영향:** Python은 모듈 로드 시 전체를 실행하므로 실제 에러는 아니지만, 코드 가독성 저하

---

## 5. Low Priority / Suggestions

### 5.1 User-Agent 로테이션의 효과
- **파일:** `httpx_crawler.py:127-133`
- **참고:** 모든 User-Agent가 Chrome 120 기반으로 비슷함. 실제 로테이션 효과가 제한적
- **제안:** 더 다양한 브라우저/버전 추가 고려

### 5.2 `progress_callback` 타입 힌트
- **파일:** `httpx_crawler.py:322`
- **문제:** `Optional[callable]` → `Optional[Callable]`이어야 함 (소문자 `callable`은 built-in, 타입 힌트용 `Callable`은 `typing`에서 import)
- **영향:** 런타임에는 문제 없으나, mypy/pyright에서 경고

### 5.3 Grafana 기본 패스워드
- **파일:** `docker-compose.stream.yml:202`
- **문제:** `admin123` 기본 패스워드
- **제안:** `.env` 파일에서 관리

### 5.4 Prometheus 설정에 scrape target 누락
- **파일:** `docker/prometheus-stream.yml` (확인 필요)
- **제안:** application 메트릭 엔드포인트 scrape 설정이 없으면 추가 필요

### 5.5 Topic 생성 스크립트 - replication-factor 미지정
- **파일:** `docker/scripts/create-topics.sh`
- **문제:** `--replication-factor` 미지정으로 서버 기본값(`1`) 사용
- **제안:** 명시적으로 지정하여 의도를 명확히

---

## 6. Positive Highlights

### 잘 된 부분:

1. **아키텍처 설계:** 4-Layer 분리가 명확하고, 각 레이어가 독립적으로 스케일링 가능
2. **DLQ 패턴:** 모든 레이어에서 실패 메시지를 DLQ로 라우팅하는 견고한 에러 처리
3. **Zstd 압축 선택:** gzip 대비 빠른 속도와 좋은 압축률의 균형
4. **msgpack 직렬화:** JSON 대비 작은 크기와 빠른 속도
5. **Smart Router 점수 시스템:** 정적/동적 페이지 판별을 위한 규칙 기반 스코어링이 잘 설계됨
6. **Graceful shutdown:** 모든 Runner에서 SIGINT/SIGTERM 처리
7. **통계 수집:** 각 컴포넌트마다 상세한 stats 수집
8. **Runner CLI:** argparse로 다양한 모드 제공 (test, demo, analyze-file 등)
9. **MinIO 키 구조:** URL 해시 기반 계층적 키 생성 (`domain/ab/cd/hash.md`)
10. **healthcheck 설정:** 모든 Docker 서비스에 헬스체크 구성

---

## 7. Summary

| Category | Count |
|----------|-------|
| Critical (Must Fix) | 6 |
| High Priority | 5 |
| Medium Priority | 6 |
| Low Priority | 5 |

### 전체 점수: **7/10**

아키텍처 설계와 코드 품질은 좋으나, 프로덕션 배포 전에 Critical/High 이슈 해결이 필요합니다.
특히 **하드코딩된 경로**, **이중 직렬화**, **비압축 HTML 라우팅**, **매 메시지 커밋** 문제는 성능과 이식성에 직접적인 영향을 미칩니다.

---

*Reviewed on: 2026-02-09*

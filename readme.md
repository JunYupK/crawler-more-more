# Distributed Sharded Crawler

M2 MacBook 8GB라는 제한된 환경에서 Tranco Top 1M(100만 개) 웹사이트를 수집하기 위해 설계한 분산 크롤링 시스템입니다.

단순히 "돌아가는 크롤러"가 아니라, **관측 가능하고(Observable)**, **확장 가능한(Scalable)** 시스템을 목표로 했습니다. 보이지 않는 시스템은 관리할 수 없다는 생각으로, 크롤링 로직만큼 모니터링 체계 구축에 공을 들였습니다.

## 프로젝트 배경

클라우드에 비용을 투자하면 성능은 당연히 나옵니다. 하지만 개인 프로젝트에서 매달 수십만 원을 쓸 수는 없었습니다. "내 맥북 한 대로 하루에 몇 페이지나 긁을 수 있을까?"라는 현실적인 질문에서 시작했고, 그 과정에서 마주친 병목들을 하나씩 해결해나갔습니다.

8GB 메모리에서 OOM 없이 안정적으로 동작하면서, 동시에 처리량을 최대한 끌어올리는 게 목표입니다.

## 현재 성능

| 지표 | 수치 |
|------|------|
| 안정 처리량 | 5.26 pages/sec |
| 1만 URL 처리 시간 | 31.7분 |
| 메모리 사용량 | 최대 4GB (전체의 50%) |
| 성공률 | 67.5% |
| **일일 예상 처리량** | **약 45만 페이지** |

31.7분 연속 크롤링에서 메모리 누수 없이 안정적으로 동작하는 것을 확인했습니다.

## 시스템 아키텍처

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        Tailscale Mesh VPN                               │
│                    (포트 포워딩 없는 보안 터널링)                           │
└─────────────────────────────────────────────────────────────────────────┘
         │                                              │
         ▼                                              ▼
┌─────────────────────────────┐          ┌─────────────────────────────┐
│   MacBook M2 (Crawler Host) │          │   Desktop (Monitor Host)    │
│                             │          │                             │
│  ┌───────────────────────┐  │          │  ┌───────────────────────┐  │
│  │   Sharded Master      │  │   ───►   │  │     Prometheus        │  │
│  │   - Tranco 리스트 로드  │  │  :8000   │  │     - 메트릭 수집       │  │
│  │   - 샤드 분배          │  │          │  │     - 15s 스크랩       │  │
│  │   - Metrics Server    │  │          │  └───────────┬───────────┘  │
│  └───────────┬───────────┘  │          │              │              │
│              │              │          │  ┌───────────▼───────────┐  │
│  ┌───────────▼───────────┐  │          │  │      Grafana          │  │
│  │   Sharded Workers     │  │          │  │      - 실시간 대시보드   │  │
│  │   - URL 소비 & 크롤링   │  │          │  │      - 알림 설정       │  │
│  │   - 에러/지연 메트릭    │  │          │  └───────────────────────┘  │
│  └───────────┬───────────┘  │          │                             │
│              │              │          │  ┌───────────────────────┐  │
│  ┌───────────▼───────────┐  │          │  │   Postgres Exporter   │  │
│  │  Redis (3 Shards)     │  │          │  │   - TPS, Connection   │  │
│  │  - 우선순위 큐         │  │          │  │   - Deadlock 감시     │  │
│  │  - DB 1, 2, 3 분리    │  │          │  └───────────────────────┘  │
│  └───────────┬───────────┘  │          │                             │
│              │              │          └─────────────────────────────┘
│  ┌───────────▼───────────┐  │
│  │     PostgreSQL        │  │
│  │     - 크롤링 결과 저장  │  │
│  └───────────────────────┘  │
│                             │
└─────────────────────────────┘
```

**왜 이렇게 분리했나요?**

처음에는 맥북 한 대에서 크롤링과 모니터링을 모두 돌렸습니다. 그런데 Prometheus + Grafana가 생각보다 메모리를 많이 먹더군요. 8GB에서 크롤러와 모니터링이 메모리를 두고 경쟁하는 상황이 됐습니다.

그래서 역할을 분리했습니다. 맥북은 크롤링에만 집중하고, 집에 놀고 있던 데스크톱이 메트릭 수집과 시각화를 담당합니다. Tailscale로 두 머신을 연결해서, 공유기 포트 포워딩 없이도 안전하게 통신할 수 있게 만들었습니다.

## 기술 스택

| 영역 | 기술 |
|------|------|
| Language | Python 3.10 (asyncio, aiohttp) |
| Container | Docker, Docker Compose |
| Database | PostgreSQL 15, Redis 7 (3-shard) |
| Monitoring | Prometheus, Grafana, Postgres-Exporter |
| Networking | Tailscale (Mesh VPN) |
| CI/CD | GitHub Actions |
| Orchestration | Kubernetes (매니페스트 준비 완료) |

## 핵심 구현

### 1. Redis 샤딩 큐

단일 Redis 인스턴스에서 여러 워커가 동시에 LPOP을 하면 Lock 경합이 생깁니다. 이걸 피하기 위해 Redis DB를 3개로 쪼개고, URL을 도메인 해시 기반으로 분산했습니다.

```python
def get_shard_for_url(self, url: str) -> int:
    domain = urlparse(url).netloc
    hash_value = int(hashlib.md5(domain.encode()).hexdigest(), 16)
    return hash_value % self.num_shards  # 0, 1, 2 중 하나
```

각 워커는 자신이 담당하는 샤드에서만 URL을 가져옵니다. 같은 도메인은 항상 같은 샤드로 가기 때문에, 도메인별 rate limiting도 자연스럽게 워커 단위로 격리됩니다.

### 2. 커스텀 메트릭 (MetricsManager)

기본 CPU/메모리 지표만으로는 크롤러 상태를 파악하기 어려웠습니다. 그래서 크롤링에 특화된 커스텀 메트릭을 추가했습니다.

```python
# 수집하는 메트릭들
crawler_queue_pending      # Redis 대기열 길이
crawler_queue_processing   # 현재 처리 중인 URL 수
crawler_tasks_completed    # 성공한 크롤링 수
crawler_tasks_failed       # 실패한 크롤링 수
crawler_error_details      # 에러 타입별 카운트 (timeout, connection_reset, ...)
crawler_process_latency    # 페이지당 처리 시간 히스토그램
crawler_shard_pending      # 샤드별 대기열 (로드 밸런싱 확인용)
```

Grafana에서 이 지표들을 실시간으로 보면서, 어느 샤드가 밀리는지, 어떤 에러가 많이 나는지 바로 확인할 수 있습니다.

### 3. 8GB 환경 최적화

메모리가 부족하면 크롤러가 죽습니다. 이걸 막기 위해 여러 장치를 넣었습니다.

**배치 처리 + 즉시 저장**
```python
def add_to_batch(self, url, content):
    self.batch_buffer.append(...)
    if len(self.batch_buffer) >= 100:  # 100개 모이면
        self.flush_batch()              # 바로 DB에 쓰고 메모리 해제
```

**커넥션 풀링**
```python
# 매번 연결 새로 만들지 않고 재사용
DatabaseManager._pool = pool.ThreadedConnectionPool(
    min_conn=1, max_conn=10, ...
)
```

**빠른 타임아웃**
```python
REQUEST_TIMEOUT = 10  # 30초 → 10초
# 느린 서버를 오래 기다리면 메모리에 응답 대기 객체가 쌓임
```

### 4. DocOps (문서 자동화)

테스트 돌릴 때마다 수동으로 결과를 정리하는 게 귀찮았습니다. 그래서 자동화했습니다.

- **pytest-json-report**: 테스트 실행하면 `TEST_REPORT.md` 자동 갱신
- **WorkLogger**: 크롤링 완료 시 성능 지표를 `PERFORMANCE.md`에 자동 기록

```python
# 크롤링 완료 후 자동 기록
work_logger.log_and_commit(
    title="10k URL 크롤링 완료",
    description="31.7분 소요, 67.5% 성공률",
    details={"pages_per_sec": 5.26, "memory_peak": "4GB"}
)
```

## 트러블슈팅 기록

### 1. 네트워크 분리 문제

**상황**: MacBook(사설 IP)과 Desktop(다른 네트워크) 사이에서 Prometheus가 메트릭을 긁어오지 못함

**시도한 것들**:
- 공유기 포트 포워딩 → 보안 문제, 동적 IP 문제
- ngrok → 무료 플랜 제한, 불안정

**해결**: Tailscale 도입. 설치하고 로그인하면 끝. 두 머신이 같은 가상 네트워크에 있는 것처럼 동작합니다. `100.x.x.x` 형태의 고정 IP가 할당되어서 Prometheus 설정도 한 번만 하면 됩니다.

### 2. DB 스키마 불일치

**상황**: `psycopg2.errors.UndefinedTable: relation "crawled_pages" does not exist`

**원인**: Python 코드는 `crawled_pages` 테이블을 쓰는데, `init.sql`에서는 `pages`로 만들고 있었음

**해결**: 
```sql
-- init.sql 수정
CREATE TABLE IF NOT EXISTS crawled_pages (  -- pages → crawled_pages
    ...
);
```
그리고 Docker 볼륨 초기화 (`docker-compose down -v && docker-compose up -d`)

### 3. DB 병목 미감지

**상황**: 크롤러는 잘 도는데 DB 저장이 느려지는 현상. 하지만 Grafana에서는 안 보임

**원인**: 애플리케이션 메트릭만 수집하고 있었고, PostgreSQL 자체 상태는 모니터링 안 하고 있었음

**해결**: `postgres-exporter` 추가
```yaml
postgres-exporter:
  image: prometheuscommunity/postgres-exporter
  environment:
    DATA_SOURCE_URI: "postgres:5432/crawler_db?sslmode=disable"
```

이제 TPS, Active Connection, Deadlock 같은 DB 지표도 Grafana에서 볼 수 있습니다.

### 4. 성공률 0% 문제

**상황**: 크롤러가 도는데 성공이 하나도 안 됨

**원인**: `robots.txt` 처리 로직이 잘못됐음. 도메인 전체를 차단하는 방식으로 구현되어 있어서, 하나라도 Disallow가 있으면 그 사이트 전체를 스킵

**해결**: URL 경로별로 개별 판단하도록 수정
```python
# Before: 도메인 단위로 차단 여부 결정
# After: 각 URL 경로마다 robots.txt 규칙 확인
if not domain_state.robots_rules.parser.can_fetch(self.user_agent, url):
    return False, f"robots.txt에 의해 차단됨: {url}"
```

## 실행 방법

### 로컬 실행 (Docker Compose)

```bash
# 의존성 설치
pip install -r requirements.txt

# 인프라 실행
docker-compose up -d redis postgres

# 크롤링 시작 (1만 URL)
python runners/sharded_master.py --count 10000 --workers 4
```

8GB 환경에서는 워커 4개가 적당합니다.

### 모니터링 환경 구축 (별도 머신)

```bash
# Tailscale 설치 후 같은 계정으로 로그인
# MacBook과 Desktop 모두에서 실행

# Desktop에서 Prometheus + Grafana 실행
cd k8s/monitoring
docker-compose up -d

# prometheus.yml에서 타겟 설정
# MacBook의 Tailscale IP (100.x.x.x:8000)로 지정
```

## 프로젝트 구조

```
crawler-challenge/
├── src/
│   ├── core/
│   │   ├── polite_crawler.py      # 크롤링 로직, robots.txt 준수
│   │   └── database.py            # PostgreSQL 배치 처리
│   ├── managers/
│   │   ├── sharded_queue_manager.py  # Redis 3-shard 큐
│   │   ├── tranco_manager.py      # URL 우선순위 관리
│   │   └── progress_tracker.py    # 진행 상황 추적
│   └── monitoring/
│       ├── metrics.py             # Prometheus 커스텀 메트릭
│       └── monitoring_dashboard.py
├── runners/
│   ├── sharded_master.py          # 마스터 프로세스
│   └── sharded_worker.py          # 워커 프로세스
├── k8s/
│   ├── base/                      # K8s 기본 매니페스트
│   ├── autoscaling/               # KEDA 설정
│   └── monitoring/                # Prometheus, Grafana
├── config/
│   └── settings.py                # 성능 튜닝 파라미터
└── tests/
```

## 다음 목표

- [ ] **성공률 80% 이상**: 실패 URL 재시도 로직 개선
- [ ] **100만 URL 완주**: 장기 안정성 검증, 메모리 누수 모니터링
- [ ] **DB 튜닝**: 대량 INSERT 시 성능 최적화 (COPY 명령어 활용)
- [ ] **Kubernetes 배포**: 로컬 한계를 넘어서 확장이 필요할 때를 위한 준비

---

비싼 장비 없이도 꽤 많은 걸 할 수 있다는 걸 보여주고 싶었습니다. 제한된 환경에서 병목을 찾고 해결하는 과정이 오히려 더 많은 걸 배우게 해준 것 같습니다.
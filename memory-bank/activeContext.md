# Active Context

## 마지막 작업 (2026-02-24)

### 작업 내용: `make test-crawl` Mac 실행 오류 해결

#### 해결한 문제 3가지

1. **`sharded_master.py` import 순서 버그**
   - `from src.monitoring.metrics import MetricsManager`가 `sys.path.append(...)` 보다 먼저 실행되어 `ModuleNotFoundError: No module named 'src'` 발생
   - `sys.path.append`를 모든 `src.*` import 위로 이동

2. **Mac Python 환경에 패키지 미설치**
   - `~/venv`가 존재하지 않았고 시스템 Python 3.14에 의존성 없음
   - `python3.13 -m venv ~/venv` 생성 후 `pip install -e ".[mac]"` 실행
   - `pyproject.toml`의 `mac` extras에 누락된 패키지 추가 설치: `psutil`, `prometheus-client`, `redis`

3. **`KAFKA_SERVERS` 환경변수 미설정**
   - Kafka가 desktop(`100.105.22.101`)에서 실행 중인데 기본값 `localhost:9092` 사용
   - `crawler-challenge/.env` 파일 생성 (`KAFKA_SERVERS=100.105.22.101:9092`)
   - Makefile에 `source .env` 추가 (macOS 구버전 make는 `-include .env` + `export`만으로 자식 프로세스에 전달 안 됨)

#### 변경된 파일
- `crawler-challenge/runners/sharded_master.py` — import 순서 수정
- `crawler-challenge/Makefile` — `.env` 로딩 추가 (`-include` + bash `source`)
- `crawler-challenge/.env` — Kafka 주소 설정 (gitignore 추천)
- `CLAUDE.md` — 프로젝트 가이드 최초 생성

#### 커밋
- `a97f466` fix: resolve Mac make test-crawl startup errors

---

## 환경 정보

| 항목 | 값 |
|---|---|
| Mac venv 경로 | `~/venv` (Python 3.13) |
| Desktop IP (Tailscale) | `100.105.22.101` |
| Kafka | `100.105.22.101:9092` (EXTERNAL listener) |
| Redis | `localhost:6379` (Mac 로컬) |
| DB 1~3 | Redis DB1, DB2, DB3 (샤드) |

## 알아둘 점

- `aiokafka`의 `Unable to connect to node with id 1: localhost:9092` 에러는 내부 메타데이터 갱신 실패 노이즈 — 실제 데이터 전송에는 영향 없음 (desktop에서 정상 수신 확인됨)
- `pyproject.toml`의 `mac` extras에 `psutil`, `prometheus-client`, `redis`가 누락되어 있음 — 추후 추가 필요
- `crawler-challenge/.env`는 gitignore에 추가 권장 (현재 미추가 상태)

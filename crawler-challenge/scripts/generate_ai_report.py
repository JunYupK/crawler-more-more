#!/usr/bin/env python3
"""
Gemini API 기반 자동 리포트 생성기

테스트 종료 후 Prometheus 메트릭과 테스트 결과를 분석하여
Markdown 형식의 기술 리포트를 자동 생성합니다.

Usage:
    python scripts/generate_ai_report.py [--prometheus-url URL] [--time-range MINUTES]

Environment Variables:
    GEMINI_API_KEY: Gemini API 키 (필수)
    PROMETHEUS_URL: Prometheus 서버 URL (기본값: http://100.105.22.101:9090, Tailscale 윈도우)
"""

import os
import sys
import json
import logging
import argparse
import time
import platform
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Any, Optional

# .env 파일 로드 (API 키 등 민감정보)
from dotenv import load_dotenv
load_dotenv()

# Gemini API
import google.generativeai as genai

# Prometheus 클라이언트
from prometheus_api_client import PrometheusConnect
from prometheus_api_client.utils import parse_datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_prometheus_url() -> str:
    """
    실행 환경에 따라 적절한 Prometheus URL 반환

    - Mac (Darwin): localhost:9090 사용 (로컬 Docker 컨테이너)
    - 기타: .env의 PROMETHEUS_URL 사용 (Tailscale 원격 접속)
    """
    # 환경변수에서 명시적으로 지정된 경우 우선 사용
    env_url = os.getenv("PROMETHEUS_URL")

    # Mac 환경인 경우 localhost 사용
    if platform.system() == "Darwin":
        logger.info("Mac 환경 감지: localhost:9090 사용")
        return "http://localhost:9090"

    # 기타 환경에서는 .env의 PROMETHEUS_URL 사용 (Tailscale)
    if env_url:
        logger.info(f"외부 환경: {env_url} 사용")
        return env_url

    # 기본값
    return "http://100.105.22.101:9090"


class MetricsCollector:
    """Prometheus에서 메트릭 데이터를 수집하는 클래스"""

    def __init__(self, prometheus_url: str = "http://localhost:9090"):
        self.prometheus_url = prometheus_url
        self.prom = None

    def connect(self) -> bool:
        """Prometheus 서버에 연결"""
        try:
            self.prom = PrometheusConnect(url=self.prometheus_url, disable_ssl=True)
            # 연결 테스트
            self.prom.check_prometheus_connection()
            logger.info(f"Prometheus 연결 성공: {self.prometheus_url}")
            return True
        except Exception as e:
            logger.warning(f"Prometheus 연결 실패: {e}")
            return False

    def query_range(self, query: str, start_time: datetime, end_time: datetime, step: str = "60s") -> List[Dict]:
        """시간 범위에 대한 쿼리 실행"""
        try:
            result = self.prom.custom_query_range(
                query=query,
                start_time=start_time,
                end_time=end_time,
                step=step
            )
            return result
        except Exception as e:
            logger.warning(f"쿼리 실패 ({query}): {e}")
            return []

    def query_instant(self, query: str) -> List[Dict]:
        """즉시 쿼리 실행"""
        try:
            result = self.prom.custom_query(query=query)
            return result
        except Exception as e:
            logger.warning(f"즉시 쿼리 실패 ({query}): {e}")
            return []

    def collect_crawler_metrics(self, time_range_minutes: int = 60) -> Dict[str, Any]:
        """크롤러 관련 메트릭 수집"""
        end_time = datetime.now()
        start_time = end_time - timedelta(minutes=time_range_minutes)

        metrics = {
            "collection_time": datetime.now().isoformat(),
            "time_range_minutes": time_range_minutes,
            "data": {}
        }

        # 수집할 메트릭 쿼리 목록
        queries = {
            # === 크롤러 작업 메트릭 ===
            "crawler_tasks_completed": "crawler_tasks_completed_total",
            "crawler_tasks_failed": "crawler_tasks_failed_total",
            "crawler_tasks_success_rate": "rate(crawler_tasks_completed_total[5m]) / (rate(crawler_tasks_completed_total[5m]) + rate(crawler_tasks_failed_total[5m])) * 100",

            # === 크롤러 큐 메트릭 ===
            "crawler_queue_pending": "crawler_queue_pending",
            "crawler_queue_processing": "crawler_queue_processing",
            "crawler_shard_pending": "crawler_shard_pending",

            # === 크롤러 시스템 리소스 ===
            "crawler_cpu_percent": "crawler_system_cpu_percent",
            "crawler_memory_percent": "crawler_system_memory_percent",

            # === 크롤러 처리 지연 시간 (Latency) ===
            "crawler_latency_avg": "rate(crawler_process_latency_seconds_sum[5m]) / rate(crawler_process_latency_seconds_count[5m])",
            "crawler_latency_p95": "histogram_quantile(0.95, rate(crawler_process_latency_seconds_bucket[5m]))",
            "crawler_latency_p99": "histogram_quantile(0.99, rate(crawler_process_latency_seconds_bucket[5m]))",

            # === PostgreSQL 크롤러 통계 (postgres-exporter) ===
            "pg_crawler_throughput": "pg_crawler_throughput_pages_per_second",
            "pg_crawler_total_pages": "pg_crawler_stats_total_pages",
            "pg_crawler_pages_1min": "pg_crawler_stats_pages_last_1min",
            "pg_crawler_pages_5min": "pg_crawler_stats_pages_last_5min",
            "pg_crawler_pages_1hour": "pg_crawler_stats_pages_last_1hour",
            "pg_crawler_unique_domains": "pg_crawler_stats_unique_domains",
            "pg_crawler_avg_content_length": "pg_crawler_stats_avg_content_length",

            # === PostgreSQL DLQ (Dead Letter Queue) ===
            "pg_crawler_dlq_total": "pg_crawler_dlq_dlq_total",
            "pg_crawler_dlq_unique_errors": "pg_crawler_dlq_dlq_unique_errors",
            "pg_crawler_dlq_last_1hour": "pg_crawler_dlq_dlq_last_1hour",

            # === PostgreSQL DB 메트릭 ===
            "pg_database_size_bytes": "pg_database_size_bytes",
            "pg_locks_count": "pg_locks_count",

            # === 크롤링 에러 분석 메트릭 (NEW) ===
            # 에러 타입별 총 발생 횟수
            "crawl_errors_by_type": "sum by (error_type) (crawl_errors_total)",

            # 문제 도메인 TOP 10 (가장 많이 실패한 도메인)
            "crawl_errors_by_domain_top10": "topk(10, sum by (domain) (domain_failures_total))",

            # HTTP 상태 코드 분포
            "crawl_http_status_distribution": "sum by (status_code) (crawl_http_status_total)",

            # 워커별 성공률
            "crawl_worker_success_rates": "crawl_success_rate",

            # 에러 발생률 추이 (5분 단위)
            "crawl_error_rate_trend": "rate(crawl_errors_total[5m])",

            # 전체 요청 수 (성공/실패별)
            "crawl_requests_success": "sum(crawl_requests_total{status='success'})",
            "crawl_requests_failed": "sum(crawl_requests_total{status='failed'})",

            # 특정 에러 타입별 상세 카운트
            "crawl_timeout_errors": "sum(crawl_errors_total{error_type='timeout'})",
            "crawl_connection_errors": "sum(crawl_errors_total{error_type='connection_error'})",
            "crawl_dns_errors": "sum(crawl_errors_total{error_type='dns_error'})",
            "crawl_ssl_errors": "sum(crawl_errors_total{error_type='ssl_error'})",
            "crawl_http_403_errors": "sum(crawl_errors_total{error_type='http_403'})",
            "crawl_http_404_errors": "sum(crawl_errors_total{error_type='http_404'})",
            "crawl_http_429_errors": "sum(crawl_errors_total{error_type='http_429'})",
            "crawl_http_5xx_errors": "sum(crawl_errors_total{error_type='http_5xx'})",
            "crawl_robots_blocked_errors": "sum(crawl_errors_total{error_type='robots_blocked'})",
            "crawl_content_errors": "sum(crawl_errors_total{error_type='content_error'})",
            "crawl_unknown_errors": "sum(crawl_errors_total{error_type='unknown'})",

            # 응답 시간 분석 (성공 vs 실패)
            "crawl_response_time_success_p50": "histogram_quantile(0.5, sum by (le) (crawl_response_time_seconds_bucket{success='true'}))",
            "crawl_response_time_success_p95": "histogram_quantile(0.95, sum by (le) (crawl_response_time_seconds_bucket{success='true'}))",
            "crawl_response_time_failed_p50": "histogram_quantile(0.5, sum by (le) (crawl_response_time_seconds_bucket{success='false'}))",
            "crawl_response_time_failed_p95": "histogram_quantile(0.95, sum by (le) (crawl_response_time_seconds_bucket{success='false'}))",

            # 도메인별 에러율 TOP 10
            "crawl_domain_error_rate_top10": "topk(10, sum by (domain) (domain_failures_total) / (sum by (domain) (domain_failures_total) + sum by (domain) (crawl_requests_total{status='success'})) * 100)",
        }

        for metric_name, query in queries.items():
            try:
                # 범위 쿼리 시도
                range_data = self.query_range(query, start_time, end_time)
                if range_data:
                    metrics["data"][metric_name] = {
                        "type": "range",
                        "values": self._extract_values(range_data)
                    }
                else:
                    # 즉시 쿼리 시도
                    instant_data = self.query_instant(query)
                    if instant_data:
                        metrics["data"][metric_name] = {
                            "type": "instant",
                            "value": self._extract_instant_value(instant_data)
                        }
            except Exception as e:
                logger.debug(f"메트릭 수집 실패 ({metric_name}): {e}")

        return metrics

    def _extract_values(self, result: List[Dict]) -> List[Dict]:
        """범위 쿼리 결과에서 값 추출"""
        extracted = []
        for item in result:
            metric_labels = item.get("metric", {})
            values = item.get("values", [])
            if values:
                # 최근 값, 최대값, 최소값, 평균값 계산
                float_values = [float(v[1]) for v in values if v[1] != "NaN"]
                if float_values:
                    extracted.append({
                        "labels": metric_labels,
                        "latest": float_values[-1],
                        "max": max(float_values),
                        "min": min(float_values),
                        "avg": sum(float_values) / len(float_values),
                        "sample_count": len(float_values)
                    })
        return extracted

    def _extract_instant_value(self, result: List[Dict]) -> Any:
        """즉시 쿼리 결과에서 값 추출"""
        if result and len(result) > 0:
            value = result[0].get("value", [None, None])
            if len(value) > 1:
                try:
                    return float(value[1])
                except:
                    return value[1]
        return None


class TestResultLoader:
    """Pytest 테스트 결과를 로드하는 클래스"""

    def __init__(self, report_path: str = "test_report.json"):
        self.report_path = Path(report_path)

    def load(self) -> Optional[Dict]:
        """테스트 결과 JSON 파일 로드"""
        if not self.report_path.exists():
            logger.warning(f"테스트 리포트 파일이 없습니다: {self.report_path}")
            return None

        try:
            with open(self.report_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            logger.info(f"테스트 리포트 로드 완료: {self.report_path}")
            return data
        except Exception as e:
            logger.error(f"테스트 리포트 로드 실패: {e}")
            return None

    def summarize(self, data: Dict) -> Dict:
        """테스트 결과 요약"""
        if not data:
            return {}

        summary = {
            "total_tests": data.get("summary", {}).get("total", 0),
            "passed": data.get("summary", {}).get("passed", 0),
            "failed": data.get("summary", {}).get("failed", 0),
            "errors": data.get("summary", {}).get("error", 0),
            "skipped": data.get("summary", {}).get("skipped", 0),
            "duration": data.get("duration", 0),
            "failed_tests": []
        }

        # 실패한 테스트 상세 정보 수집
        tests = data.get("tests", [])
        for test in tests:
            if test.get("outcome") in ["failed", "error"]:
                summary["failed_tests"].append({
                    "name": test.get("nodeid", "unknown"),
                    "outcome": test.get("outcome"),
                    "message": test.get("call", {}).get("longrepr", "")[:500]
                })

        return summary


class GeminiReportGenerator:
    """Gemini API를 사용하여 리포트를 생성하는 클래스"""

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.model = None
        self._configure()

    def _configure(self):
        """Gemini API 설정"""
        try:
            genai.configure(api_key=self.api_key)
            self.model = genai.GenerativeModel('gemini-2.5-flash')
            logger.info("Gemini API 설정 완료")
        except Exception as e:
            logger.error(f"Gemini API 설정 실패: {e}")
            raise

    def generate_report(self, metrics: Dict, test_results: Optional[Dict], previous_report: Optional[str] = None, max_retries: int = 3) -> str:
        """메트릭과 테스트 결과를 분석하여 리포트 생성 (자동 재시도 포함)"""
        prompt = self._build_prompt(metrics, test_results, previous_report)

        for attempt in range(max_retries):
            try:
                response = self.model.generate_content(
                    prompt,
                    generation_config=genai.types.GenerationConfig(
                        temperature=0.3,
                        max_output_tokens=4096,
                    )
                )
                return response.text
            except Exception as e:
                error_str = str(e)
                # Rate limit (429) 에러인 경우 재시도
                if "429" in error_str or "quota" in error_str.lower():
                    wait_time = 30 * (attempt + 1)  # 30초, 60초, 90초
                    logger.warning(f"Rate limit 초과. {wait_time}초 후 재시도... ({attempt + 1}/{max_retries})")
                    time.sleep(wait_time)
                    continue
                else:
                    logger.error(f"리포트 생성 실패: {e}")
                    raise

        raise Exception(f"최대 재시도 횟수({max_retries})를 초과했습니다.")

    def _build_prompt(self, metrics: Dict, test_results: Optional[Dict], previous_report: Optional[str] = None) -> str:
        """분석 프롬프트 구성"""

        # 기본 프롬프트
        if previous_report:
            # 이전 리포트가 있는 경우 - 비교 분석 요청
            prompt = """당신은 분산 크롤러 시스템의 성능 분석 전문가입니다.
아래 제공된 Prometheus 메트릭 데이터와 테스트 결과를 분석하여
한국어로 된 기술 리포트를 작성해주세요.

**중요: 이전 리포트와 비교하여 변화 사항을 분석해주세요.**

## 요청사항
1. **성능 요약 (Performance Summary)**:
   - 전체적인 시스템 성능을 요약
   - **이전 리포트 대비 개선/악화된 지표를 명확히 표시** (예: ↑ 10% 증가, ↓ 5% 감소)

2. **변화 분석 (Change Analysis)**:
   - 이전 리포트와 비교하여 주요 변화 사항 식별
   - 성능이 개선된 부분과 악화된 부분을 구분하여 설명
   - 변화의 원인 추정

3. **병목 구간 분석 (Bottleneck Analysis)**:
   - 현재 성능 저하가 발생하는 지점 식별
   - 이전에 없던 새로운 병목이 발생했다면 강조

4. **에러 분석 (Error Analysis)**:
   - 에러 타입별 발생 빈도 분석 (timeout, connection_error, http_403, http_404, http_429, http_5xx 등)
   - 가장 많이 실패한 도메인 TOP 10 분석
   - HTTP 상태 코드 분포 분석
   - 워커별 성공률 비교
   - 에러 발생 추이 분석 (증가/감소 추세)
   - 실패한 요청의 응답 시간과 성공 요청의 응답 시간 비교
   - 주요 문제점 및 원인 추정

5. **개선 제안 (Recommendations)**:
   - 구체적인 개선 방안 제시
   - 에러율을 낮추기 위한 구체적인 방안 포함
   - 우선순위가 높은 개선 사항 강조

## 출력 형식
- Markdown 형식으로 작성
- 기술 용어는 영어 병기 (예: 처리량(Throughput))
- 수치 데이터는 표로 정리
- **변화가 있는 지표는 화살표와 함께 표시** (↑, ↓, →)
- 중요한 인사이트는 강조 표시

---

## 이전 리포트 (비교 대상)
```markdown
{previous_report}
```

---

## 현재 수집된 메트릭 데이터
```json
{metrics_json}
```

"""
        else:
            # 첫 번째 리포트인 경우 - 일반 분석
            prompt = """당신은 분산 크롤러 시스템의 성능 분석 전문가입니다.
아래 제공된 Prometheus 메트릭 데이터와 테스트 결과를 분석하여
한국어로 된 기술 리포트를 작성해주세요.

**참고: 이것은 첫 번째 리포트입니다. 이전 데이터가 없으므로 현재 상태만 분석합니다.**

## 요청사항
1. **성능 요약 (Performance Summary)**: 전체적인 시스템 성능을 요약해주세요.
2. **병목 구간 분석 (Bottleneck Analysis)**: 성능 저하가 발생할 수 있는 지점을 식별해주세요.
3. **에러 분석 (Error Analysis)**:
   - 에러 타입별 발생 빈도 분석 (timeout, connection_error, http_403, http_404, http_429, http_5xx 등)
   - 가장 많이 실패한 도메인 TOP 10 분석
   - HTTP 상태 코드 분포 분석
   - 워커별 성공률 비교
   - 에러 발생 추이 분석
   - 실패한 요청의 응답 시간과 성공 요청의 응답 시간 비교
   - 주요 문제점 및 원인 추정
4. **개선 제안 (Recommendations)**: 구체적인 개선 방안을 제시해주세요. 특히 에러율을 낮추기 위한 방안을 포함해주세요.

## 출력 형식
- Markdown 형식으로 작성
- 기술 용어는 영어 병기 (예: 처리량(Throughput))
- 수치 데이터는 표로 정리
- 중요한 인사이트는 강조 표시

---

## 수집된 메트릭 데이터
```json
{metrics_json}
```

"""

        # 테스트 결과 추가
        if test_results:
            prompt += """
## 테스트 결과
```json
{test_json}
```
"""
            format_dict = {
                "metrics_json": json.dumps(metrics, indent=2, ensure_ascii=False, default=str),
                "test_json": json.dumps(test_results, indent=2, ensure_ascii=False, default=str)
            }
        else:
            prompt += "\n## 테스트 결과\n테스트 결과 파일이 제공되지 않았습니다.\n"
            format_dict = {
                "metrics_json": json.dumps(metrics, indent=2, ensure_ascii=False, default=str)
            }

        # 이전 리포트 추가 (있는 경우)
        if previous_report:
            format_dict["previous_report"] = previous_report

        prompt = prompt.format(**format_dict)

        prompt += """
---

위 데이터를 바탕으로 상세한 기술 리포트를 작성해주세요.
리포트 제목은 "Crawler System Performance Report"로 시작해주세요.
"""

        return prompt


class ReportSaver:
    """리포트를 파일로 저장하는 클래스"""

    def __init__(self, output_dir: str = "docs/reports"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def get_latest_report(self) -> Optional[str]:
        """가장 최근에 생성된 리포트 파일의 내용을 반환"""
        try:
            # report_*.md 패턴의 파일들을 찾기
            report_files = list(self.output_dir.glob("report_*.md"))

            if not report_files:
                logger.info("이전 리포트가 없습니다. 첫 번째 리포트를 생성합니다.")
                return None

            # 수정 시간 기준으로 정렬하여 가장 최근 파일 찾기
            latest_file = max(report_files, key=lambda p: p.stat().st_mtime)

            with open(latest_file, 'r', encoding='utf-8') as f:
                content = f.read()

            logger.info(f"이전 리포트 발견: {latest_file.name}")
            return content
        except Exception as e:
            logger.warning(f"이전 리포트 로드 실패: {e}")
            return None

    def save(self, content: str) -> str:
        """리포트 저장 및 파일 경로 반환"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        filename = f"report_{timestamp}.md"
        filepath = self.output_dir / filename

        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                # 메타데이터 헤더 추가
                f.write(f"---\n")
                f.write(f"generated_at: {datetime.now().isoformat()}\n")
                f.write(f"generator: Gemini 1.5 Flash\n")
                f.write(f"---\n\n")
                f.write(content)

            logger.info(f"리포트 저장 완료: {filepath}")
            return str(filepath)
        except Exception as e:
            logger.error(f"리포트 저장 실패: {e}")
            raise


def main():
    """메인 함수"""
    parser = argparse.ArgumentParser(description="Gemini 기반 크롤러 성능 리포트 생성기")
    parser.add_argument(
        "--prometheus-url",
        default=None,  # None으로 설정하여 자동 감지 활성화
        help="Prometheus 서버 URL (미지정 시 자동 감지: Mac=localhost:9090, 기타=Tailscale URL)"
    )
    parser.add_argument(
        "--time-range",
        type=int,
        default=60,
        help="분석할 시간 범위 (분 단위, 기본값: 60)"
    )
    parser.add_argument(
        "--test-report",
        default="test_report.json",
        help="Pytest 결과 JSON 파일 경로"
    )
    parser.add_argument(
        "--output-dir",
        default="docs/reports",
        help="리포트 출력 디렉토리"
    )
    args = parser.parse_args()

    # API 키 확인
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        logger.error("GEMINI_API_KEY 환경 변수가 설정되지 않았습니다.")
        sys.exit(1)

    logger.info("=" * 60)
    logger.info("크롤러 성능 리포트 생성 시작")
    logger.info("=" * 60)

    # Prometheus URL 자동 결정
    prometheus_url = args.prometheus_url if args.prometheus_url else get_prometheus_url()
    logger.info(f"Prometheus URL: {prometheus_url}")

    # 1. Prometheus 메트릭 수집
    logger.info("[1/4] Prometheus 메트릭 수집 중...")
    collector = MetricsCollector(prometheus_url)

    if collector.connect():
        metrics = collector.collect_crawler_metrics(args.time_range)
        logger.info(f"  - 수집된 메트릭 수: {len(metrics.get('data', {}))}")
    else:
        logger.warning("  - Prometheus 연결 실패, 빈 메트릭으로 진행")
        metrics = {
            "collection_time": datetime.now().isoformat(),
            "time_range_minutes": args.time_range,
            "data": {},
            "note": "Prometheus 서버에 연결할 수 없어 메트릭을 수집하지 못했습니다."
        }

    # 2. 테스트 결과 로드
    logger.info("[2/4] 테스트 결과 로드 중...")
    test_loader = TestResultLoader(args.test_report)
    test_data = test_loader.load()
    test_summary = test_loader.summarize(test_data) if test_data else None

    if test_summary:
        logger.info(f"  - 총 테스트: {test_summary.get('total_tests', 0)}")
        logger.info(f"  - 성공: {test_summary.get('passed', 0)}, 실패: {test_summary.get('failed', 0)}")
    else:
        logger.info("  - 테스트 결과 없음 (Skip)")

    # 3. 이전 리포트 로드 (있는 경우)
    logger.info("[3/5] 이전 리포트 확인 중...")
    saver = ReportSaver(args.output_dir)
    previous_report = saver.get_latest_report()

    if previous_report:
        logger.info("  - 이전 리포트를 참고하여 비교 분석 수행")
    else:
        logger.info("  - 첫 번째 리포트 생성 (비교 대상 없음)")

    # 4. Gemini 리포트 생성
    logger.info("[4/5] Gemini API로 리포트 생성 중...")
    try:
        generator = GeminiReportGenerator(api_key)
        report_content = generator.generate_report(metrics, test_summary, previous_report)
        logger.info("  - 리포트 생성 완료")
    except Exception as e:
        logger.error(f"리포트 생성 실패: {e}")
        sys.exit(1)

    # 5. 리포트 저장
    logger.info("[5/5] 리포트 저장 중...")
    saved_path = saver.save(report_content)

    logger.info("=" * 60)
    logger.info(f"리포트 생성 완료: {saved_path}")
    logger.info("=" * 60)

    return saved_path


if __name__ == "__main__":
    main()

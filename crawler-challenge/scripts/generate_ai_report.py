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
            # TPS (Transactions Per Second)
            "tps": "rate(crawler_requests_total[5m])",
            "tps_avg": "avg_over_time(rate(crawler_requests_total[5m])[{}m:1m])".format(time_range_minutes),

            # DB Connections
            "db_connections_active": "pg_stat_activity_count",
            "db_connections_idle": "pg_stat_activity_count{state='idle'}",

            # Rollback Rate
            "db_rollbacks_total": "pg_stat_database_xact_rollback",
            "db_commits_total": "pg_stat_database_xact_commit",

            # CPU Usage
            "cpu_usage_percent": "100 - (avg(rate(node_cpu_seconds_total{mode='idle'}[5m])) * 100)",

            # Memory Usage
            "memory_usage_percent": "(1 - (node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes)) * 100",

            # Crawler specific metrics
            "crawler_pages_crawled": "crawler_pages_crawled_total",
            "crawler_errors_total": "crawler_errors_total",
            "crawler_queue_size": "crawler_queue_size",
            "crawler_batch_size": "crawler_batch_size",
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

    def __init__(self, report_path: str = "docs/test_report.json"):
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

    def generate_report(self, metrics: Dict, test_results: Optional[Dict], max_retries: int = 3) -> str:
        """메트릭과 테스트 결과를 분석하여 리포트 생성 (자동 재시도 포함)"""
        prompt = self._build_prompt(metrics, test_results)

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
                    wait_time = 10 * (attempt + 1)  # 10초, 20초, 30초
                    logger.warning(f"Rate limit 초과. {wait_time}초 후 재시도... ({attempt + 1}/{max_retries})")
                    time.sleep(wait_time)
                    continue
                else:
                    logger.error(f"리포트 생성 실패: {e}")
                    raise

        raise Exception(f"최대 재시도 횟수({max_retries})를 초과했습니다.")

    def _build_prompt(self, metrics: Dict, test_results: Optional[Dict]) -> str:
        """분석 프롬프트 구성"""
        prompt = """당신은 분산 크롤러 시스템의 성능 분석 전문가입니다.
아래 제공된 Prometheus 메트릭 데이터와 테스트 결과를 분석하여
한국어로 된 기술 리포트를 작성해주세요.

## 요청사항
1. **성능 요약 (Performance Summary)**: 전체적인 시스템 성능을 요약해주세요.
2. **병목 구간 분석 (Bottleneck Analysis)**: 성능 저하가 발생할 수 있는 지점을 식별해주세요.
3. **개선 제안 (Recommendations)**: 구체적인 개선 방안을 제시해주세요.

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

        if test_results:
            prompt += """
## 테스트 결과
```json
{test_json}
```
"""
            prompt = prompt.format(
                metrics_json=json.dumps(metrics, indent=2, ensure_ascii=False, default=str),
                test_json=json.dumps(test_results, indent=2, ensure_ascii=False, default=str)
            )
        else:
            prompt += "\n## 테스트 결과\n테스트 결과 파일이 제공되지 않았습니다.\n"
            prompt = prompt.format(
                metrics_json=json.dumps(metrics, indent=2, ensure_ascii=False, default=str)
            )

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
        default=os.getenv("PROMETHEUS_URL", "http://100.105.22.101:9090"),
        help="Prometheus 서버 URL (Tailscale: 윈도우 데스크탑)"
    )
    parser.add_argument(
        "--time-range",
        type=int,
        default=60,
        help="분석할 시간 범위 (분 단위, 기본값: 60)"
    )
    parser.add_argument(
        "--test-report",
        default="docs/test_report.json",
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

    # 1. Prometheus 메트릭 수집
    logger.info("[1/4] Prometheus 메트릭 수집 중...")
    collector = MetricsCollector(args.prometheus_url)

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

    # 3. Gemini 리포트 생성
    logger.info("[3/4] Gemini API로 리포트 생성 중...")
    try:
        generator = GeminiReportGenerator(api_key)
        report_content = generator.generate_report(metrics, test_summary)
        logger.info("  - 리포트 생성 완료")
    except Exception as e:
        logger.error(f"리포트 생성 실패: {e}")
        sys.exit(1)

    # 4. 리포트 저장
    logger.info("[4/4] 리포트 저장 중...")
    saver = ReportSaver(args.output_dir)
    saved_path = saver.save(report_content)

    logger.info("=" * 60)
    logger.info(f"리포트 생성 완료: {saved_path}")
    logger.info("=" * 60)

    return saved_path


if __name__ == "__main__":
    main()

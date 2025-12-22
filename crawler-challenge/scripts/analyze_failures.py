#!/usr/bin/env python3
"""
크롤링 실패 원인 분석 스크립트

사용법:
    python scripts/analyze_failures.py
    python scripts/analyze_failures.py --hours 24
    python scripts/analyze_failures.py --domain example.com
"""

import os
import sys
import argparse
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional

import psycopg2
from psycopg2.extras import RealDictCursor

# 환경 변수에서 DB 설정 로드
DB_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'localhost'),
    'port': os.getenv('POSTGRES_PORT', '5432'),
    'database': os.getenv('POSTGRES_DB', 'crawler_db'),
    'user': os.getenv('POSTGRES_USER', 'postgres'),
    'password': os.getenv('POSTGRES_PASSWORD', 'postgres')
}


def get_connection():
    """데이터베이스 연결 생성"""
    return psycopg2.connect(**DB_CONFIG, cursor_factory=RealDictCursor)


def print_header(title: str, width: int = 60):
    """섹션 헤더 출력"""
    print()
    print("=" * width)
    print(f" {title}")
    print("=" * width)


def print_table(headers: List[str], rows: List[Tuple], col_widths: List[int] = None):
    """테이블 형식으로 출력"""
    if not col_widths:
        col_widths = [max(len(str(h)), max(len(str(row[i])) for row in rows) if rows else 0)
                      for i, h in enumerate(headers)]

    # 헤더 출력
    header_line = " | ".join(str(h).ljust(w) for h, w in zip(headers, col_widths))
    print(header_line)
    print("-" * len(header_line))

    # 데이터 출력
    for row in rows:
        row_line = " | ".join(str(v).ljust(w) for v, w in zip(row, col_widths))
        print(row_line)


def analyze_overall_stats(cursor, hours: int = None) -> Dict:
    """전체 통계 분석"""
    time_filter = ""
    if hours:
        time_filter = f"WHERE crawl_time > NOW() - INTERVAL '{hours} hours'"

    cursor.execute(f"""
        SELECT
            COUNT(*) as total,
            SUM(CASE WHEN success THEN 1 ELSE 0 END) as successes,
            SUM(CASE WHEN success THEN 0 ELSE 1 END) as failures,
            ROUND(AVG(response_time)::numeric, 2) as avg_response_time
        FROM crawl_results
        {time_filter}
    """)

    result = cursor.fetchone()
    return dict(result) if result else {}


def analyze_error_types(cursor, hours: int = None) -> List[Dict]:
    """에러 타입별 분석"""
    time_filter = ""
    if hours:
        time_filter = f"AND crawl_time > NOW() - INTERVAL '{hours} hours'"

    cursor.execute(f"""
        SELECT
            error_type,
            COUNT(*) as count,
            ROUND(COUNT(*) * 100.0 / NULLIF(
                (SELECT COUNT(*) FROM crawl_results WHERE success = false {time_filter.replace('AND', 'AND' if time_filter else '')}),
                0
            ), 2) as percentage
        FROM crawl_results
        WHERE success = false {time_filter}
        GROUP BY error_type
        ORDER BY count DESC
    """)

    return [dict(row) for row in cursor.fetchall()]


def analyze_failing_domains(cursor, hours: int = None, limit: int = 20) -> List[Dict]:
    """실패율 높은 도메인 분석"""
    time_filter = ""
    if hours:
        time_filter = f"WHERE crawl_time > NOW() - INTERVAL '{hours} hours'"

    cursor.execute(f"""
        SELECT
            domain,
            COUNT(*) as attempts,
            SUM(CASE WHEN success THEN 0 ELSE 1 END) as failures,
            ROUND(SUM(CASE WHEN success THEN 0 ELSE 1 END) * 100.0 / NULLIF(COUNT(*), 0), 2) as failure_rate
        FROM crawl_results
        {time_filter}
        GROUP BY domain
        HAVING COUNT(*) >= 3
        ORDER BY failure_rate DESC
        LIMIT {limit}
    """)

    return [dict(row) for row in cursor.fetchall()]


def analyze_domain_detail(cursor, domain: str) -> Dict:
    """특정 도메인 상세 분석"""
    cursor.execute("""
        SELECT
            COUNT(*) as total_attempts,
            SUM(CASE WHEN success THEN 1 ELSE 0 END) as successes,
            SUM(CASE WHEN success THEN 0 ELSE 1 END) as failures,
            ROUND(SUM(CASE WHEN success THEN 0 ELSE 1 END) * 100.0 / NULLIF(COUNT(*), 0), 2) as failure_rate,
            ROUND(AVG(response_time)::numeric, 2) as avg_response_time,
            MAX(CASE WHEN success THEN crawl_time END) as last_success,
            MAX(CASE WHEN NOT success THEN crawl_time END) as last_failure
        FROM crawl_results
        WHERE domain = %s
    """, (domain,))

    result = cursor.fetchone()

    # 에러 타입별 분포
    cursor.execute("""
        SELECT error_type, COUNT(*) as count
        FROM crawl_results
        WHERE domain = %s AND success = false
        GROUP BY error_type
        ORDER BY count DESC
    """, (domain,))

    error_types = [dict(row) for row in cursor.fetchall()]

    return {
        'stats': dict(result) if result else {},
        'error_types': error_types
    }


def analyze_response_times(cursor, hours: int = None) -> Dict:
    """응답 시간 분석"""
    time_filter = ""
    if hours:
        time_filter = f"WHERE crawl_time > NOW() - INTERVAL '{hours} hours'"

    cursor.execute(f"""
        SELECT
            CASE WHEN success THEN 'Success' ELSE 'Failure' END as status,
            COUNT(*) as count,
            ROUND(AVG(response_time)::numeric, 2) as avg,
            ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY response_time)::numeric, 2) as median,
            ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY response_time)::numeric, 2) as p95
        FROM crawl_results
        {time_filter}
        {"AND" if time_filter else "WHERE"} response_time IS NOT NULL
        GROUP BY success
    """)

    return [dict(row) for row in cursor.fetchall()]


def print_analysis_report(hours: int = None, domain: str = None):
    """분석 리포트 출력"""
    conn = get_connection()
    cursor = conn.cursor()

    try:
        time_desc = f"Last {hours} hours" if hours else "All time"
        print_header(f"CRAWL FAILURE ANALYSIS REPORT ({time_desc})")

        # 1. 전체 통계
        print_header("1. Overall Statistics", 40)
        stats = analyze_overall_stats(cursor, hours)
        if stats and stats.get('total'):
            total = stats['total']
            successes = stats['successes'] or 0
            failures = stats['failures'] or 0
            success_rate = (successes / total * 100) if total > 0 else 0

            print(f"  Total Requests:  {total:,}")
            print(f"  Successes:       {successes:,} ({success_rate:.1f}%)")
            print(f"  Failures:        {failures:,} ({100-success_rate:.1f}%)")
            print(f"  Avg Response:    {stats['avg_response_time']}s")
        else:
            print("  No data available")

        # 2. 에러 타입 분포
        print_header("2. Error Type Distribution", 40)
        error_types = analyze_error_types(cursor, hours)
        if error_types:
            print_table(
                ["Error Type", "Count", "%"],
                [(e['error_type'] or 'unknown', e['count'], f"{e['percentage']}%")
                 for e in error_types],
                [25, 10, 10]
            )
        else:
            print("  No errors found")

        # 3. 실패율 높은 도메인
        print_header("3. Top Failing Domains", 40)
        failing_domains = analyze_failing_domains(cursor, hours)
        if failing_domains:
            print_table(
                ["Domain", "Attempts", "Failures", "Rate"],
                [(d['domain'][:30], d['attempts'], d['failures'], f"{d['failure_rate']}%")
                 for d in failing_domains[:10]],
                [30, 10, 10, 10]
            )
        else:
            print("  No failing domains found")

        # 4. 응답 시간 통계
        print_header("4. Response Time Statistics", 40)
        response_times = analyze_response_times(cursor, hours)
        if response_times:
            print_table(
                ["Status", "Count", "Avg", "Median", "P95"],
                [(r['status'], r['count'], f"{r['avg']}s", f"{r['median']}s", f"{r['p95']}s")
                 for r in response_times],
                [10, 10, 10, 10, 10]
            )
        else:
            print("  No response time data")

        # 5. 특정 도메인 상세 (옵션)
        if domain:
            print_header(f"5. Domain Detail: {domain}", 40)
            domain_detail = analyze_domain_detail(cursor, domain)
            if domain_detail['stats'].get('total_attempts'):
                stats = domain_detail['stats']
                print(f"  Total Attempts:   {stats['total_attempts']}")
                print(f"  Successes:        {stats['successes']}")
                print(f"  Failures:         {stats['failures']}")
                print(f"  Failure Rate:     {stats['failure_rate']}%")
                print(f"  Avg Response:     {stats['avg_response_time']}s")
                print(f"  Last Success:     {stats['last_success']}")
                print(f"  Last Failure:     {stats['last_failure']}")

                if domain_detail['error_types']:
                    print("\n  Error Types:")
                    for e in domain_detail['error_types']:
                        print(f"    - {e['error_type']}: {e['count']}")
            else:
                print(f"  No data for domain: {domain}")

        print()
        print("=" * 60)
        print(f" Report generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 60)

    finally:
        cursor.close()
        conn.close()


def main():
    parser = argparse.ArgumentParser(
        description='Crawl Failure Analysis Tool',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python analyze_failures.py                    # Analyze all data
    python analyze_failures.py --hours 24         # Last 24 hours
    python analyze_failures.py --domain google.com # Specific domain
    python analyze_failures.py --hours 1 --domain example.com
        """
    )
    parser.add_argument('--hours', type=int, default=None,
                        help='Analyze data from last N hours')
    parser.add_argument('--domain', type=str, default=None,
                        help='Analyze specific domain in detail')

    args = parser.parse_args()

    try:
        print_analysis_report(hours=args.hours, domain=args.domain)
    except psycopg2.Error as e:
        print(f"Database error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()

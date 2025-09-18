#!/usr/bin/env python3
"""
크롤러 상태 모니터링 대시보드
실시간 상태 확인 및 통계 조회
"""

import time
import json
from datetime import datetime, timedelta
from progress_tracker import ProgressTracker
import argparse
import sys

def format_uptime(minutes):
    """업타임을 읽기 쉬운 형식으로 변환"""
    if minutes < 60:
        return f"{minutes}분"
    hours = minutes // 60
    remaining_minutes = minutes % 60
    if hours < 24:
        return f"{hours}시간 {remaining_minutes}분"
    days = hours // 24
    remaining_hours = hours % 24
    return f"{days}일 {remaining_hours}시간 {remaining_minutes}분"

def display_dashboard():
    """대시보드 표시"""
    try:
        tracker = ProgressTracker()

        while True:
            # 터미널 클리어
            print("\033[2J\033[H")

            # 헤더
            print("=" * 80)
            print(f"{'🕷️  크롤러 상태 대시보드':^80}")
            print(f"{'업데이트: ' + datetime.now().strftime('%Y-%m-%d %H:%M:%S'):^80}")
            print("=" * 80)

            # 대시보드 통계 조회
            stats = tracker.get_dashboard_stats()

            if not stats:
                print("❌ 활성 크롤러 세션이 없습니다.")
                print("\n새로고침하려면 Ctrl+C를 누르고 다시 실행하세요.")
                return

            # 기본 통계
            print(f"\n📊 기본 통계")
            print(f"세션 ID    : {stats['session_id']}")
            print(f"상태       : {stats['status']}")
            print(f"업타임     : {format_uptime(stats['uptime_minutes'])}")
            print(f"활성 세션  : {stats['active_sessions']}개")

            # 크롤링 통계
            print(f"\n🕸️  크롤링 통계")
            print(f"총 처리    : {stats['total_processed']:,}개")
            print(f"성공       : {stats['success_count']:,}개")
            print(f"실패       : {stats['error_count']:,}개")
            print(f"성공률     : {(1-stats['error_rate'])*100:.1f}%")
            print(f"처리속도   : {stats['pages_per_minute']:.1f} pages/분")

            # 에러 분석
            if stats['error_breakdown']:
                print(f"\n🚨 에러 분석")
                for error_type, count in stats['error_breakdown'].items():
                    print(f"{error_type:15s}: {count}개")

            # 메모리 정보 (최근 데이터)
            memory_key = f"crawler:memory:{stats['session_id']}"
            try:
                latest_memory = tracker.redis_client.lindex(memory_key, 0)
                if latest_memory:
                    memory_data = json.loads(latest_memory)
                    print(f"\n💾 메모리 사용량")
                    print(f"프로세스   : {memory_data.get('process_memory_mb', 0):.1f} MB")
                    print(f"시스템     : {memory_data.get('system_memory_percent', 0):.1f}%")
            except:
                pass

            # 최근 활동
            print(f"\n⏰ 최근 업데이트: {stats['last_update']}")

            print("\n" + "=" * 80)
            print("종료하려면 Ctrl+C를 누르세요. 5초마다 자동 업데이트됩니다.")

            # 5초 대기
            time.sleep(5)

    except KeyboardInterrupt:
        print("\n\n대시보드를 종료합니다.")
        return
    except Exception as e:
        print(f"\n❌ 대시보드 오류: {e}")
        return

def show_session_list():
    """활성 세션 목록 표시"""
    try:
        tracker = ProgressTracker()
        sessions = tracker.get_active_sessions()

        if not sessions:
            print("활성 세션이 없습니다.")
            return

        print(f"활성 세션 목록 ({len(sessions)}개):")
        print("-" * 60)

        for session_id in sessions:
            progress = tracker.load_progress(session_id)
            if progress:
                uptime = progress.get('uptime_minutes', 0)
                processed = progress.get('total_processed', 0)
                error_rate = progress.get('error_rate', 0) * 100

                print(f"세션 ID: {session_id}")
                print(f"  업타임: {format_uptime(uptime)}")
                print(f"  처리: {processed:,}개, 에러율: {error_rate:.1f}%")
                print(f"  마지막 업데이트: {progress.get('timestamp', 'Unknown')}")
                print()

    except Exception as e:
        print(f"세션 목록 조회 실패: {e}")

def show_session_details(session_id):
    """특정 세션 상세 정보 표시"""
    try:
        tracker = ProgressTracker()
        progress = tracker.load_progress(session_id)

        if not progress:
            print(f"세션 {session_id}을(를) 찾을 수 없습니다.")
            return

        print(f"세션 상세 정보: {session_id}")
        print("=" * 60)

        for key, value in progress.items():
            print(f"{key:20s}: {value}")

    except Exception as e:
        print(f"세션 정보 조회 실패: {e}")

def cleanup_old_sessions():
    """오래된 세션 데이터 정리"""
    try:
        tracker = ProgressTracker()
        result = tracker.cleanup_old_data(days_old=2)

        if result:
            print("✅ 오래된 세션 데이터 정리 완료")
        else:
            print("❌ 데이터 정리 실패")

    except Exception as e:
        print(f"데이터 정리 실패: {e}")

def main():
    """메인 함수"""
    parser = argparse.ArgumentParser(description='크롤러 상태 모니터링 대시보드')
    parser.add_argument('command', nargs='?', default='dashboard',
                       choices=['dashboard', 'sessions', 'details', 'cleanup'],
                       help='실행할 명령')
    parser.add_argument('--session-id', help='세션 ID (details 명령용)')

    args = parser.parse_args()

    if args.command == 'dashboard':
        display_dashboard()
    elif args.command == 'sessions':
        show_session_list()
    elif args.command == 'details':
        if not args.session_id:
            print("--session-id 인자가 필요합니다.")
            sys.exit(1)
        show_session_details(args.session_id)
    elif args.command == 'cleanup':
        cleanup_old_sessions()

if __name__ == "__main__":
    main()
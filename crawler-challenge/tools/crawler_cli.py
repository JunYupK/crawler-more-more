#!/usr/bin/env python3
"""
원격 크롤링 제어 CLI
Windows 데스크톱에서 MacBook 서버의 크롤링을 제어

Usage:
    python crawler_cli.py start --workers 4 --urls 10000
    python crawler_cli.py status
    python crawler_cli.py stop
    python crawler_cli.py watch
"""

import argparse
import requests
import sys
import time
import os

# ============================================
# 설정
# ============================================

# MacBook Tailscale IP (환경변수 또는 기본값)
SERVER_URL = os.environ.get("CRAWLER_SERVER", "http://100.125.119.99:8080")


# ============================================
# 유틸리티
# ============================================
def print_colored(text: str, color: str = "white"):
    colors = {
        "red": "\033[91m",
        "green": "\033[92m",
        "yellow": "\033[93m",
        "blue": "\033[94m",
        "cyan": "\033[96m",
        "white": "\033[97m",
        "reset": "\033[0m"
    }
    print(f"{colors.get(color, '')}{text}{colors['reset']}")


def format_time(seconds: float) -> str:
    if seconds is None:
        return "N/A"
    minutes = int(seconds // 60)
    secs = int(seconds % 60)
    return f"{minutes}분 {secs}초"


# ============================================
# 명령어 구현
# ============================================
def cmd_start(args):
    """크롤링 시작"""
    print_colored("\n🚀 크롤링 시작 요청", "cyan")
    print(f"   서버: {SERVER_URL}")
    print(f"   Workers: {args.workers}")
    print(f"   URLs: {args.urls:,}")

    try:
        response = requests.post(
            f"{SERVER_URL}/crawl/start",
            json={
                "workers": args.workers,
                "url_count": args.urls
            },
            timeout=10
        )

        if response.status_code == 200:
            print_colored("\n✅ 크롤링이 시작되었습니다!", "green")
            print("   상태 확인: python crawler_cli.py status")
            print("   실시간 모니터링: python crawler_cli.py watch")
        else:
            error = response.json().get('detail', 'Unknown error')
            print_colored(f"\n❌ 에러: {error}", "red")

    except requests.exceptions.ConnectionError:
        print_colored(f"\n❌ 서버에 연결할 수 없습니다: {SERVER_URL}", "red")
        print("   - Tailscale 연결을 확인하세요")
        print("   - 서버가 실행 중인지 확인하세요")
    except Exception as e:
        print_colored(f"\n❌ 에러: {e}", "red")


def cmd_status(args):
    """크롤링 상태 조회"""
    try:
        response = requests.get(f"{SERVER_URL}/crawl/status", timeout=5)
        data = response.json()

        print("\n" + "=" * 50)
        print_colored("📊 크롤링 상태", "cyan")
        print("=" * 50)

        # 상태 아이콘
        if data["is_running"]:
            print_colored("상태: 🟢 실행 중", "green")
        else:
            print_colored("상태: 🟡 대기 중", "yellow")

        print(f"Workers: {data['workers']}")
        print(f"목표 URL: {data['target_urls']:,}")
        print(f"처리 완료: {data['processed_urls']:,}")
        print(f"성공: {data['success_count']:,}")
        print(f"실패: {data['fail_count']:,}")
        print(f"진행률: {data['progress_percent']}%")
        print(f"경과 시간: {format_time(data['elapsed_seconds'])}")

        if data['pages_per_second']:
            print(f"처리 속도: {data['pages_per_second']} pages/sec")

        print("=" * 50)

    except requests.exceptions.ConnectionError:
        print_colored(f"\n❌ 서버에 연결할 수 없습니다: {SERVER_URL}", "red")
    except Exception as e:
        print_colored(f"\n❌ 에러: {e}", "red")


def cmd_stop(args):
    """크롤링 중지"""
    print_colored("\n🛑 크롤링 중지 요청...", "yellow")

    try:
        response = requests.post(f"{SERVER_URL}/crawl/stop", timeout=10)

        if response.status_code == 200:
            data = response.json()
            print_colored("✅ 크롤링이 중지되었습니다.", "green")
            print(f"   처리: {data['summary']['processed']:,}")
            print(f"   성공: {data['summary']['success']:,}")
            print(f"   실패: {data['summary']['failed']:,}")
        else:
            error = response.json().get('detail', 'Unknown error')
            print_colored(f"❌ 에러: {error}", "red")

    except requests.exceptions.ConnectionError:
        print_colored(f"\n❌ 서버에 연결할 수 없습니다: {SERVER_URL}", "red")
    except Exception as e:
        print_colored(f"\n❌ 에러: {e}", "red")


def cmd_watch(args):
    """실시간 상태 모니터링"""
    print_colored("\n👀 실시간 모니터링 시작 (Ctrl+C로 종료)", "cyan")
    print(f"   갱신 주기: {args.interval}초\n")

    try:
        while True:
            # 화면 클리어 (Windows/Unix 호환)
            os.system('cls' if os.name == 'nt' else 'clear')

            print_colored("👀 실시간 모니터링 (Ctrl+C로 종료)\n", "cyan")

            try:
                response = requests.get(f"{SERVER_URL}/crawl/status", timeout=5)
                data = response.json()

                # 상태 표시
                status = "🟢 실행 중" if data["is_running"] else "🟡 대기 중"
                print(f"상태: {status}")
                print(f"진행률: {data['progress_percent']}% ({data['processed_urls']:,}/{data['target_urls']:,})")
                print(f"성공/실패: {data['success_count']:,} / {data['fail_count']:,}")
                print(f"경과: {format_time(data['elapsed_seconds'])}")

                if data['pages_per_second']:
                    print(f"속도: {data['pages_per_second']} pages/sec")

                # 프로그레스 바
                if data['target_urls'] > 0:
                    progress = int(data['progress_percent'] / 2)  # 50칸 기준
                    bar = "█" * progress + "░" * (50 - progress)
                    print(f"\n[{bar}] {data['progress_percent']}%")

            except requests.exceptions.ConnectionError:
                print_colored("❌ 연결 끊김 - 재시도 중...", "red")

            time.sleep(args.interval)

    except KeyboardInterrupt:
        print("\n\n모니터링 종료")


def cmd_health(args):
    """서버 헬스체크"""
    try:
        response = requests.get(f"{SERVER_URL}/health", timeout=5)
        if response.status_code == 200:
            print_colored("✅ 서버 정상", "green")
            print(f"   URL: {SERVER_URL}")
        else:
            print_colored("⚠️ 서버 응답 이상", "yellow")
    except requests.exceptions.ConnectionError:
        print_colored(f"❌ 서버 연결 실패: {SERVER_URL}", "red")


# ============================================
# 메인
# ============================================
def main():
    parser = argparse.ArgumentParser(
        description="원격 크롤링 제어 CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python crawler_cli.py health              # 서버 상태 확인
  python crawler_cli.py start -w 4 -u 10000 # 크롤링 시작
  python crawler_cli.py status              # 상태 조회
  python crawler_cli.py watch               # 실시간 모니터링
  python crawler_cli.py stop                # 크롤링 중지

Environment:
  CRAWLER_SERVER  서버 주소 (기본값: http://100.125.119.99:8080)
        """
    )

    parser.add_argument(
        "--server", "-s",
        default=SERVER_URL,
        help="서버 주소"
    )

    subparsers = parser.add_subparsers(dest="command", help="명령어")

    # health
    subparsers.add_parser("health", help="서버 헬스체크")

    # start
    start_parser = subparsers.add_parser("start", help="크롤링 시작")
    start_parser.add_argument("--workers", "-w", type=int, default=4)
    start_parser.add_argument("--urls", "-u", type=int, default=10000)

    # status
    subparsers.add_parser("status", help="상태 조회")

    # stop
    subparsers.add_parser("stop", help="크롤링 중지")

    # watch
    watch_parser = subparsers.add_parser("watch", help="실시간 모니터링")
    watch_parser.add_argument("--interval", "-i", type=int, default=3)

    args = parser.parse_args()

    # 서버 주소 업데이트
    global SERVER_URL
    SERVER_URL = args.server

    # 명령어 실행
    if args.command == "health":
        cmd_health(args)
    elif args.command == "start":
        cmd_start(args)
    elif args.command == "status":
        cmd_status(args)
    elif args.command == "stop":
        cmd_stop(args)
    elif args.command == "watch":
        cmd_watch(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()

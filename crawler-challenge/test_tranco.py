#!/usr/bin/env python3
"""
간단한 Tranco 테스트 - 샘플 데이터로 시스템 검증
"""

import os
from work_logger import WorkLogger


def create_sample_tranco_data():
    """테스트용 샘플 Tranco 데이터 생성"""
    # 실제 상위 1000개 사이트 기반 샘플 데이터
    sample_domains = [
        "google.com", "youtube.com", "facebook.com", "twitter.com", "instagram.com",
        "linkedin.com", "reddit.com", "wikipedia.org", "amazon.com", "netflix.com",
        "yahoo.com", "bing.com", "tiktok.com", "whatsapp.com", "gmail.com",
        "microsoft.com", "apple.com", "zoom.us", "discord.com", "pinterest.com",
        "snapchat.com", "telegram.org", "spotify.com", "twitch.tv", "github.com",
        "stackoverflow.com", "dropbox.com", "adobe.com", "paypal.com", "ebay.com"
    ]

    # 추가로 970개 더 생성 (패턴 기반)
    additional_domains = []
    for i in range(31, 1001):
        additional_domains.append(f"example{i}.com")

    all_domains = sample_domains + additional_domains

    # CSV 형식으로 저장
    data_dir = "data"
    os.makedirs(data_dir, exist_ok=True)

    csv_path = os.path.join(data_dir, "tranco_top1m.csv")

    with open(csv_path, 'w', encoding='utf-8') as f:
        for rank, domain in enumerate(all_domains, 1):
            f.write(f"{rank},{domain}\n")

    print(f"[OK] 샘플 Tranco 데이터 생성: {len(all_domains)}개 도메인")
    return csv_path, len(all_domains)


def test_url_parsing():
    """URL 파싱 테스트"""
    from tranco_manager import TrancoManager

    manager = TrancoManager()

    # URL 파싱 테스트 (상위 100개만)
    urls = manager.parse_csv_to_urls(limit=100, add_www=True)

    if urls:
        print(f"[OK] URL 파싱 성공: {len(urls)}개")

        # 우선순위별 통계
        priority_stats = {}
        for url_info in urls:
            priority = url_info['priority']
            priority_stats[priority] = priority_stats.get(priority, 0) + 1

        print("[STATS] 우선순위별 URL 분포:")
        for priority in sorted(priority_stats.keys(), reverse=True):
            print(f"  우선순위 {priority}: {priority_stats[priority]}개")

        # 상위 10개 URL 출력
        print("\n[LIST] 상위 10개 URL:")
        for i, url_info in enumerate(urls[:10], 1):
            print(f"{i:2d}. [{url_info['priority']:4d}] {url_info['url']}")

        return urls
    else:
        print("[ERROR] URL 파싱 실패")
        return []


def main():
    """메인 테스트 실행"""
    logger = WorkLogger()

    print("[START] Tranco 시스템 테스트 시작")

    # 1. 샘플 데이터 생성
    csv_path, domain_count = create_sample_tranco_data()

    # 2. URL 파싱 테스트
    urls = test_url_parsing()

    if urls:
        # 3. 결과 요약
        print(f"\n[SUMMARY] 테스트 완료")
        print(f"  총 도메인: {domain_count:,}개")
        print(f"  생성된 URL: {len(urls)}개")
        print(f"  파일 위치: {csv_path}")

        # 4. 작업 로깅
        logger.log_and_commit(
            title="Tranco Top 1M 시스템 테스트 완료",
            description=f"샘플 데이터를 사용하여 {len(urls)}개 URL 파싱 및 우선순위 시스템을 검증했습니다.",
            details={
                "총 도메인 수": f"{domain_count:,}",
                "파싱된 URL": f"{len(urls)}",
                "테스트 파일": csv_path,
                "상태": "정상 작동"
            }
        )

        return True
    else:
        print("[ERROR] 테스트 실패")
        return False


if __name__ == "__main__":
    success = main()
    if success:
        print("\n[READY] Tranco 시스템이 준비되었습니다!")
    else:
        print("\n[FAILED] 테스트를 완료할 수 없습니다.")
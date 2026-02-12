"""
URL Extractor - 크롤링 중 발견된 URL 정규화 및 필터링
=====================================================

발견된 링크 목록에서 유효한 URL만 추출하고 정규화
- 트래킹 파라미터 제거 (utm_*, fbclid, gclid 등)
- 프래그먼트(#anchor) 제거
- http/https 스킴만 허용
- 배치 내 중복 제거
"""

import logging
from urllib.parse import urlparse, urlunparse, urlencode, parse_qs

logger = logging.getLogger(__name__)

# 제거할 트래킹/분석 파라미터 목록
TRACKING_PARAMS = frozenset({
    # UTM 파라미터 (Google Analytics)
    'utm_source', 'utm_medium', 'utm_campaign', 'utm_term', 'utm_content',
    'utm_id', 'utm_source_platform', 'utm_creative_format', 'utm_marketing_tactic',
    # 광고 클릭 ID
    'fbclid',    # Facebook
    'gclid',     # Google Ads
    'msclkid',   # Microsoft Ads
    'twclid',    # Twitter
    'igshid',    # Instagram
    'yclid',     # Yandex
    'dclid',     # Google Display
    'ttclid',    # TikTok
    'li_fat_id', # LinkedIn
    # 이메일 마케팅
    'mc_cid', 'mc_eid',   # MailChimp
    'vero_id',             # Vero
    '_hsenc', '_hsmi',     # HubSpot
    # 레퍼러/소스 추적
    'ref', 'referrer', 'source',
    # 기타 분석 파라미터
    '_ga', '_gl',          # Google Analytics
    'pk_campaign', 'pk_kwd', 'pk_source', 'pk_medium',  # Matomo/Piwik
})


class URLExtractor:
    """
    발견된 URL 목록 정규화 및 필터링 유틸리티

    사용 예시:
        raw_links = ['https://example.com/page?utm_source=twitter#section', ...]
        clean_links = URLExtractor.filter_and_normalize(raw_links)
    """

    @staticmethod
    def normalize(url: str) -> str | None:
        """
        URL 단건 정규화

        처리 내용:
        - http/https 스킴 검증
        - netloc(도메인) 소문자 변환
        - 트래킹 파라미터 제거
        - 프래그먼트(#) 제거
        - 빈 path → '/' 로 정규화

        Returns:
            정규화된 URL 문자열, 유효하지 않으면 None
        """
        try:
            url = url.strip()
            if not url:
                return None

            parsed = urlparse(url)

            # http/https 스킴만 허용
            if parsed.scheme not in ('http', 'https'):
                return None

            # 빈 netloc 제외 (상대 URL 등)
            if not parsed.netloc:
                return None

            # 쿼리 파라미터에서 트래킹 파라미터 제거
            if parsed.query:
                params = parse_qs(parsed.query, keep_blank_values=False)
                filtered = {
                    k: v for k, v in params.items()
                    if k.lower() not in TRACKING_PARAMS
                }
                new_query = urlencode(filtered, doseq=True) if filtered else ''
            else:
                new_query = ''

            # 정규화된 URL 재조립 (프래그먼트 제거, netloc 소문자)
            normalized = urlunparse((
                parsed.scheme,
                parsed.netloc.lower(),
                parsed.path or '/',
                parsed.params,
                new_query,
                '',  # 프래그먼트 제거
            ))

            return normalized

        except Exception:
            return None

    @staticmethod
    def filter_and_normalize(urls: list[str]) -> list[str]:
        """
        URL 목록 정규화 및 중복 제거

        Args:
            urls: 원본 URL 목록 (절대 URL 기준)

        Returns:
            정규화된 유효 URL 목록 (순서 유지, 중복 제거)
        """
        seen: set[str] = set()
        result: list[str] = []

        for url in urls:
            normalized = URLExtractor.normalize(url)
            if normalized and normalized not in seen:
                seen.add(normalized)
                result.append(normalized)

        return result

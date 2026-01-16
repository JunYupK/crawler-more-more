#!/usr/bin/env python3
"""
Router Runner - 지능형 라우터 실행기
====================================

Desktop에서 실행하여 raw.page 토픽의 메시지를
분석 후 process.fast 또는 process.rich로 라우팅

Usage:
    # 기본 실행
    python runners/router_runner.py

    # Kafka 서버 지정
    python runners/router_runner.py --kafka-servers localhost:9092

    # 테스트 모드 (100개만 처리)
    python runners/router_runner.py --test --max-messages 100

    # 임계값 변경 (기본 80)
    python runners/router_runner.py --threshold 70

    # HTML 파일 직접 분석
    python runners/router_runner.py --analyze-file page.html
"""

import asyncio
import argparse
import logging
import signal
import sys
import time
from pathlib import Path
from typing import Optional

# 프로젝트 루트를 path에 추가
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.router.smart_router import SmartRouter, RoutedMessage
from src.router.page_analyzer import PageAnalyzer
from src.router.scoring import RouteDecision
from config.kafka_config import get_config

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
logger = logging.getLogger(__name__)


class RouterRunner:
    """라우터 실행기"""

    def __init__(
        self,
        kafka_servers: Optional[str] = None,
        group_id: Optional[str] = None,
        threshold: int = 80,
    ):
        self.kafka_servers = kafka_servers or get_config().kafka.bootstrap_servers
        self.group_id = group_id
        self.threshold = threshold

        self._running = False
        self._router: Optional[SmartRouter] = None

        # 종료 시그널 핸들러
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """종료 시그널 처리"""
        logger.info(f"Received signal {signum}, shutting down...")
        self._running = False
        if self._router:
            self._router._running = False

    async def run(self, max_messages: Optional[int] = None) -> None:
        """
        라우터 실행

        Args:
            max_messages: 최대 처리 메시지 수 (None이면 무한)
        """
        self._running = True
        start_time = time.time()
        routed_count = 0

        def on_routed(msg: RoutedMessage):
            nonlocal routed_count
            routed_count += 1

            if routed_count % 100 == 0:
                elapsed = time.time() - start_time
                logger.info(
                    f"[Progress] Routed: {routed_count:,} | "
                    f"Rate: {routed_count/elapsed:.1f} msg/s"
                )

        try:
            logger.info(f"Connecting to Kafka: {self.kafka_servers}")
            logger.info(f"Score threshold: {self.threshold}")

            self._router = SmartRouter(
                bootstrap_servers=self.kafka_servers,
                group_id=self.group_id,
                score_threshold=self.threshold,
            )

            async with self._router:
                await self._router.run(
                    max_messages=max_messages,
                    callback=on_routed,
                )

            # 최종 통계
            elapsed = time.time() - start_time
            logger.info("=" * 60)
            logger.info("Router stopped!")
            logger.info(f"Total routed: {routed_count:,}")
            logger.info(f"Total time: {elapsed:.1f}s")
            logger.info(f"Average rate: {routed_count/elapsed:.1f} msg/s")
            logger.info(f"Stats: {self._router.stats}")
            logger.info("=" * 60)

        except Exception as e:
            logger.error(f"Error running router: {e}", exc_info=True)

    async def test_connection(self) -> bool:
        """Kafka 연결 테스트"""
        try:
            from aiokafka import AIOKafkaConsumer
            consumer = AIOKafkaConsumer(
                bootstrap_servers=self.kafka_servers,
            )
            await consumer.start()
            await consumer.stop()
            logger.info("Kafka connection test: SUCCESS")
            return True
        except Exception as e:
            logger.error(f"Kafka connection test: FAILED - {e}")
            return False


def analyze_file(file_path: str, threshold: int = 80) -> None:
    """
    HTML 파일 직접 분석 (오프라인 테스트용)

    Args:
        file_path: HTML 파일 경로
        threshold: 점수 임계값
    """
    logger.info(f"Analyzing file: {file_path}")
    logger.info(f"Threshold: {threshold}")

    with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
        html = f.read()

    analyzer = PageAnalyzer(score_threshold=threshold)
    result = analyzer.analyze(html, file_path)

    print("\n" + "=" * 60)
    print("ANALYSIS RESULT")
    print("=" * 60)

    print(f"\n📊 Score: {result.score}/100")
    print(f"🚀 Route: {result.route.value.upper()}")
    print(f"   {'→ BeautifulSoup (fast)' if result.route == RouteDecision.FAST else '→ Crawl4AI (browser)'}")

    print(f"\n📝 Metadata:")
    print(f"   Title: {result.metadata.title or 'N/A'}")
    print(f"   Description: {(result.metadata.description or 'N/A')[:100]}...")
    print(f"   Language: {result.metadata.language or 'N/A'}")

    print(f"\n🛠️ Tech Stack:")
    print(f"   Frameworks: {', '.join(result.tech_stack.frameworks) or 'None detected'}")
    print(f"   CMS: {result.tech_stack.cms or 'None detected'}")
    print(f"   Analytics: {', '.join(result.tech_stack.analytics) or 'None detected'}")

    print(f"\n📈 Content Stats:")
    print(f"   Text length: {result.content_stats.text_length:,} chars")
    print(f"   Word count: {result.content_stats.word_count:,}")
    print(f"   Paragraphs: {result.content_stats.paragraph_count}")
    print(f"   Headings: {result.content_stats.heading_count}")
    print(f"   Scripts: {result.content_stats.script_count}")

    print(f"\n🎯 Score Breakdown:")
    for reason in result.score_result.reasons[:10]:
        print(f"   • {reason}")

    print("\n" + "=" * 60)


def analyze_html_string(html: str, threshold: int = 80) -> None:
    """
    HTML 문자열 직접 분석

    Args:
        html: HTML 콘텐츠
        threshold: 점수 임계값
    """
    analyzer = PageAnalyzer(score_threshold=threshold)
    result = analyzer.analyze(html, "inline")

    print(f"\nScore: {result.score}/100 → {result.route.value.upper()}")
    print(f"Reasons: {result.score_result.reasons[:5]}")


def demo_scoring():
    """점수 계산 데모"""
    from src.router.scoring import StaticScoreCalculator

    calculator = StaticScoreCalculator(threshold=80)

    test_cases = [
        (
            "Static Blog",
            """
            <html>
            <head><title>My Blog</title></head>
            <body>
                <article>
                    <h1>Hello World</h1>
                    <p>This is a very long paragraph with lots of content that should
                    indicate this is a static page with real content. The more text we
                    have here, the better the score should be for static classification.</p>
                    <p>Another paragraph with more content to demonstrate that this is
                    a real blog post with substantial text content.</p>
                </article>
            </body>
            </html>
            """
        ),
        (
            "React SPA",
            """
            <html>
            <head><title>React App</title></head>
            <body>
                <div id="root"></div>
                <script src="/static/js/bundle.a1b2c3.js"></script>
                <noscript>You need to enable JavaScript to run this app.</noscript>
            </body>
            </html>
            """
        ),
        (
            "Next.js SSR",
            """
            <html>
            <head>
                <title>Next.js Blog</title>
                <meta name="description" content="A blog built with Next.js featuring SSR">
            </head>
            <body>
                <div id="__next">
                    <article>
                        <h1>Server Rendered Content</h1>
                        <p>This content was rendered on the server and should be
                        visible immediately without JavaScript.</p>
                    </article>
                </div>
                <script id="__NEXT_DATA__" type="application/json">{"props":{}}</script>
            </body>
            </html>
            """
        ),
        (
            "WordPress",
            """
            <html>
            <head>
                <title>WordPress Site</title>
                <meta name="generator" content="WordPress 6.0">
            </head>
            <body>
                <div class="wp-content">
                    <article class="post">
                        <h1>WordPress Post</h1>
                        <p>This is a WordPress blog post with server-rendered content.
                        WordPress typically generates static HTML on the server.</p>
                        <p>Another paragraph with more detailed content about the topic
                        that demonstrates this is a real article.</p>
                    </article>
                </div>
            </body>
            </html>
            """
        ),
    ]

    print("\n" + "=" * 70)
    print("SCORING DEMO")
    print("=" * 70)

    for name, html in test_cases:
        result = calculator.calculate(html)
        route_symbol = "🟢" if result.route == RouteDecision.FAST else "🔴"

        print(f"\n{route_symbol} {name}")
        print(f"   Score: {result.score}/100 → {result.route.value.upper()}")
        print(f"   Top reasons: {result.reasons[:3]}")

    print("\n" + "=" * 70)


def parse_args():
    """명령줄 인자 파싱"""
    parser = argparse.ArgumentParser(
        description='Smart Router for Stream Pipeline',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    # Kafka 설정
    parser.add_argument(
        '--kafka-servers',
        type=str,
        default=None,
        help='Kafka 브로커 주소',
    )
    parser.add_argument(
        '--group-id',
        type=str,
        default=None,
        help='Consumer 그룹 ID',
    )

    # 라우팅 설정
    parser.add_argument(
        '--threshold',
        type=int,
        default=80,
        help='FAST/RICH 라우팅 임계값 (0-100)',
    )

    # 실행 모드
    parser.add_argument(
        '--max-messages',
        type=int,
        default=None,
        help='최대 처리 메시지 수',
    )
    parser.add_argument(
        '--test',
        action='store_true',
        help='테스트 모드 (기본 100개)',
    )
    parser.add_argument(
        '--test-connection',
        action='store_true',
        help='Kafka 연결만 테스트',
    )

    # 오프라인 분석
    parser.add_argument(
        '--analyze-file',
        type=str,
        help='HTML 파일 직접 분석',
    )
    parser.add_argument(
        '--demo',
        action='store_true',
        help='점수 계산 데모 실행',
    )

    # 로깅
    parser.add_argument(
        '--debug',
        action='store_true',
        help='디버그 로깅 활성화',
    )

    return parser.parse_args()


async def main():
    """메인 함수"""
    args = parse_args()

    # 디버그 로깅
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    # 데모 모드
    if args.demo:
        demo_scoring()
        return

    # 파일 분석 모드
    if args.analyze_file:
        analyze_file(args.analyze_file, args.threshold)
        return

    # Runner 생성
    runner = RouterRunner(
        kafka_servers=args.kafka_servers,
        group_id=args.group_id,
        threshold=args.threshold,
    )

    # 연결 테스트
    if args.test_connection:
        await runner.test_connection()
        return

    # 테스트 모드
    max_messages = args.max_messages
    if args.test and not max_messages:
        max_messages = 100

    # 라우터 실행
    await runner.run(max_messages=max_messages)


if __name__ == '__main__':
    asyncio.run(main())

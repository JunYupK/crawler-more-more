"""
Smart Router - Intelligent Routing Layer
=========================================

Kafka Consumer로 raw.page 토픽을 소비하고
페이지 분석 후 process.fast 또는 process.rich로 라우팅

데이터 흐름:
raw.page (압축된 HTML)
  → 분석 (PageAnalyzer)
  → 점수 계산 (StaticScoreCalculator)
  → 라우팅 (process.fast or process.rich)
"""

import asyncio
import time
import logging
from dataclasses import dataclass, field
from typing import Optional, Callable, Any

import msgpack
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.errors import KafkaError

import sys
sys.path.insert(0, '/home/user/crawler-more-more/crawler-challenge')

from src.ingestor.compression import decompress_to_str
from src.router.page_analyzer import PageAnalyzer, AnalysisResult, AnalyzerStats
from src.router.scoring import RouteDecision
from config.kafka_config import get_config, TopicConfig, ConsumerConfig

logger = logging.getLogger(__name__)


@dataclass
class RouterStats:
    """라우터 통계"""
    messages_consumed: int = 0
    messages_routed_fast: int = 0
    messages_routed_rich: int = 0
    messages_failed: int = 0
    total_processing_time_ms: float = 0.0
    start_time: float = field(default_factory=time.time)

    @property
    def total_routed(self) -> int:
        return self.messages_routed_fast + self.messages_routed_rich

    @property
    def fast_ratio(self) -> float:
        return self.messages_routed_fast / self.total_routed if self.total_routed > 0 else 0

    @property
    def messages_per_second(self) -> float:
        elapsed = time.time() - self.start_time
        return self.messages_consumed / elapsed if elapsed > 0 else 0

    @property
    def average_processing_time_ms(self) -> float:
        return self.total_processing_time_ms / self.messages_consumed if self.messages_consumed > 0 else 0

    def __str__(self) -> str:
        return (
            f"RouterStats("
            f"consumed={self.messages_consumed:,}, "
            f"fast={self.messages_routed_fast:,} ({self.fast_ratio:.1%}), "
            f"rich={self.messages_routed_rich:,}, "
            f"failed={self.messages_failed:,}, "
            f"mps={self.messages_per_second:.1f}, "
            f"avg_time={self.average_processing_time_ms:.1f}ms)"
        )


@dataclass
class RoutedMessage:
    """라우팅된 메시지"""
    url: str
    html: str                    # 압축 해제된 HTML
    score: int                   # 정적 점수
    route: RouteDecision         # 라우팅 결정
    route_reason: str            # 라우팅 이유
    metadata: dict               # 원본 메타데이터
    analysis: dict               # 분석 결과 요약
    original_timestamp: float    # 원본 크롤링 타임스탬프
    router_timestamp: float      # 라우터 처리 타임스탬프


class SmartRouter:
    """
    지능형 라우터

    raw.page 토픽에서 메시지를 소비하고
    페이지 분석 후 적절한 처리 토픽으로 라우팅

    라우팅 결정:
    - score >= 80: process.fast (BeautifulSoup 처리)
    - score < 80: process.rich (Crawl4AI 처리)
    """

    def __init__(
        self,
        bootstrap_servers: Optional[str] = None,
        group_id: Optional[str] = None,
        score_threshold: int = 80,
    ):
        """
        Args:
            bootstrap_servers: Kafka 브로커 주소
            group_id: Consumer 그룹 ID
            score_threshold: FAST/RICH 라우팅 임계값
        """
        config = get_config()

        self.bootstrap_servers = bootstrap_servers or config.kafka.bootstrap_servers
        self.group_id = group_id or f"{config.consumer.group_id_prefix}-router"
        self.topics = config.topics
        self.score_threshold = score_threshold

        # 컴포넌트
        self.analyzer = PageAnalyzer(score_threshold=score_threshold)
        self.analyzer_stats = AnalyzerStats()

        # Kafka 클라이언트
        self._consumer: Optional[AIOKafkaConsumer] = None
        self._producer: Optional[AIOKafkaProducer] = None
        self._running = False

        # 통계
        self.stats = RouterStats()

        logger.info(
            f"SmartRouter initialized: "
            f"bootstrap={self.bootstrap_servers}, "
            f"group_id={self.group_id}, "
            f"threshold={score_threshold}"
        )

    async def start(self) -> None:
        """라우터 시작"""
        if self._running:
            logger.warning("Router already running")
            return

        try:
            # Consumer 생성
            self._consumer = AIOKafkaConsumer(
                self.topics.raw_page,
                bootstrap_servers=self.bootstrap_servers,
                group_id=self.group_id,
                value_deserializer=lambda m: msgpack.unpackb(m, raw=False),
                auto_offset_reset='earliest',
                enable_auto_commit=False,
                max_poll_records=100,
                session_timeout_ms=30000,
                heartbeat_interval_ms=10000,
            )

            # Producer 생성
            self._producer = AIOKafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: msgpack.packb(v, use_bin_type=True),
                compression_type='lz4',
                linger_ms=20,
                batch_size=32768,
            )

            await self._consumer.start()
            await self._producer.start()

            self._running = True
            self.stats = RouterStats()

            logger.info("SmartRouter started successfully")

        except Exception as e:
            logger.error(f"Failed to start router: {e}")
            await self.stop()
            raise

    async def stop(self) -> None:
        """라우터 중지"""
        self._running = False

        if self._consumer:
            await self._consumer.stop()
            self._consumer = None

        if self._producer:
            await self._producer.flush()
            await self._producer.stop()
            self._producer = None

        logger.info(f"SmartRouter stopped. {self.stats}")
        logger.info(f"Analyzer stats: {self.analyzer_stats}")

    async def __aenter__(self) -> "SmartRouter":
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.stop()

    async def run(
        self,
        max_messages: Optional[int] = None,
        callback: Optional[Callable[[RoutedMessage], Any]] = None,
    ) -> None:
        """
        라우터 실행 (메인 루프)

        Args:
            max_messages: 최대 처리 메시지 수 (None이면 무한)
            callback: 라우팅 완료 시 호출될 콜백
        """
        if not self._running:
            raise RuntimeError("Router not started")

        logger.info("Starting routing loop...")
        processed = 0

        try:
            async for message in self._consumer:
                if not self._running:
                    break

                start_time = time.time()

                try:
                    # 메시지 처리
                    routed = await self._process_message(message.value)

                    if routed:
                        # 대상 토픽으로 전송
                        await self._route_message(routed)

                        # 콜백 호출
                        if callback:
                            callback(routed)

                    # 오프셋 커밋
                    await self._consumer.commit()

                    # 처리 시간 기록
                    processing_time = (time.time() - start_time) * 1000
                    self.stats.total_processing_time_ms += processing_time

                    processed += 1

                    # 진행 상황 로깅 (100개마다)
                    if processed % 100 == 0:
                        logger.info(f"Progress: {self.stats}")

                    # 최대 처리 수 확인
                    if max_messages and processed >= max_messages:
                        logger.info(f"Reached max messages: {max_messages}")
                        break

                except Exception as e:
                    logger.error(f"Error processing message: {e}", exc_info=True)
                    self.stats.messages_failed += 1

        except asyncio.CancelledError:
            logger.info("Router loop cancelled")
        except Exception as e:
            logger.error(f"Error in router loop: {e}", exc_info=True)
        finally:
            logger.info(f"Routing loop ended. Processed: {processed}")

    async def _process_message(self, message: dict) -> Optional[RoutedMessage]:
        """
        메시지 처리

        Args:
            message: raw.page 토픽에서 받은 메시지

        Returns:
            RoutedMessage 또는 None (처리 실패 시)
        """
        self.stats.messages_consumed += 1

        try:
            url = message.get('url', '')
            html_compressed = message.get('html_compressed', b'')
            original_timestamp = message.get('timestamp', 0)
            original_metadata = message.get('metadata', {})

            # HTML 압축 해제
            if isinstance(html_compressed, bytes):
                html = decompress_to_str(html_compressed)
            else:
                # 이미 문자열인 경우
                html = html_compressed

            if not html:
                logger.warning(f"Empty HTML for {url}")
                return None

            # 페이지 분석
            analysis = self.analyzer.analyze(html, url)
            self.analyzer_stats.record(analysis)

            # 라우팅 결정
            route = analysis.route
            route_reason = self.analyzer.score_calculator.get_route_reason(analysis.score_result)

            # 통계 업데이트
            if route == RouteDecision.FAST:
                self.stats.messages_routed_fast += 1
            else:
                self.stats.messages_routed_rich += 1

            return RoutedMessage(
                url=url,
                html=html,
                score=analysis.score,
                route=route,
                route_reason=route_reason,
                metadata=original_metadata,
                analysis=analysis.to_dict(),
                original_timestamp=original_timestamp,
                router_timestamp=time.time(),
            )

        except Exception as e:
            logger.error(f"Error processing message: {e}")
            return None

    async def _route_message(self, routed: RoutedMessage) -> bool:
        """
        메시지를 대상 토픽으로 라우팅

        Args:
            routed: 라우팅된 메시지

        Returns:
            성공 여부
        """
        # 대상 토픽 결정
        if routed.route == RouteDecision.FAST:
            target_topic = self.topics.process_fast
        else:
            target_topic = self.topics.process_rich

        # 메시지 구성
        message = {
            'url': routed.url,
            'html': routed.html,  # 압축 해제된 HTML
            'score': routed.score,
            'route': routed.route.value,
            'route_reason': routed.route_reason,
            'metadata': routed.metadata,
            'analysis': routed.analysis,
            'original_timestamp': routed.original_timestamp,
            'router_timestamp': routed.router_timestamp,
        }

        try:
            await self._producer.send_and_wait(
                topic=target_topic,
                value=message,
                key=routed.url.encode('utf-8'),
            )

            logger.debug(
                f"Routed to {target_topic}: {routed.url} "
                f"(score={routed.score}, route={routed.route.value})"
            )
            return True

        except KafkaError as e:
            logger.error(f"Failed to route message: {e}")
            return False

    async def process_single(self, html: str, url: str = "") -> RoutedMessage:
        """
        단일 HTML 처리 (테스트용)

        Args:
            html: HTML 콘텐츠
            url: URL

        Returns:
            RoutedMessage 객체
        """
        analysis = self.analyzer.analyze(html, url)
        route_reason = self.analyzer.score_calculator.get_route_reason(analysis.score_result)

        return RoutedMessage(
            url=url,
            html=html,
            score=analysis.score,
            route=analysis.route,
            route_reason=route_reason,
            metadata={},
            analysis=analysis.to_dict(),
            original_timestamp=0,
            router_timestamp=time.time(),
        )

    def get_stats(self) -> dict:
        """현재 통계 반환"""
        return {
            'consumed': self.stats.messages_consumed,
            'routed_fast': self.stats.messages_routed_fast,
            'routed_rich': self.stats.messages_routed_rich,
            'failed': self.stats.messages_failed,
            'fast_ratio': self.stats.fast_ratio,
            'messages_per_second': self.stats.messages_per_second,
            'average_processing_time_ms': self.stats.average_processing_time_ms,
            'analyzer': {
                'average_score': self.analyzer_stats.average_score,
                'score_distribution': self.analyzer_stats.score_distribution,
            }
        }


class RouterWorker:
    """
    라우터 워커 (멀티 인스턴스용)

    여러 워커가 같은 Consumer Group으로 병렬 처리
    """

    def __init__(
        self,
        worker_id: int,
        bootstrap_servers: Optional[str] = None,
        group_id: Optional[str] = None,
    ):
        self.worker_id = worker_id
        config = get_config()

        self.router = SmartRouter(
            bootstrap_servers=bootstrap_servers or config.kafka.bootstrap_servers,
            group_id=group_id or f"{config.consumer.group_id_prefix}-router",
        )

    async def run(self):
        """워커 실행"""
        logger.info(f"Router Worker {self.worker_id} starting...")

        async with self.router:
            await self.router.run()

        logger.info(f"Router Worker {self.worker_id} stopped")

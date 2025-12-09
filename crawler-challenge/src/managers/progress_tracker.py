import redis
import json
import time
import hashlib
import os
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

class ProgressTracker:
    """Redis를 사용한 진행 상황 추적 시스템"""

    def __init__(self, redis_host=None, redis_port=6379, redis_db=0):
        if redis_host is None:
            redis_host = os.getenv('REDIS_HOST', 'localhost')
            
        self.redis_client = redis.Redis(
            host=redis_host,
            port=redis_port,
            db=redis_db,
            decode_responses=True,
            socket_keepalive=True,
            socket_keepalive_options={},
            health_check_interval=30
        )
        self.session_id = self._generate_session_id()
        self.start_time = datetime.now()

    def _generate_session_id(self) -> str:
        """현재 시간 기반 세션 ID 생성"""
        timestamp = int(time.time())
        return hashlib.md5(f"crawler_{timestamp}".encode()).hexdigest()[:8]

    def test_connection(self) -> bool:
        """Redis 연결 테스트"""
        try:
            self.redis_client.ping()
            logger.info(f"Redis 연결 성공 (세션 ID: {self.session_id})")
            return True
        except Exception as e:
            logger.error(f"Redis 연결 실패: {e}")
            return False

    def save_progress(self, stats: Dict[str, Any]) -> bool:
        """진행 상황을 Redis에 저장"""
        try:
            progress_data = {
                'session_id': self.session_id,
                'timestamp': datetime.now().isoformat(),
                'start_time': self.start_time.isoformat(),
                'uptime_minutes': int((datetime.now() - self.start_time).total_seconds() / 60),
                **stats
            }

            # 현재 진행 상황 저장
            key = f"crawler:progress:{self.session_id}"
            self.redis_client.hset(key, mapping={
                'data': json.dumps(progress_data),
                'last_update': datetime.now().isoformat()
            })

            # TTL 설정 (48시간)
            self.redis_client.expire(key, 48 * 3600)

            # 히스토리 저장 (최근 100개)
            history_key = f"crawler:history:{self.session_id}"
            self.redis_client.lpush(history_key, json.dumps(progress_data))
            self.redis_client.ltrim(history_key, 0, 99)  # 최근 100개만 유지
            self.redis_client.expire(history_key, 48 * 3600)

            return True
        except Exception as e:
            logger.error(f"진행 상황 저장 실패: {e}")
            return False

    def load_progress(self, session_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Redis에서 진행 상황 로드"""
        try:
            target_session = session_id or self.session_id
            key = f"crawler:progress:{target_session}"

            data = self.redis_client.hget(key, 'data')
            if data:
                return json.loads(data)
            return None
        except Exception as e:
            logger.error(f"진행 상황 로드 실패: {e}")
            return None

    def get_active_sessions(self) -> List[str]:
        """활성 세션 목록 조회"""
        try:
            pattern = "crawler:progress:*"
            keys = self.redis_client.keys(pattern)
            return [key.split(":")[-1] for key in keys]
        except Exception as e:
            logger.error(f"활성 세션 조회 실패: {e}")
            return []

    def save_error(self, error_info: Dict[str, Any]) -> bool:
        """에러 정보 저장"""
        try:
            error_data = {
                'session_id': self.session_id,
                'timestamp': datetime.now().isoformat(),
                **error_info
            }

            # 에러 리스트에 추가
            error_key = f"crawler:errors:{self.session_id}"
            self.redis_client.lpush(error_key, json.dumps(error_data))
            self.redis_client.ltrim(error_key, 0, 499)  # 최근 500개 에러만 유지
            self.redis_client.expire(error_key, 48 * 3600)

            # 에러 통계 업데이트
            stats_key = f"crawler:error_stats:{self.session_id}"
            error_type = error_info.get('type', 'unknown')
            self.redis_client.hincrby(stats_key, error_type, 1)
            self.redis_client.expire(stats_key, 48 * 3600)

            return True
        except Exception as e:
            logger.error(f"에러 정보 저장 실패: {e}")
            return False

    def get_error_stats(self) -> Dict[str, int]:
        """에러 통계 조회"""
        try:
            stats_key = f"crawler:error_stats:{self.session_id}"
            return self.redis_client.hgetall(stats_key)
        except Exception as e:
            logger.error(f"에러 통계 조회 실패: {e}")
            return {}

    def save_memory_stats(self, memory_info: Dict[str, Any]) -> bool:
        """메모리 통계 저장"""
        try:
            memory_data = {
                'session_id': self.session_id,
                'timestamp': datetime.now().isoformat(),
                **memory_info
            }

            # 메모리 히스토리 저장 (최근 1440개 = 24시간, 1분마다)
            memory_key = f"crawler:memory:{self.session_id}"
            self.redis_client.lpush(memory_key, json.dumps(memory_data))
            self.redis_client.ltrim(memory_key, 0, 1439)
            self.redis_client.expire(memory_key, 48 * 3600)

            return True
        except Exception as e:
            logger.error(f"메모리 통계 저장 실패: {e}")
            return False

    def should_restart(self) -> bool:
        """재시작이 필요한지 판단"""
        try:
            # 에러율이 너무 높은 경우
            error_stats = self.get_error_stats()
            total_errors = sum(int(v) for v in error_stats.values())

            progress = self.load_progress()
            if progress:
                total_processed = progress.get('total_processed', 0)
                if total_processed > 100:  # 충분한 샘플이 있는 경우
                    error_rate = total_errors / total_processed
                    if error_rate > 0.5:  # 에러율 50% 초과
                        logger.warning(f"에러율이 높아 재시작 필요: {error_rate:.2%}")
                        return True

            # 메모리 사용량이 너무 높은 경우
            memory_key = f"crawler:memory:{self.session_id}"
            latest_memory = self.redis_client.lindex(memory_key, 0)
            if latest_memory:
                memory_data = json.loads(latest_memory)
                memory_percent = memory_data.get('memory_percent', 0)
                if memory_percent > 90:  # 메모리 사용률 90% 초과
                    logger.warning(f"메모리 사용률이 높아 재시작 필요: {memory_percent}%")
                    return True

            return False
        except Exception as e:
            logger.error(f"재시작 필요성 판단 실패: {e}")
            return False

    def mark_session_completed(self) -> bool:
        """세션 완료 표시"""
        try:
            completion_data = {
                'session_id': self.session_id,
                'completed_at': datetime.now().isoformat(),
                'start_time': self.start_time.isoformat(),
                'duration_minutes': int((datetime.now() - self.start_time).total_seconds() / 60)
            }

            completion_key = f"crawler:completed:{self.session_id}"
            self.redis_client.hset(completion_key, mapping=completion_data)
            self.redis_client.expire(completion_key, 7 * 24 * 3600)  # 7일간 보관

            return True
        except Exception as e:
            logger.error(f"세션 완료 표시 실패: {e}")
            return False

    def cleanup_old_data(self, days_old: int = 2) -> bool:
        """오래된 데이터 정리"""
        try:
            cutoff_time = datetime.now() - timedelta(days=days_old)

            # 오래된 세션 데이터 찾기 및 삭제
            for session_id in self.get_active_sessions():
                progress = self.load_progress(session_id)
                if progress:
                    session_time = datetime.fromisoformat(progress['start_time'])
                    if session_time < cutoff_time:
                        patterns = [
                            f"crawler:progress:{session_id}",
                            f"crawler:history:{session_id}",
                            f"crawler:errors:{session_id}",
                            f"crawler:error_stats:{session_id}",
                            f"crawler:memory:{session_id}",
                            f"crawler:completed:{session_id}"
                        ]

                        for pattern in patterns:
                            if self.redis_client.exists(pattern):
                                self.redis_client.delete(pattern)

                        logger.info(f"오래된 세션 데이터 삭제: {session_id}")

            return True
        except Exception as e:
            logger.error(f"오래된 데이터 정리 실패: {e}")
            return False

    def get_dashboard_stats(self) -> Dict[str, Any]:
        """대시보드용 통계 조회"""
        try:
            progress = self.load_progress() or {}
            error_stats = self.get_error_stats()

            return {
                'session_id': self.session_id,
                'status': 'running',
                'uptime_minutes': progress.get('uptime_minutes', 0),
                'total_processed': progress.get('total_processed', 0),
                'success_count': progress.get('success_count', 0),
                'error_count': sum(int(v) for v in error_stats.values()),
                'error_rate': progress.get('error_rate', 0.0),
                'pages_per_minute': progress.get('pages_per_minute', 0.0),
                'last_update': progress.get('timestamp', 'Unknown'),
                'active_sessions': len(self.get_active_sessions()),
                'error_breakdown': error_stats
            }
        except Exception as e:
            logger.error(f"대시보드 통계 조회 실패: {e}")
            return {}

    def __del__(self):
        """객체 소멸시 Redis 연결 종료"""
        try:
            if hasattr(self, 'redis_client'):
                self.redis_client.close()
        except:
            pass
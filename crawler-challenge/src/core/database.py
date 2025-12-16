import psycopg2
import psycopg2.extras
from psycopg2 import Error
from urllib.parse import urlparse
import json
from typing import List, Dict, Any
import logging

logger = logging.getLogger(__name__)

import psycopg2
import psycopg2.extras
from psycopg2 import pool
from urllib.parse import urlparse
import json
from typing import List, Dict, Any
import logging

logger = logging.getLogger(__name__)

class DatabaseManager:
    _pool = None

    def __init__(self, min_conn=1, max_conn=10, host="localhost", port="5432", 
                 database="crawler_db", user="postgres", password="postgres"):
        if not DatabaseManager._pool:
            self.connection_params = {
                'host': host,
                'port': port,
                'database': database,
                'user': user,
                'password': password
            }
            try:
                DatabaseManager._pool = pool.ThreadedConnectionPool(
                    min_conn, max_conn, **self.connection_params
                )
                logger.info(f"DB 커넥션 풀 생성 성공 (min: {min_conn}, max: {max_conn})")
            except psycopg2.Error as e:
                logger.error(f"DB 커넥션 풀 생성 실패: {e}")
                raise
        
        self.batch_size = 1000
        self.batch_buffer = []

    def get_connection(self):
        """커넥션 풀에서 커넥션 가져오기"""
        try:
            return self._pool.getconn()
        except psycopg2.Error as e:
            logger.error(f"커넥션 풀에서 커넥션 가져오기 실패: {e}")
            raise

    def release_connection(self, conn):
        """커넥션을 풀에 반환"""
        if conn:
            self._pool.putconn(conn)

    def get_pool_stats(self) -> Dict[str, int]:
        """커넥션 풀 상태 조회"""
        if self._pool:
            return {
                'pool_min': self._pool.minconn,
                'pool_max': self._pool.maxconn,
            }
        return {}

    def close_all_connections(self):
        """모든 커넥션 종료"""
        if self._pool:
            self._pool.closeall()
            DatabaseManager._pool = None
            logger.info("모든 DB 커넥션 종료")

    def extract_domain(self, url: str) -> str:
        """URL에서 도메인 추출"""
        try:
            return urlparse(url).netloc
        except:
            return "unknown"

    def extract_title_from_html(self, html_content: str) -> str:
        """HTML에서 title 태그 추출"""
        try:
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(html_content, 'html.parser')
            title_tag = soup.find('title')
            return title_tag.get_text().strip() if title_tag else ""
        except Exception as e:
            logger.warning(f"Title 추출 실패: {e}")
            return ""

    def create_metadata(self, html_content: str, url: str) -> Dict[str, Any]:
        """메타데이터 생성"""
        metadata = {
            'content_length': len(html_content),
            'url_path': urlparse(url).path,
            'has_title': bool(self.extract_title_from_html(html_content)),
        }

        # 추가 메타데이터 (선택적)
        try:
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(html_content, 'html.parser')

            # 메타 태그 수집
            meta_description = soup.find('meta', attrs={'name': 'description'})
            if meta_description:
                metadata['description'] = meta_description.get('content', '')[:500]

            # 링크 수
            metadata['link_count'] = len(soup.find_all('a'))

            # 이미지 수
            metadata['image_count'] = len(soup.find_all('img'))

        except Exception as e:
            logger.warning(f"메타데이터 생성 중 오류: {e}")

        return metadata

    def add_to_batch(self, url: str, html_content: str):
        """배치에 데이터 추가"""
        # 이미 배치에 있는 URL인지 확인 (중복 방지)
        if any(item['url'] == url for item in self.batch_buffer):
            logger.warning(f"Duplicate URL in batch, skipping: {url}")
            return

        domain = self.extract_domain(url)
        title = self.extract_title_from_html(html_content)
        metadata = self.create_metadata(html_content, url)

        # 텍스트 추출 (BeautifulSoup 사용)
        try:
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(html_content, 'html.parser')
            # 스크립트와 스타일 태그 제거
            for script in soup(["script", "style"]):
                script.decompose()
            content_text = soup.get_text()
            # 공백 정리
            content_text = ' '.join(content_text.split())
        except Exception as e:
            logger.warning(f"텍스트 추출 실패: {e}")
            content_text = ""

        if title:
            title = title.replace('\x00', '')
        if content_text:
            content_text = content_text.replace('\x00', '')
        # metadata는 dict이므로 JSON 직렬화 후 NUL 바이트 제거
        if metadata:
            metadata = {k: (v.replace('\x00', '') if isinstance(v, str) else v) for k, v in metadata.items()}

        self.batch_buffer.append({
            'url': url,
            'domain': domain,
            'title': title,
            'content_text': content_text,
            'metadata': json.dumps(metadata)
        })

        # 배치 크기에 도달하면 자동으로 플러시
        if len(self.batch_buffer) >= self.batch_size:
            self.flush_batch()

    def flush_batch(self):
        """배치 데이터를 DB에 저장 (실패 시 DLQ로 격리)"""
        if not self.batch_buffer:
            return

        # 배치 데이터 복사 (DLQ 저장용)
        batch_data_copy = list(self.batch_buffer)
        conn = self.get_connection()

        try:
            with conn.cursor() as cursor:
                insert_query = """
                    INSERT INTO crawled_pages (url, domain, title, content_text, metadata)
                    VALUES %s
                    ON CONFLICT (url) DO UPDATE SET
                        title = EXCLUDED.title,
                        content_text = EXCLUDED.content_text,
                        metadata = EXCLUDED.metadata,
                        updated_at = CURRENT_TIMESTAMP
                """
                values = [(
                    item['url'], item['domain'], item['title'],
                    item['content_text'], item['metadata']
                ) for item in self.batch_buffer]

                psycopg2.extras.execute_values(cursor, insert_query, values, page_size=100)
                conn.commit()
                logger.info(f"{len(self.batch_buffer)}개 레코드 배치 저장 완료")
        except psycopg2.Error as e:
            logger.error(f"배치 저장 실패, DLQ로 격리 저장 시도: {e}")
            conn.rollback()
            # DLQ로 실패 데이터 격리 저장
            self.save_to_dlq(batch_data_copy, e)
        except Exception as e:
            logger.error(f"예상치 못한 에러 발생, DLQ로 격리 저장 시도: {e}")
            conn.rollback()
            self.save_to_dlq(batch_data_copy, e)
        finally:
            # 성공하든 실패하든 버퍼는 비워야 무한 루프를 방지함
            self.batch_buffer.clear()
            self.release_connection(conn)

    def save_to_dlq(self, batch_data: List[Dict], error: Exception):
        """
        실패한 배치 데이터를 DLQ(Dead Letter Queue) 테이블에 저장

        Args:
            batch_data: 실패한 배치 데이터 리스트
            error: 발생한 예외 객체
        """
        error_type = type(error).__name__
        error_message = str(error)[:1000]  # 에러 메시지 길이 제한

        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                for item in batch_data:
                    try:
                        # raw_data를 JSON으로 직렬화 (직렬화 오류 방지를 위해 default=str 사용)
                        raw_data_json = json.dumps(item, default=str, ensure_ascii=False)
                        # NUL 바이트 제거
                        raw_data_json = raw_data_json.replace('\x00', '')

                        url = item.get('url', 'unknown')
                        if url:
                            url = url.replace('\x00', '')

                        cursor.execute("""
                            INSERT INTO crawler_dlq (url, error_message, error_type, raw_data)
                            VALUES (%s, %s, %s, %s)
                        """, (url, error_message, error_type, raw_data_json))
                    except Exception as item_error:
                        # 개별 아이템 저장 실패 시에도 계속 진행
                        logger.warning(f"DLQ 개별 아이템 저장 실패: {item_error}")
                        continue

                conn.commit()
                logger.warning(f"DLQ에 {len(batch_data)}개 레코드 격리 저장 완료 (error_type: {error_type})")
        except Exception as dlq_error:
            # DLQ 저장도 실패하면 로그만 남기고 무시 (시스템 멈춤 방지)
            logger.critical(f"DLQ 저장 실패! 데이터 유실 발생: {dlq_error}")
            logger.critical(f"유실된 URL 목록: {[item.get('url', 'unknown') for item in batch_data[:10]]}...")
            if conn:
                try:
                    conn.rollback()
                except:
                    pass
        finally:
            if conn:
                self.release_connection(conn)

    def get_crawled_count(self) -> int:
        """크롤링된 페이지 수 조회"""
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) FROM crawled_pages")
                return cursor.fetchone()[0]
        except psycopg2.Error as e:
            logger.error(f"카운트 조회 실패: {e}")
            return 0
        finally:
            self.release_connection(conn)

    def get_domain_stats(self) -> List[Dict]:
        """도메인별 통계 조회"""
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT domain, COUNT(*) as count,
                           AVG(LENGTH(content_text)) as avg_content_length
                    FROM crawled_pages
                    GROUP BY domain
                    ORDER BY count DESC
                """)
                results = cursor.fetchall()
                return [
                    {'domain': row[0], 'count': row[1], 'avg_content_length': float(row[2]) if row[2] else 0}
                    for row in results
                ]
        except psycopg2.Error as e:
            logger.error(f"도메인 통계 조회 실패: {e}")
            return []
        finally:
            self.release_connection(conn)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # 남은 배치 처리
        if self.batch_buffer:
            self.flush_batch()
        self.close_all_connections()
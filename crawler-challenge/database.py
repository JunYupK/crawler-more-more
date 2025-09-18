import psycopg2
import psycopg2.extras
from psycopg2 import Error
from urllib.parse import urlparse
import json
from typing import List, Dict, Any
import logging

logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self, host="localhost", port="5432", database="crawler_db",
                 user="postgres", password="postgres"):
        self.connection_params = {
            'host': host,
            'port': port,
            'database': database,
            'user': user,
            'password': password
        }
        self.connection = None
        self.batch_size = 1000
        self.batch_buffer = []

    def connect(self):
        """데이터베이스 연결"""
        try:
            self.connection = psycopg2.connect(**self.connection_params)
            self.connection.autocommit = False
            logger.info("PostgreSQL 연결 성공")
        except Error as e:
            logger.error(f"PostgreSQL 연결 실패: {e}")
            raise

    def disconnect(self):
        """데이터베이스 연결 해제"""
        if self.connection:
            # 남은 배치 처리
            if self.batch_buffer:
                self.flush_batch()
            self.connection.close()
            self.connection = None
            logger.info("PostgreSQL 연결 해제")

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
        """배치 데이터를 DB에 저장"""
        if not self.batch_buffer:
            return

        if not self.connection:
            self.connect()

        try:
            cursor = self.connection.cursor()

            # execute_values를 사용한 배치 삽입
            insert_query = """
                INSERT INTO crawled_pages (url, domain, title, content_text, metadata)
                VALUES %s
                ON CONFLICT (url) DO UPDATE SET
                    title = EXCLUDED.title,
                    content_text = EXCLUDED.content_text,
                    metadata = EXCLUDED.metadata,
                    updated_at = CURRENT_TIMESTAMP
            """

            # 데이터 튜플로 변환
            values = [(
                item['url'],
                item['domain'],
                item['title'],
                item['content_text'],
                item['metadata']
            ) for item in self.batch_buffer]

            psycopg2.extras.execute_values(
                cursor, insert_query, values, template=None, page_size=100
            )

            self.connection.commit()
            logger.info(f"{len(self.batch_buffer)}개 레코드 배치 저장 완료")

            # 배치 버퍼 초기화
            self.batch_buffer.clear()

        except Error as e:
            logger.error(f"배치 저장 실패: {e}")
            self.connection.rollback()
            raise
        finally:
            cursor.close()

    def get_crawled_count(self) -> int:
        """크롤링된 페이지 수 조회"""
        if not self.connection:
            self.connect()

        try:
            cursor = self.connection.cursor()
            cursor.execute("SELECT COUNT(*) FROM crawled_pages")
            count = cursor.fetchone()[0]
            return count
        except Error as e:
            logger.error(f"카운트 조회 실패: {e}")
            return 0
        finally:
            cursor.close()

    def get_domain_stats(self) -> List[Dict]:
        """도메인별 통계 조회"""
        if not self.connection:
            self.connect()

        try:
            cursor = self.connection.cursor()
            cursor.execute("""
                SELECT domain, COUNT(*) as count,
                       AVG(LENGTH(content_text)) as avg_content_length
                FROM crawled_pages
                GROUP BY domain
                ORDER BY count DESC
            """)
            results = cursor.fetchall()
            return [
                {
                    'domain': row[0],
                    'count': row[1],
                    'avg_content_length': float(row[2]) if row[2] else 0
                }
                for row in results
            ]
        except Error as e:
            logger.error(f"도메인 통계 조회 실패: {e}")
            return []
        finally:
            cursor.close()

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()
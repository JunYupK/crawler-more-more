import psycopg2
from psycopg2 import Error
import sys

def test_postgres_connection():
    """PostgreSQL 연결 테스트"""
    try:
        # PostgreSQL 연결
        connection = psycopg2.connect(
            host="localhost",
            database="crawler_db",
            user="postgres",
            password="postgres",
            port="5432"
        )

        # 커서 생성
        cursor = connection.cursor()

        # PostgreSQL 버전 확인
        cursor.execute("SELECT version();")
        record = cursor.fetchone()
        print(f"PostgreSQL 서버 버전: {record[0]}")

        # 테이블 존재 확인
        cursor.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
        """)
        tables = cursor.fetchall()
        print(f"생성된 테이블: {[table[0] for table in tables]}")

        # crawled_pages 테이블 구조 확인
        cursor.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = 'crawled_pages'
            ORDER BY ordinal_position
        """)
        columns = cursor.fetchall()
        print(f"crawled_pages 테이블 구조:")
        for column in columns:
            print(f"  - {column[0]}: {column[1]}")

        # 인덱스 확인
        cursor.execute("""
            SELECT indexname, tablename
            FROM pg_indexes
            WHERE tablename = 'crawled_pages'
        """)
        indexes = cursor.fetchall()
        print(f"crawled_pages 인덱스:")
        for index in indexes:
            print(f"  - {index[0]}")

        print("\n✅ PostgreSQL 연결 성공!")

    except Error as e:
        print(f"❌ PostgreSQL 연결 오류: {e}")
        sys.exit(1)

    finally:
        if connection:
            cursor.close()
            connection.close()
            print("PostgreSQL 연결이 닫혔습니다.")

if __name__ == "__main__":
    test_postgres_connection()
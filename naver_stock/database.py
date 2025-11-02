"""
Database module for Naver Stock Crawler
Handles PostgreSQL connection and data insertion (Optimized for Batch)
"""

import os
import psycopg2
from psycopg2.extras import Json, execute_batch
from dotenv import load_dotenv
import logging

# Load environment variables
load_dotenv()

# Configure logging
# (main.py에서 이미 로깅을 설정하므로, 여기서는 기본 설정만 가져올 수 있습니다.)
logger = logging.getLogger(__name__)


class Database:
    """Database handler for stock discussion data"""

    def __init__(self):
        """Initialize database connection"""
        self.connection = None
        self.cursor = None
        self.connect()

    def connect(self):
        """Establish connection to PostgreSQL database"""
        try:
            self.connection = psycopg2.connect(
                host=os.getenv('DB_HOST', 'localhost'),
                port=os.getenv('DB_PORT', '5432'),
                database=os.getenv('DB_NAME', 'naver_stock'),
                user=os.getenv('DB_USER', 'postgres'),
                password=os.getenv('DB_PASSWORD', '')
            )
            # autocommit을 끕니다 (기본값). commit()을 수동으로 호출해야 합니다.
            self.connection.autocommit = False 
            self.cursor = self.connection.cursor()
            logger.info("Successfully connected to PostgreSQL database")
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise

    def create_table(self):
        """Create table if not exists (using schema.sql)"""
        try:
            with open('schema.sql', 'r', encoding='utf-8') as f:
                schema_sql = f.read()
            self.cursor.execute(schema_sql)
            self.connection.commit() # 테이블 생성은 즉시 커밋
            logger.info("Database schema created/verified successfully")
        except Exception as e:
            logger.error(f"Failed to create table: {e}")
            self.connection.rollback()
            raise

    def insert_batch(self, discussions):
        """
        Insert multiple discussions at once using execute_batch for high performance.
        DOES NOT COMMIT.

        Args:
            discussions (list): List of discussion dictionaries

        Returns:
            int: Number of successfully inserted/updated records
        """
        if not discussions:
            return 0

        # 1. SQL 쿼리 (ON CONFLICT 포함)
        sql = """
            INSERT INTO naver_stock
            (stock_code, stock_name, comment_id, author_name, date, content, likes_count, dislikes_count, comment_data)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (stock_code, comment_id)
            DO UPDATE SET
                stock_name = EXCLUDED.stock_name,
                author_name = EXCLUDED.author_name,
                date = EXCLUDED.date,
                content = EXCLUDED.content,
                likes_count = EXCLUDED.likes_count,
                dislikes_count = EXCLUDED.dislikes_count,
                comment_data = EXCLUDED.comment_data
        """

        # 2. execute_batch에 맞게 데이터를 [List of Tuples]로 변환
        data_to_insert = []
        for d in discussions:
            data_to_insert.append((
                d['stock_code'],
                d.get('stock_name'), # .get()으로 None 방지
                d['comment_id'],
                d['author_name'],
                d.get('date'),
                d['content'],
                d['likes_count'],
                d['dislikes_count'],
                Json(d['comment_data'])  # dict/list를 Json 객체로 래핑
            ))

        # 3. execute_batch로 '진짜' 배치 실행
        try:
            # 단 한 번의 네트워크 요청으로 모든 데이터를 전송
            execute_batch(self.cursor, sql, data_to_insert)
            logger.info(f"Batch executed for {len(data_to_insert)} records.")
            
            # 🚨 여기서 COMMIT 하지 않습니다!
            # main.py에서 모든 작업이 끝난 후 한 번만 commit()을 호출할 것입니다.
            
            return len(data_to_insert)
        
        except Exception as e:
            logger.error(f"Failed in execute_batch: {e}")
            # 롤백도 main.py에서 관리합니다.
            return 0

    def commit(self):
        """Commit the current transaction"""
        try:
            self.connection.commit()
            logger.info("Database commit successful")
        except Exception as e:
            logger.error(f"Database commit failed: {e}")
            # 커밋 실패 시 롤백 시도
            self.rollback()

    def rollback(self):
        """Roll back the current transaction"""
        try:
            self.connection.rollback()
            logger.warning("Database rollback initiated")
        except Exception as e:
            logger.error(f"Database rollback failed: {e}")

    def close(self):
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        logger.info("Database connection closed")

    def __enter__(self):
        """Context manager entry"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        # 컨텍스트 매니저가 종료될 때
        # 예외가 발생했다면 롤백, 아니면 커밋
        if exc_type:
            logger.error(f"Exception occurred, rolling back: {exc_val}")
            self.rollback()
        else:
            logger.info("Context manager exiting, committing.")
            self.commit()
        
        self.close()


if __name__ == "__main__":
    # Test database connection
    try:
        # 'with' 구문 테스트 (자동 commit/close 테스트)
        with Database() as db:
            db.create_table()
            logger.info("Database test successful")
            
        # 수동 commit/close 테스트 (main.py가 사용할 방식)
        db_manual = Database()
        db_manual.create_table()
        # db_manual.insert_batch(...) # (테스트 데이터)
        db_manual.commit()
        db_manual.close()
        logger.info("Manual database test successful")
        
    except Exception as e:
        logger.error(f"Database test failed: {e}")

"""
Main script for Naver Stock Discussion Crawler (PC Version)
Reads target stock codes and crawls discussion data
Uses requests + BeautifulSoup for better performance
"""

import os
import sys
import time
import logging
import asyncio
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from concurrent.futures import ProcessPoolExecutor, as_completed
from crawler_pc import NaverStockCrawlerPC
from database import Database

# Load environment variables
load_dotenv()

# logs 디렉터리 생성 (로그 파일을 사용하지 않더라도 경로 참조 오류 방지)
os.makedirs('logs', exist_ok=True)

# Generate timestamp-based log filename
log_timestamp = datetime.now().strftime('%Y%m%d-%H%M%S')
log_filename = f'logs/crawler_{log_timestamp}.log'

# Configure logging
# (파일 저장은 주석 처리됨, 콘솔로만 출력)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        # logging.FileHandler(log_filename, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


def read_target_stocks(filename='Market Data_top10.csv'):
    """
    Read stock codes from CSV file

    Args:
        filename: Path to CSV file

    Returns:
        list: List of stock codes
    """
    stock_codes = []

    try:
        df = pd.read_csv(filename, dtype={'stock_code': str})
        # 'isu_cd' 컬럼에서 종목 코드를 읽어옵니다.
        stock_codes = df['isu_cd'].tolist()
        logger.info(f"Loaded {len(stock_codes)} stock codes from {filename}")

    except FileNotFoundError:
        logger.error(f"Target file not found: {filename}")
        sys.exit(1)
        
    except KeyError:
        logger.error(f"Column 'isu_cd' not found in {filename}.")
        logger.error("Please check the CSV file header.")
        sys.exit(1)

    except Exception as e:
        logger.error(f"Error reading target file: {e}")
        sys.exit(1)

    return stock_codes


def process_single_stock(stock_code, max_posts, max_concurrent=50):
    """
    Process a single stock code - for use in parallel processing
    (This function runs in a separate process)
    Uses ASYNC crawler for high performance
    """
    start_time = time.time()

    try:
        crawler = NaverStockCrawlerPC()

        # Run async crawler in this process's event loop
        discussions = asyncio.run(
            crawler.crawl_stock_discussions_async(
                stock_code,
                max_posts=max_posts,
                max_concurrent=max_concurrent
            )
        )

        elapsed = time.time() - start_time
        return (stock_code, discussions, elapsed)

    except Exception as e:
        # 자식 프로세스에서 발생한 에러 로깅
        logger_sp = logging.getLogger(__name__)
        logger_sp.error(f"Error in child process for stock {stock_code}: {e}")
        elapsed = time.time() - start_time
        return (stock_code, None, elapsed)


def main():
    """Main execution function"""

    print('=' * 80)
    print('Naver Stock Discussion Crawler (PC Version)')
    print('=' * 80)
    print(f'Started at: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    # print(f'Log file: {log_filename}') # 로그 파일 저장 안 함
    print()

    logger.info("=" * 60)
    logger.info("Naver Stock Discussion Crawler Started")
    # logger.info(f"Log file: {log_filename}")
    logger.info("=" * 60)

    # Configuration
    max_workers = int(os.getenv('MAX_THREADS', 10))
    max_posts = int(os.getenv('MAX_POSTS', 50))
    stock_processes = int(os.getenv('STOCK_PROCESSES', 2))

    logger.info(f"Configuration:")
    logger.info(f"  - Max Workers (detail crawling): {max_workers}")
    logger.info(f"  - Max Posts per Stock: {max_posts}")
    logger.info(f"  - Stock Processes (parallel stocks): {stock_processes}")

    # Read target stocks
    stock_codes = read_target_stocks('Market Data_top10.csv')

    if not stock_codes:
        logger.error("No stock codes found in CSV file")
        sys.exit(1)

    print(f"Target stocks: {len(stock_codes)} stocks")
    print(f"Posts per stock: {max_posts}")
    print(f"Workers: {max_workers}")
    print(f"Stock processes: {stock_processes}")
    print()

    db = None # finally에서 db 객체를 참조할 수 있도록 외부에 선언
    error_occurred = False # 에러 발생 여부 플래그
    
    # Initialize database
    try:
        db = Database()
        db.create_table()
        logger.info("Database initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")
        logger.error("Please check your .env file and database connection")
        sys.exit(1)

    # Process each stock
    total_crawled = 0
    total_saved = 0
    start_time = time.time()

    print('=' * 80)
    print('Starting crawl process...')
    print('=' * 80)
    print()

    try:
        # Use ProcessPoolExecutor for parallel stock processing
        with ProcessPoolExecutor(max_workers=stock_processes) as executor:
            # Submit all stocks for processing
            future_to_stock = {
                executor.submit(process_single_stock, stock_code, max_posts, max_workers): stock_code
                for stock_code in stock_codes
            }

            # Process results as they complete
            completed = 0
            for future in as_completed(future_to_stock):
                stock_code = future_to_stock[future]
                completed += 1

                try:
                    stock_code, discussions, stock_elapsed = future.result()

                    print(f'\n[{completed}/{len(stock_codes)}] Processing stock: {stock_code}')
                    print('-' * 80)

                    logger.info("-" * 60)
                    logger.info(f"Processing stock {completed}/{len(stock_codes)}: {stock_code}")
                    logger.info("-" * 60)

                    if discussions:
                        # Save to database (using efficient execute_batch)
                        saved_count = db.insert_batch(discussions)

                        total_crawled += len(discussions)
                        total_saved += saved_count

                        logger.info(f"Stock {stock_code}: Crawled {len(discussions)}, Saved {saved_count} in {stock_elapsed:.2f}s")

                        print(f'  ✅ Crawled: {len(discussions)} posts')
                        print(f'  ✅ Saved: {saved_count} posts')
                        print(f'  ⏱️  Time: {stock_elapsed:.2f}s')
                    else:
                        logger.warning(f"No discussions found for stock {stock_code}")
                        print(f'  ⚠️  No posts found')

                except Exception as e:
                    error_occurred = True # 에러 플래그 설정
                    logger.error(f"Error processing result for stock {stock_code}: {e}")
                    print(f'  ❌ Error: {e}')

    except KeyboardInterrupt:
        error_occurred = True # 중단도 에러로 간주 (커밋 방지)
        logger.info("\nCrawling interrupted by user")
        print('\n\nProcess interrupted by user. Cleaning up...')
        

    except Exception as e:
        error_occurred = True # 에러 플래그 설정
        logger.error(f"Unexpected error during crawling: {e}")
        print(f'  ❌ Error: {e}')

    finally:
        total_elapsed = time.time() - start_time
        
        if db:
            if error_occurred:
                logger.warning("Errors occurred. Rolling back changes...")
                db.rollback() # ⭐️ 1. 에러가 났으면 롤백
            else:
                logger.info("Crawl complete. Committing all changes...")
                db.commit() # ⭐️ 2. 에러가 없으면 최종 커밋
            
            # ⭐️ 3. 커밋/롤백 후 연결 종료
            db.close()

        # Summary
        print()
        print('=' * 80)
        print('Crawl completed!' if not error_occurred else 'Crawl finished with errors!')
        print('=' * 80)
        print(f'Total stocks processed: {completed}/{len(stock_codes)}')
        print(f'Total posts crawled: {total_crawled}')
        print(f'Total posts saved (uncommitted if errors): {total_saved}')
        print(f'Total time: {total_elapsed:.2f}s ({total_elapsed/60:.1f} minutes)')
        if total_crawled > 0:
            print(f'Average speed: {total_elapsed/total_crawled:.2f}s per post')
        print(f'Finished at: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
        print('=' * 80)

        logger.info("=" * 60)
        logger.info("Naver Crawling Summary")
        logger.info("=" * 60)
        logger.info(f"Total Stocks Processed: {completed}/{len(stock_codes)}")
        logger.info(f"Total Discussions Crawled: {total_crawled}")
        logger.info(f"Total Discussions Saved: {total_saved}")
        if error_occurred:
            logger.warning("Changes were ROLLED BACK due to errors.")
        else:
            logger.info("Changes were COMMITTED successfully.")
        logger.info(f"Total Time: {total_elapsed:.2f}s")
        logger.info("=" * 60)
        logger.info("Naver Crawler finished")


if __name__ == "__main__":
    main()

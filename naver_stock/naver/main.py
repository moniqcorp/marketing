"""
Main script for Naver Stock Discussion Crawler (PC Version )
Reads target stock codes and crawls discussion data
Uses requests + BeautifulSoup for better performance
"""

import os
import sys
import time
import logging
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from concurrent.futures import ProcessPoolExecutor, as_completed
from crawler_pc import NaverStockCrawlerPC
from database import Database

# Load environment variables
load_dotenv()

# Create logs directory if not exists
# os.makedirs('logs', exist_ok=True)

# Generate timestamp-based log filename
log_timestamp = datetime.now().strftime('%Y%m%d-%H%M%S')
log_filename = f'logs/crawler_{log_timestamp}.log'

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        # logging.FileHandler(log_filename, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


def read_target_stocks(filename='Moniq Company Info.csv'):
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
        stock_codes = df['stock_code'].tolist()
        logger.info(f"Loaded {len(stock_codes)} stock codes from {filename}")

    except FileNotFoundError:
        logger.error(f"Target file not found: {filename}")
        sys.exit(1)

    except Exception as e:
        logger.error(f"Error reading target file: {e}")
        sys.exit(1)

    return stock_codes


def process_single_stock(stock_code, max_posts, max_workers):
    """
    Process a single stock code - for use in parallel processing

    Args:
        stock_code: Stock code to process
        max_posts: Maximum posts to crawl
        max_workers: Number of workers for detail crawling

    Returns:
        tuple: (stock_code, discussions, elapsed_time)
    """
    start_time = time.time()

    try:
        crawler = NaverStockCrawlerPC()
        discussions = crawler.crawl_stock_discussions(
            stock_code,
            max_posts=max_posts,
            max_workers=max_workers
        )
        elapsed = time.time() - start_time
        return (stock_code, discussions, elapsed)

    except Exception as e:
        logger = logging.getLogger(__name__)
        logger.error(f"Error processing stock {stock_code}: {e}")
        elapsed = time.time() - start_time
        return (stock_code, None, elapsed)


def main():
    """Main execution function"""

    print('=' * 80)
    print('Naver Stock Discussion Crawler (PC Version)')
    print('=' * 80)
    print(f'Started at: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    print(f'Log file: {log_filename}')
    print()

    logger.info("=" * 60)
    logger.info("Naver Stock Discussion Crawler Started")
    logger.info(f"Log file: {log_filename}")
    logger.info("=" * 60)

    # Configuration
    max_workers = int(os.getenv('MAX_THREADS', 10))
    max_posts = int(os.getenv('MAX_POSTS', 50))
    stock_processes = int(os.getenv('STOCK_PROCESSES', 2))

    logger.info(f"Configuration:")
    logger.info(f"  - Max Workers: {max_workers}")
    logger.info(f"  - Max Posts per Stock: {max_posts}")
    logger.info(f"  - Stock Processes: {stock_processes}")

    # Read target stocks
    stock_codes = read_target_stocks('Moniq Company Info.csv')

    if not stock_codes:
        logger.error("No stock codes found in CSV file")
        sys.exit(1)

    print(f"Target stocks: {len(stock_codes)} stocks")
    print(f"Posts per stock: {max_posts}")
    print(f"Workers: {max_workers}")
    print(f"Stock processes: {stock_processes}")
    print()

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
                        # Save to database
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
                    logger.error(f"Error processing stock {stock_code}: {e}")
                    print(f'  ❌ Error: {e}')

    except KeyboardInterrupt:
        logger.info("\nCrawling interrupted by user")
        print('\n\nProcess interrupted by user. Cleaning up...')

    except Exception as e:
        logger.error(f"Unexpected error during crawling: {e}")
        print(f'  ❌ Error: {e}')

    finally:
        # Close database connection
        db.close()

        # Summary
        total_elapsed = time.time() - start_time

        print()
        print('=' * 80)
        print('Crawl completed!')
        print('=' * 80)
        print(f'Total stocks processed: {len(stock_codes)}')
        print(f'Total posts crawled: {total_crawled}')
        print(f'Total posts saved: {total_saved}')
        print(f'Total time: {total_elapsed:.2f}s ({total_elapsed/60:.1f} minutes)')
        if total_crawled > 0:
            print(f'Average speed: {total_elapsed/total_crawled:.2f}s per post')
        print(f'Finished at: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
        print('=' * 80)

        logger.info("=" * 60)
        logger.info("Naver Crawling Summary")
        logger.info("=" * 60)
        logger.info(f"Total Stocks Processed: {len(stock_codes)}")
        logger.info(f"Total Discussions Crawled: {total_crawled}")
        logger.info(f"Total Discussions Saved: {total_saved}")
        logger.info(f"Total Time: {total_elapsed:.2f}s")
        logger.info("=" * 60)
        logger.info("Naver Crawler finished")


if __name__ == "__main__":
    main()

"""
Main script for Naver Stock Discussion Crawler (GCS Version)
- Pure async architecture (no ProcessPoolExecutor)
- Date-based filtering
- Streaming save to GCS in Parquet format with Hive partitioning (dt=YYYY-MM-DD)
- Memory-efficient batch processing
- Supports CSV and BigQuery stock sources
"""

import os
import sys
import time
import json
import logging
import asyncio
import aiohttp
from datetime import datetime
from dotenv import load_dotenv
from crawler_pc import NaverStockCrawlerPC
from gcs_writer import GCSParquetWriter, DatePartitionedBuffer
from bigquery_utils import load_stocks

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)


async def collect_metadata(stock_codes, start_date, end_date):
    """
    Phase 1: Collect metadata (NID + date) for all stocks

    Args:
        stock_codes: List of stock codes
        start_date: Start date (datetime)
        end_date: End date (datetime)

    Returns:
        dict: {(stock_code, date_key): [nid_list]}
    """
    logger.info("=" * 60)
    logger.info("PHASE 1: Collecting metadata (NIDs with dates)")
    logger.info("=" * 60)

    batch_map = {}  # {(stock_code, date_key): [nid_list]}
    crawler = NaverStockCrawlerPC()

    # Create aiohttp session
    connector = aiohttp.TCPConnector(
        limit=150,
        limit_per_host=100,
        ttl_dns_cache=300,
        enable_cleanup_closed=True,
        force_close=False,
        keepalive_timeout=60
    )
    timeout = aiohttp.ClientTimeout(total=None, connect=30, sock_read=30)

    async with aiohttp.ClientSession(
        connector=connector,
        timeout=timeout,
        headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'},
        cookies={'hide_cleanbot_contents': 'off'}
    ) as session:

        for idx, stock_code in enumerate(stock_codes, 1):
            logger.info(f"[{idx}/{len(stock_codes)}] Collecting metadata for stock {stock_code}")

            try:
                # Get NIDs with dates (unlimited pages, stops at start_date)
                nid_date_pairs = await crawler.get_nids_with_dates_async(
                    session, stock_code, start_date, end_date
                )

                # Group by (stock_code, date)
                for nid, post_date in nid_date_pairs:
                    date_key = post_date.strftime('%Y%m%d')
                    key = (stock_code, date_key)

                    if key not in batch_map:
                        batch_map[key] = []
                    batch_map[key].append(nid)

                logger.info(f"  Found {len(nid_date_pairs)} posts in date range")

            except Exception as e:
                logger.error(f"Error collecting metadata for stock {stock_code}: {e}")

    # Summary
    total_nids = sum(len(nids) for nids in batch_map.values())
    unique_dates = len(set(key[1] for key in batch_map.keys()))

    logger.info("=" * 60)
    logger.info(f"Phase 1 Complete:")
    logger.info(f"  Total posts: {total_nids}")
    logger.info(f"  Unique dates: {unique_dates}")
    logger.info(f"  Stock-Date pairs: {len(batch_map)}")
    logger.info("=" * 60)

    return batch_map


async def crawl_and_save(batch_map, gcs_writer, stock_name_map, max_concurrent=20):
    """
    Phase 2: Crawl details by date and save to GCS

    Args:
        batch_map: {(stock_code, date_key): [nid_list]}
        gcs_writer: GCSParquetWriter instance
        stock_name_map: {stock_code: stock_name} mapping
        max_concurrent: Max concurrent requests

    Returns:
        dict: Statistics
    """
    logger.info("=" * 60)
    logger.info("PHASE 2: Crawling details and streaming to GCS")
    logger.info("=" * 60)

    # Reorganize by date
    date_groups = {}  # {date_key: [(stock_code, [nids])]}
    for (stock_code, date_key), nids in batch_map.items():
        if date_key not in date_groups:
            date_groups[date_key] = []
        date_groups[date_key].append((stock_code, nids))

    logger.info(f"Processing {len(date_groups)} unique dates")

    # Statistics
    stats = {
        'total_posts': 0,
        'saved_files': [],
        'errors': 0
    }

    crawler = NaverStockCrawlerPC()

    # Create aiohttp session
    connector = aiohttp.TCPConnector(
        limit=150,
        limit_per_host=100,
        ttl_dns_cache=300,
        enable_cleanup_closed=True,
        force_close=False,
        keepalive_timeout=60
    )
    timeout = aiohttp.ClientTimeout(total=None, connect=30, sock_read=30)

    async with aiohttp.ClientSession(
        connector=connector,
        timeout=timeout,
        headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'},
        cookies={'hide_cleanbot_contents': 'off'}
    ) as session:

        # Process each date
        for date_idx, (date_key, stock_nid_list) in enumerate(sorted(date_groups.items()), 1):
            logger.info(f"\n[Date {date_idx}/{len(date_groups)}] Processing {date_key}")

            # Create buffer for this date
            buffer = DatePartitionedBuffer(
                gcs_writer,
                buffer_size=1000,
                source='naver'
            )

            # Collect all NIDs for this date
            all_tasks = []
            for stock_code, nids in stock_nid_list:
                for nid in nids:
                    all_tasks.append((stock_code, nid))

            logger.info(f"  Total posts to crawl: {len(all_tasks)}")

            # Create semaphore for concurrency control
            semaphore = asyncio.Semaphore(max_concurrent)

            async def fetch_with_semaphore(stock_code, nid):
                """Fetch single post with semaphore"""
                async with semaphore:
                    try:
                        # Get stock name from mapping
                        stock_name = stock_name_map.get(stock_code)
                        post = await crawler.get_discussion_detail_async(
                            session, stock_code, nid, stock_name=stock_name
                        )
                        if post:
                            # Convert comment_data to JSON string
                            if 'comment_data' in post:
                                post['comment_data'] = json.dumps(post['comment_data'], ensure_ascii=False)
                            return post
                        return None
                    except Exception as e:
                        logger.error(f"Error fetching nid={nid}: {e}")
                        stats['errors'] += 1
                        return None

            # Fetch all posts concurrently
            results = await asyncio.gather(*[
                fetch_with_semaphore(stock_code, nid)
                for stock_code, nid in all_tasks
            ])

            # Add to buffer and save
            # Use each post's actual date for partitioning (not the search date_key)
            for post in results:
                if post:
                    # Extract date from post and use it for partitioning
                    post_date_str = post.get('date')  # "2025-11-08 20:10:23"
                    if post_date_str:
                        # Extract just the date part for partitioning
                        post_date_only = post_date_str.split()[0] if ' ' in post_date_str else post_date_str
                        # Convert to YYYYMMDD format for buffer
                        post_date_key = post_date_only.replace('-', '')
                        buffer.add(post_date_key, post)
                    else:
                        # Fallback to search date if post date is missing
                        logger.warning(f"Post missing date field, using search date: {date_key}")
                        buffer.add(date_key, post)
                    stats['total_posts'] += 1

            # Flush remaining buffer
            saved = buffer.flush()
            stats['saved_files'].extend(saved)

            logger.info(f"  ✅ Saved {len(results)} posts to {len(saved)} files")

    logger.info("=" * 60)
    logger.info("Phase 2 Complete")
    logger.info("=" * 60)

    return stats


async def main():
    """Main execution function"""

    print('=' * 80)
    print('Naver Stock Discussion Crawler (GCS Version)')
    print('=' * 80)
    print(f'Started at: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    print()

    # Parse command line arguments
    # Usage: python main.py [start_date] [end_date] [stock_code1,stock_code2,...]
    target_stock_codes = None  # None = load from source, otherwise use provided codes

    if len(sys.argv) >= 3:
        # Command line arguments: python main.py 2025-01-01 2025-01-07 [stock_codes]
        start_date_str = sys.argv[1]
        end_date_str = sys.argv[2]

        # Check if stock codes are provided
        if len(sys.argv) >= 4:
            stock_codes_arg = sys.argv[3]
            target_stock_codes = [code.strip() for code in stock_codes_arg.split(',')]
            logger.info(f"Using specific stock codes: {target_stock_codes}")

        logger.info(f"Using date range from arguments: {start_date_str} ~ {end_date_str}")
    else:
        # Default: Last 3 days (today - 2 days to today)
        from datetime import timedelta
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        start_date = today - timedelta(days=2)
        end_date = today
        start_date_str = start_date.strftime("%Y-%m-%d")
        end_date_str = end_date.strftime("%Y-%m-%d")
        logger.info(f"Using default date range (last 3 days): {start_date_str} ~ {end_date_str}")

    try:
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
    except ValueError as e:
        logger.error(f"Invalid date format: {e}")
        logger.error("Usage: python main.py [start_date] [end_date]")
        logger.error("Example: python main.py 2025-01-01 2025-01-07")
        sys.exit(1)

    print(f"\n✅ Date range: {start_date.strftime('%Y-%m-%d')} ~ {end_date.strftime('%Y-%m-%d')}")

    # GCS configuration
    bucket_name = os.getenv('GCS_BUCKET_NAME')
    credentials_path = os.getenv('GCS_CREDENTIALS_PATH')
    gcs_prefix = os.getenv('GCS_PREFIX')

    if not bucket_name:
        logger.error("GCS_BUCKET_NAME not set in .env file")
        sys.exit(1)

    print(f"☁️  GCS Bucket: {bucket_name}")
    print()

    # Load target stocks
    stock_name_map = {}  # {stock_code: stock_name}

    if target_stock_codes:
        # Use command-line provided stock codes
        stock_codes = target_stock_codes
        logger.info(f"Using {len(stock_codes)} stock codes from command line")

        # Try to load names from source if available
        stock_source = os.getenv('STOCK_SOURCE', 'csv')
        try:
            if stock_source == 'bigquery':
                _, full_name_map = load_stocks(
                    source='bigquery',
                    dataset_id=os.getenv('BQ_DATASET_ID'),
                    table_id=os.getenv('BQ_STOCK_TABLE_ID'),
                    project_id=os.getenv('GCP_PROJECT_ID'),
                    credentials_path=credentials_path,
                    code_column=os.getenv('BQ_STOCK_CODE_COLUMN', 'stock_code'),
                    name_column=os.getenv('BQ_STOCK_NAME_COLUMN', 'stock_name')
                )
                # Filter name map to only target codes
                stock_name_map = {code: full_name_map.get(code, '') for code in stock_codes}
            else:
                csv_filename = os.getenv('STOCK_CSV_FILE', 'Market Data_top10.csv')
                _, full_name_map = load_stocks(
                    source='csv',
                    filename=csv_filename,
                    code_column='isu_cd',
                    name_column='isu_nm'
                )
                stock_name_map = {code: full_name_map.get(code, '') for code in stock_codes}
        except Exception as e:
            logger.warning(f"Could not load stock names: {e}")
            stock_name_map = {code: '' for code in stock_codes}
    else:
        # Load from configured source
        stock_source = os.getenv('STOCK_SOURCE', 'csv')

        if stock_source == 'bigquery':
            logger.info("Loading stocks from BigQuery...")
            stock_codes, stock_name_map = load_stocks(
                source='bigquery',
                dataset_id=os.getenv('BQ_DATASET_ID'),
                table_id=os.getenv('BQ_STOCK_TABLE_ID'),
                project_id=os.getenv('GCP_PROJECT_ID'),
                credentials_path=credentials_path,
                code_column=os.getenv('BQ_STOCK_CODE_COLUMN', 'stock_code'),
                name_column=os.getenv('BQ_STOCK_NAME_COLUMN', 'stock_name'),
                filters='target_stock = 1',  # Only load stocks with target_stock = 1
                limit=int(os.getenv('BQ_LIMIT', 0)) or None  # 0 means no limit
            )
        else:
            # Load from CSV (default)
            csv_filename = os.getenv('STOCK_CSV_FILE', 'Market Data_top10.csv')
            logger.info(f"Loading stocks from CSV: {csv_filename}")
            stock_codes, stock_name_map = load_stocks(
                source='csv',
                filename=csv_filename,
                code_column='isu_cd',
                name_column='isu_nm'
            )

    if not stock_codes:
        logger.error("No stock codes found")
        sys.exit(1)

    logger.info(f"Loaded {len(stock_codes)} stocks with names")
    print(f"📊 Target stocks: {len(stock_codes)}")
    print()

    # Initialize GCS writer
    try:
        gcs_writer = GCSParquetWriter(bucket_name, credentials_path,gcs_prefix)
        logger.info("GCS writer initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize GCS writer: {e}")
        sys.exit(1)

    # Start processing
    # Note: Old files will be deleted per stock when saving new data
    start_time = time.time()

    try:
        # Phase 1: Collect metadata
        batch_map = await collect_metadata(stock_codes, start_date, end_date)

        if not batch_map:
            logger.warning("No posts found in date range")
            sys.exit(0)

        # Phase 2: Crawl and save
        stats = await crawl_and_save(batch_map, gcs_writer, stock_name_map, max_concurrent=20)

        # Summary
        total_elapsed = time.time() - start_time

        print()
        print('=' * 80)
        print('✅ Crawl completed successfully!')
        print('=' * 80)
        print(f'Total posts crawled: {stats["total_posts"]}')
        print(f'Total files saved: {len(stats["saved_files"])}')
        print(f'Errors: {stats["errors"]}')
        print(f'Total time: {total_elapsed:.2f}s ({total_elapsed/60:.1f} minutes)')
        if stats['total_posts'] > 0:
            print(f'Average speed: {total_elapsed/stats["total_posts"]:.3f}s per post')
        print(f'Finished at: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
        print('=' * 80)
        print()
        print('📂 Saved files:')
        for file_path in stats['saved_files'][:10]:  # Show first 10
            print(f'  - {file_path}')
        if len(stats['saved_files']) > 10:
            print(f'  ... and {len(stats["saved_files"]) - 10} more files')
        print('=' * 80)

    except KeyboardInterrupt:
        logger.info("\nCrawling interrupted by user")
        print('\n\nProcess interrupted by user.')
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())

"""
Main script for Naver Stock Discussion Crawler (Refactored Version)
- Uses modular architecture with reusable components
- Pure async architecture
- Date-based filtering
- Streaming save to GCS in Parquet format
"""

import sys
import time
import asyncio
from datetime import datetime, timedelta

from config import settings
from utils import setup_logging
from core.gcs import GCSParquetWriter, DatePartitionedBuffer
from core.database import load_stocks
from crawlers.naver import NaverStockCrawler
from pipelines import NaverCrawlPipeline

# Setup logging
logger = setup_logging()


async def main():
    """Main execution function"""

    print('=' * 80)
    print('Naver Stock Discussion Crawler (Refactored Version)')
    print('=' * 80)
    print(f'Started at: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    print()

    # Validate configuration
    try:
        settings.validate()
    except ValueError as e:
        logger.error(f"Configuration validation failed:\n{e}")
        sys.exit(1)

    # Parse command line arguments
    target_stock_codes = None

    if len(sys.argv) >= 3:
        start_date_str = sys.argv[1]
        end_date_str = sys.argv[2]

        if len(sys.argv) >= 4:
            stock_codes_arg = sys.argv[3]
            target_stock_codes = [code.strip() for code in stock_codes_arg.split(',')]
            logger.info(f"Using specific stock codes: {target_stock_codes}")

        logger.info(f"Using date range from arguments: {start_date_str} ~ {end_date_str}")
    else:
        # Default: Last 3 days
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
        logger.error("Usage: python main.py [start_date] [end_date] [stock_codes]")
        logger.error("Example: python main.py 2025-01-01 2025-01-07 005930,000660")
        sys.exit(1)

    print(f"\n✅ Date range: {start_date.strftime('%Y-%m-%d')} ~ {end_date.strftime('%Y-%m-%d')}")
    print(f"☁️  GCS Bucket: {settings.GCS_BUCKET_NAME}")
    print()

    # Load target stocks
    stock_name_map = {}
    stock_isin_map = {}

    if target_stock_codes:
        stock_codes = target_stock_codes
        logger.info(f"Using {len(stock_codes)} stock codes from command line")

        # Try to load names and ISIN codes
        try:
            config = settings.get_stock_loader_config()
            _, full_name_map, full_isin_map = load_stocks(**config)
            stock_name_map = {code: full_name_map.get(code, '') for code in stock_codes}
            stock_isin_map = {code: full_isin_map.get(code, '') for code in stock_codes}
        except Exception as e:
            logger.warning(f"Could not load stock names/ISIN: {e}")
            stock_name_map = {code: '' for code in stock_codes}
            stock_isin_map = {code: '' for code in stock_codes}
    else:
        # Load from configured source
        logger.info(f"Loading stocks from {settings.STOCK_SOURCE}...")
        config = settings.get_stock_loader_config()
        stock_codes, stock_name_map, stock_isin_map = load_stocks(**config)

    if not stock_codes:
        logger.error("No stock codes found")
        sys.exit(1)

    logger.info(f"Loaded {len(stock_codes)} stocks with names")
    print(f"📊 Target stocks: {len(stock_codes)}")
    print()

    # Initialize GCS writer
    try:
        gcs_writer = GCSParquetWriter(
            settings.GCS_BUCKET_NAME,
            settings.GCS_CREDENTIALS_PATH,
            settings.GCS_PREFIX
        )
        logger.info("GCS writer initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize GCS writer: {e}")
        sys.exit(1)

    # Initialize crawler
    crawler = NaverStockCrawler(
        request_delay=settings.REQUEST_DELAY,
        max_retries=settings.MAX_RETRIES
    )
    logger.info("Naver crawler initialized")

    # Initialize pipeline
    pipeline = NaverCrawlPipeline(crawler, gcs_writer, stock_name_map, stock_isin_map)
    logger.info("Pipeline initialized")

    # Start processing
    start_time = time.time()

    try:
        # Phase 1: Collect metadata
        batch_map = await pipeline.collect_metadata(stock_codes, start_date, end_date)

        if not batch_map:
            logger.warning("No posts found in date range")
            sys.exit(0)

        # Phase 2: Crawl and save
        buffer = DatePartitionedBuffer(
            gcs_writer,
            buffer_size=1000,
            source='naver'
        )

        stats = await pipeline.crawl_and_save(
            batch_map,
            buffer,
            max_concurrent=settings.MAX_CONCURRENT_REQUESTS
        )

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
        for file_path in stats['saved_files'][:10]:
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

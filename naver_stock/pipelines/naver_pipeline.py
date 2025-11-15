"""
Naver Stock Discussion Crawling Pipeline
- Handles metadata collection, crawling, and GCS storage
"""

import logging
import asyncio
import aiohttp
import json
from datetime import datetime

logger = logging.getLogger(__name__)


class NaverCrawlPipeline:
    """Pipeline for Naver stock discussion crawling workflow"""

    def __init__(self, crawler, gcs_writer, stock_name_map, stock_isin_map):
        """
        Initialize pipeline

        Args:
            crawler: NaverStockCrawler instance
            gcs_writer: GCSParquetWriter instance
            stock_name_map: {stock_code: stock_name} mapping
            stock_isin_map: {stock_code: isin_code} mapping
        """
        self.crawler = crawler
        self.gcs_writer = gcs_writer
        self.stock_name_map = stock_name_map
        self.stock_isin_map = stock_isin_map

    async def collect_metadata(self, stock_codes, start_date, end_date):
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

        # Create aiohttp session
        connector = aiohttp.TCPConnector(
            limit=30,
            limit_per_host=20,
            ttl_dns_cache=300,
            enable_cleanup_closed=True,
            force_close=False,
            keepalive_timeout=30
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
                    # Get NIDs with dates
                    nid_date_pairs = await self.crawler.get_nids_with_dates_async(
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

    async def crawl_and_save(self, batch_map, buffer, max_concurrent=20):
        """
        Phase 2: Crawl details by date and save to GCS

        Args:
            batch_map: {(stock_code, date_key): [nid_list]}
            buffer: DatePartitionedBuffer instance
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

        # Create aiohttp session
        connector = aiohttp.TCPConnector(
            limit=30,
            limit_per_host=20,
            ttl_dns_cache=300,
            enable_cleanup_closed=True,
            force_close=False,
            keepalive_timeout=30
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
                            await asyncio.sleep(0.05)

                            stock_name = self.stock_name_map.get(stock_code, '')
                            isin_code = self.stock_isin_map.get(stock_code, '')
                            post = await self.crawler.get_discussion_detail_async(
                                session, stock_code, nid, stock_name=stock_name, isin_code=isin_code
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
                for post in results:
                    if post:
                        # Extract date from post
                        post_date_str = post.get('date')
                        if post_date_str:
                            post_date_only = post_date_str.split()[0] if ' ' in post_date_str else post_date_str
                            post_date_key = post_date_only.replace('-', '')
                            buffer.add(post_date_key, post)
                        else:
                            logger.warning(f"Post missing date field, using search date: {date_key}")
                            buffer.add(date_key, post)
                        stats['total_posts'] += 1

                # Flush remaining buffer for this date
                saved = buffer.flush()
                logger.info(f"  ✅ Saved {len(results)} posts to {len(saved)} files")

            # Get all saved files from buffer
            stats['saved_files'] = buffer.get_all_saved_files()

        logger.info("=" * 60)
        logger.info("Phase 2 Complete")
        logger.info("=" * 60)

        return stats

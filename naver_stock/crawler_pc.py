"""
Naver Stock Discussion Crawler for PC Version
- Uses requests + BeautifulSoup (no Selenium/Playwright needed)
- Hybrid approach: PC HTML for list + Mobile JSON for detail
- Much faster and lighter than v1
"""

import os
import time
import json
import logging
import re
import asyncio
import aiohttp
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup
import requests
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class NaverStockCrawlerPC:
    """Crawler for Naver Stock discussions (PC version)"""

    def __init__(self):
        self.base_url = "https://finance.naver.com"
        self.mobile_url = "https://m.stock.naver.com"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        # Disable cleanbot to see all posts
        self.session.cookies.set('hide_cleanbot_contents', 'off', domain='.naver.com')
        self.request_delay = float(os.getenv('REQUEST_DELAY', '0.3'))
        self.max_retries = int(os.getenv('MAX_RETRIES', '3'))

    def reset_session(self):
        """Reset session to clear any connection issues"""
        self.session.close()
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        # Disable cleanbot to see all posts
        self.session.cookies.set('hide_cleanbot_contents', 'off', domain='.naver.com')
        logger.info("Session reset")

    def get_discussion_list(self, stock_code, max_pages=5):
        """
        Get discussion list from PC version

        Args:
            stock_code: Stock code (e.g., '005930')
            max_pages: Maximum number of pages to crawl

        Returns:
            tuple: (nid_list, stock_name)
        """
        nid_list = []
        stock_name = None

        logger.info(f"🔓 Cleanbot disabled - fetching all posts including cleanbot-filtered ones")

        for page in range(1, max_pages + 1):
            try:
                url = f"{self.base_url}/item/board.naver?code={stock_code}&page={page}"
                logger.info(f"Fetching list page {page}: {url}")

                response = self.session.get(url)
                response.raise_for_status()
                soup = BeautifulSoup(response.text, 'html.parser')

                # Extract stock name (only on first page)
                if page == 1:
                    stock_name_elem = soup.select_one('.wrap_company h2 a')
                    if stock_name_elem:
                        stock_name = stock_name_elem.text.strip()
                        logger.info(f"Stock name: {stock_name}")

                # Parse discussion table
                table = soup.select_one('table.type2')
                if not table:
                    logger.warning(f"Table not found on page {page}")
                    break

                rows = table.select('tbody tr')
                page_nids = 0

                for row in rows:
                    # Skip blank rows
                    if row.get('class') and 'blank_row' in row.get('class'):
                        continue

                    # Skip cleanbot UI row (not an actual post)
                    if 'u_cbox_cleanbot' in str(row):
                        logger.debug("Skipping cleanbot UI row")
                        continue

                    cells = row.select('td')
                    if len(cells) < 6:
                        continue

                    # Extract title link
                    title_link = cells[1].select_one('a')
                    if not title_link:
                        continue

                    href = title_link.get('href', '')

                    # Extract nid using regex
                    nid_match = re.search(r'nid=(\d+)', href)
                    if not nid_match:
                        continue

                    nid = nid_match.group(1)

                    if nid not in nid_list:
                        nid_list.append(nid)
                        page_nids += 1

                logger.info(f"Page {page}: Found {page_nids} discussions")

                # Add delay between requests
                if page < max_pages:
                    time.sleep(self.request_delay)

            except Exception as e:
                logger.error(f"Error fetching page {page}: {e}")
                break

        logger.info(f"Total discussions found: {len(nid_list)}")
        return nid_list, stock_name

    async def get_discussion_list_async(self, session, stock_code, max_pages=10):
        """
        Async version of get_discussion_list - fetches all pages concurrently

        Args:
            session: aiohttp ClientSession
            stock_code: Stock code (e.g., '005930')
            max_pages: Maximum number of pages to crawl

        Returns:
            tuple: (nid_list, stock_name)
        """
        logger.info(f"🔓 [ASYNC] Cleanbot disabled - fetching all posts including cleanbot-filtered ones")

        async def fetch_page(page_num):
            """Fetch a single page and return parsed data"""
            try:
                url = f"{self.base_url}/item/board.naver?code={stock_code}&page={page_num}"
                logger.debug(f"[ASYNC] Fetching list page {page_num}: {url}")

                async with session.get(url) as response:
                    if response.status != 200:
                        logger.warning(f"HTTP {response.status} for page {page_num}")
                        return [], None, page_num

                    html = await response.text()
                    soup = BeautifulSoup(html, 'html.parser')

                    # Extract stock name (from page 1)
                    stock_name = None
                    if page_num == 1:
                        stock_name_elem = soup.select_one('.wrap_company h2 a')
                        if stock_name_elem:
                            stock_name = stock_name_elem.text.strip()

                    # Parse discussion table
                    table = soup.select_one('table.type2')
                    if not table:
                        logger.warning(f"Table not found on page {page_num}")
                        return [], stock_name, page_num

                    rows = table.select('tbody tr')
                    page_nids = []

                    for row in rows:
                        # Skip blank rows
                        if row.get('class') and 'blank_row' in row.get('class'):
                            continue

                        # Skip cleanbot UI row
                        if 'u_cbox_cleanbot' in str(row):
                            continue

                        cells = row.select('td')
                        if len(cells) < 6:
                            continue

                        # Extract title link
                        title_link = cells[1].select_one('a')
                        if not title_link:
                            continue

                        href = title_link.get('href', '')

                        # Extract nid using regex
                        nid_match = re.search(r'nid=(\d+)', href)
                        if not nid_match:
                            continue

                        nid = nid_match.group(1)
                        page_nids.append(nid)

                    return page_nids, stock_name, page_num

            except Exception as e:
                logger.error(f"Error fetching page {page_num}: {e}")
                return [], None, page_num

        # Fetch all pages concurrently
        tasks = [fetch_page(page) for page in range(1, max_pages + 1)]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Combine results
        nid_list = []
        stock_name = None

        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Task failed: {result}")
                continue

            page_nids, page_stock_name, page_num = result

            # Get stock name from page 1
            if page_num == 1 and page_stock_name:
                stock_name = page_stock_name
                logger.info(f"Stock name: {stock_name}")

            # Add unique nids
            for nid in page_nids:
                if nid not in nid_list:
                    nid_list.append(nid)

            if page_nids:
                logger.info(f"Page {page_num}: Found {len(page_nids)} discussions")

        logger.info(f"[ASYNC] Total discussions found: {len(nid_list)}")
        return nid_list, stock_name

    async def get_nids_with_dates_async(self, session, stock_code, start_date=None, end_date=None, max_pages=50):
        """
        Get NIDs with their dates from list pages (for date-based filtering)

        Args:
            session: aiohttp ClientSession
            stock_code: Stock code
            start_date: Start date (datetime object)
            end_date: End date (datetime object)
            max_pages: Maximum pages to crawl

        Returns:
            list: [(nid, datetime), (nid, datetime), ...]
        """
        logger.info(f"[ASYNC] Collecting NIDs with dates for stock {stock_code}")
        if start_date:
            logger.info(f"  Start date: {start_date.strftime('%Y-%m-%d')}")
        if end_date:
            logger.info(f"  End date: {end_date.strftime('%Y-%m-%d')}")

        async def fetch_page(page_num):
            """Fetch a single page and return NID-date pairs"""
            try:
                url = f"{self.base_url}/item/board.naver?code={stock_code}&page={page_num}"

                async with session.get(url) as response:
                    if response.status != 200:
                        logger.warning(f"HTTP {response.status} for page {page_num}")
                        return []

                    html = await response.text()
                    soup = BeautifulSoup(html, 'html.parser')

                    # Parse discussion table
                    table = soup.select_one('table.type2')
                    if not table:
                        return []

                    rows = table.select('tbody tr')
                    nid_date_pairs = []
                    should_stop = False

                    for row in rows:
                        # Skip blank rows and cleanbot UI
                        if row.get('class') and 'blank_row' in row.get('class'):
                            continue
                        if 'u_cbox_cleanbot' in str(row):
                            continue

                        cells = row.select('td')
                        if len(cells) < 6:
                            continue

                        # Extract NID
                        title_link = cells[1].select_one('a')
                        if not title_link:
                            continue

                        href = title_link.get('href', '')
                        nid_match = re.search(r'nid=(\d+)', href)
                        if not nid_match:
                            continue

                        nid = nid_match.group(1)

                        # Extract date from first column (cells[0])
                        date_text = cells[0].get_text(strip=True)
                        post_date = self.parse_list_date(date_text)

                        if not post_date:
                            logger.warning(f"Could not parse date: {date_text}")
                            continue

                        # Date filtering
                        if start_date and post_date < start_date:
                            should_stop = True  # Older than start_date, stop pagination
                            break

                        if end_date and post_date > end_date:
                            continue  # Newer than end_date, skip this post

                        nid_date_pairs.append((nid, post_date))

                    logger.debug(f"Page {page_num}: Found {len(nid_date_pairs)} posts in date range")
                    return nid_date_pairs, should_stop

            except Exception as e:
                logger.error(f"Error fetching page {page_num}: {e}")
                return [], False

        # Fetch pages sequentially (not concurrently) to enable early stopping
        all_nid_date_pairs = []

        for page in range(1, max_pages + 1):
            result = await fetch_page(page)

            if isinstance(result, tuple):
                nid_date_pairs, should_stop = result
                all_nid_date_pairs.extend(nid_date_pairs)

                if should_stop:
                    logger.info(f"Stopping at page {page}: posts older than start_date")
                    break
            else:
                # Old behavior compatibility
                all_nid_date_pairs.extend(result)

        logger.info(f"[ASYNC] Collected {len(all_nid_date_pairs)} NIDs with dates for stock {stock_code}")
        return all_nid_date_pairs

    def get_discussion_detail(self, stock_code, nid, stock_name=None, retry_count=0):
        """
        Get discussion detail from mobile iframe + PC page for comments

        Args:
            stock_code: Stock code
            nid: Discussion ID
            stock_name: Stock name (optional)
            retry_count: Current retry attempt (internal use)

        Returns:
            dict: Discussion data
        """
        mobile_url = f"{self.mobile_url}/pc/domestic/stock/{stock_code}/discussion/{nid}"
        pc_url = f"{self.base_url}/item/board_read.naver?code={stock_code}&nid={nid}"

        try:
            # Step 1: Get mobile iframe for content
            logger.debug(f"Fetching detail from: {mobile_url}")

            response = self.session.get(mobile_url)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')

            # Extract __NEXT_DATA__
            script = soup.find('script', {'id': '__NEXT_DATA__'})
            if not script:
                logger.error(f"__NEXT_DATA__ not found for nid={nid}")
                return None

            data = json.loads(script.string)
            queries = data.get('props', {}).get('pageProps', {}).get('dehydratedState', {}).get('queries', [])

            # Extract discussion detail
            discussion_data = None
            for query in queries:
                query_key = query.get('queryKey', [{}])
                if len(query_key) > 0 and query_key[0].get('url') == '/discussion/detail':
                    discussion_data = query.get('state', {}).get('data', {}).get('result', {})
                    break

            if not discussion_data:
                logger.error(f"Discussion data not found for nid={nid}")
                return None

            # Parse discussion data
            title = discussion_data.get('title') or discussion_data.get('subject', '')
            writer = discussion_data.get('writer', {})
            author_name = writer.get('nickname', '')

            # Parse content HTML to text
            content_html = discussion_data.get('contentHtml', '')
            content_text = ''

            if content_html:
                # Normal post with HTML content
                content_soup = BeautifulSoup(content_html, 'html.parser')
                content_text = content_soup.get_text(separator='\n', strip=True)
            else:
                # AI summary post or other special format
                # Try contentJsonSwReplaced
                content_json = discussion_data.get('contentJsonSwReplaced', '')
                if content_json:
                    try:
                        import json as json_module
                        content_data = json_module.loads(content_json)
                        # Extract summary HTML
                        summary_html = content_data.get('contentSummary', '')
                        if summary_html:
                            summary_soup = BeautifulSoup(summary_html, 'html.parser')
                            content_text = summary_soup.get_text(separator='\n', strip=True)
                    except:
                        # If JSON parsing fails, just use the raw string
                        content_text = content_json

            # Combine title and content
            full_content = f"{title}\n\n{content_text}" if title else content_text

            likes_count = discussion_data.get('recommendCount', 0)
            dislikes_count = discussion_data.get('notRecommendCount', 0)

            # Parse date (writtenAt format: ISO 8601)
            written_at = discussion_data.get('writtenAt', '')
            date = self.parse_date(written_at)

            # Step 2: Get comments using Naver comment API
            comments = []
            try:
                comments = self.get_comments_via_api(nid, stock_code)
                logger.debug(f"Fetched {len(comments)} comments for nid={nid}")
            except Exception as comment_error:
                logger.warning(f"Failed to fetch comments for nid={nid}: {comment_error}")

            logger.info(f"✅ Crawled nid={nid}: {author_name} ({len(comments)} comments)")

            return {
                'stock_code': stock_code,
                'stock_name': stock_name,
                'comment_id': int(nid),
                'author_name': author_name,
                'date': date,
                'content': full_content,
                'likes_count': likes_count,
                'dislikes_count': dislikes_count,
                'comment_data': comments
            }

        except Exception as e:
            if retry_count < self.max_retries:
                logger.warning(f"Error crawling nid={nid} (attempt {retry_count + 1}/{self.max_retries}): {e}")
                logger.info(f"Inaccessible URL: {mobile_url}")
                logger.info(f"Retrying nid={nid} after session reset...")

                # Reset session before retry
                self.reset_session()
                time.sleep(1)  # Wait before retry

                # Retry with incremented count
                return self.get_discussion_detail(stock_code, nid, stock_name, retry_count + 1)
            else:
                logger.error(f"Failed to crawl nid={nid} after {self.max_retries} retries: {e}")
                logger.info(f"Permanently inaccessible URL: {mobile_url}")
                return None

    def get_comments_via_api(self, nid, stock_code, page=1, page_size=100):
        """
        Get comments using Naver comment API (no Selenium needed!)

        Args:
            nid: Discussion ID
            stock_code: Stock code (for Referer header)
            page: Page number (default: 1)
            page_size: Number of comments per page (default: 100)

        Returns:
            list: List of comment dictionaries
        """
        url = "https://apis.naver.com/commentBox/cbox/web_naver_list_jsonp.json"

        params = {
            'ticket': 'finance',
            'templateId': 'community',
            'pool': 'cbox12',
            'lang': 'ko',
            'country': 'KR',
            'objectId': str(nid),
            'categoryId': '',
            'pageSize': page_size,
            'indexSize': 10,
            'groupId': '',
            'listType': 'OBJECT',
            'pageType': 'more',
            'page': page,
            'initialize': 'true',
            'followSize': 5,
            'useAltSort': 'true',
            'replyPageSize': 5,
            '_callback': 'jQuery',
            '_': str(int(time.time() * 1000))
        }

        # Add mobile User-Agent and Referer for comment API
        headers = {
            'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 13_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.0.4 Mobile/15E148 Safari/604.1',
            'Referer': f'https://m.stock.naver.com/domestic/stock/{stock_code}/discussion/{nid}'
        }

        response = self.session.get(url, params=params, headers=headers)

        if response.status_code == 200:
            # Remove JSONP wrapper
            jsonp_text = response.text
            json_text = re.sub(r'^[^(]*\(', '', jsonp_text)
            json_text = re.sub(r'\);?\s*$', '', json_text)

            data = json.loads(json_text)

            if data.get('success'):
                comment_list = data.get('result', {}).get('commentList', [])
                comments = []

                for idx, comment in enumerate(comment_list, 1):
                    # Parse comment date
                    reg_time = comment.get('regTime', '')
                    comment_date = self.parse_date(reg_time)

                    comments.append({
                        'index': idx,
                        'author': comment.get('userName', ''),
                        'text': comment.get('contents', ''),
                        'date': comment_date,
                        'likes': comment.get('sympathyCount', 0),
                        'dislikes': comment.get('antipathyCount', 0)
                    })

                return comments
            else:
                logger.warning(f"Comment API returned success=false for nid={nid}")
                return []
        else:
            logger.warning(f"Comment API returned status {response.status_code} for nid={nid}")
            return []

    def parse_date(self, date_str):
        """
        Parse date string to DB format

        Args:
            date_str: ISO 8601 format (e.g., "2025-11-01T13:36:00+09:00" or "2025-11-01T13:36:00.000Z")

        Returns:
            str: "YYYY-MM-DD HH:MM:SS" format
        """
        if not date_str:
            return None

        try:
            # Remove timezone info (everything after + or Z)
            if '+' in date_str:
                date_str = date_str.split('+')[0]
            elif date_str.endswith('Z'):
                date_str = date_str[:-1]

            # Remove milliseconds if present
            if '.' in date_str:
                date_str = date_str.split('.')[0]

            # Parse ISO format
            dt = datetime.fromisoformat(date_str)
            return dt.strftime("%Y-%m-%d %H:%M:%S")

        except Exception as e:
            logger.warning(f"Date parsing error: {e} for date_str={date_str}")
            return None

    def parse_list_date(self, date_text):
        """
        Parse date from discussion list page
        Handles formats like: "2025.11.01", "11.01", "2시간 전", "1분 전"

        Args:
            date_text: Date text from list page

        Returns:
            datetime: Parsed datetime object (or None if parsing fails)
        """
        if not date_text:
            return None

        date_text = date_text.strip()
        now = datetime.now()

        try:
            # Format: "2025.11.01 14:32" or "2025.11.01" or "11.01"
            if '.' in date_text:
                # Remove time part if exists (e.g., "2025.11.10 14:32" -> "2025.11.10")
                date_part = date_text.split()[0] if ' ' in date_text else date_text
                parts = date_part.split('.')

                if len(parts) == 3:  # "2025.11.01"
                    return datetime(int(parts[0]), int(parts[1]), int(parts[2]))
                elif len(parts) == 2:  # "11.01" - assume current year
                    return datetime(now.year, int(parts[0]), int(parts[1]))

            # Format: "N시간 전", "N분 전", "N초 전"
            if '시간' in date_text:
                hours = int(re.search(r'(\d+)', date_text).group(1))
                return now - timedelta(hours=hours)
            elif '분' in date_text:
                minutes = int(re.search(r'(\d+)', date_text).group(1))
                return now - timedelta(minutes=minutes)
            elif '초' in date_text:
                seconds = int(re.search(r'(\d+)', date_text).group(1))
                return now - timedelta(seconds=seconds)

            logger.warning(f"Unknown date format: {date_text}")
            return None

        except Exception as e:
            logger.warning(f"Error parsing list date '{date_text}': {e}")
            return None

    def crawl_stock_discussions(self, stock_code, max_posts=50, max_workers=10):
        """
        Main method: Get list then fetch details in parallel

        Args:
            stock_code: Stock code
            max_posts: Maximum number of posts to crawl
            max_workers: Number of parallel workers

        Returns:
            list: List of discussion dictionaries
        """
        logger.info(f"Starting to crawl stock {stock_code}")

        # Step 1: Get discussion list
        nid_list, stock_name = self.get_discussion_list(stock_code, max_pages=10)

        if not nid_list:
            logger.warning(f"No discussions found for stock {stock_code}")
            return []

        # Limit to max_posts
        nid_list = nid_list[:max_posts]

        # Step 2: Fetch details in parallel
        logger.info(f"Fetching {len(nid_list)} discussion details with {max_workers} workers...")
        results = []

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_nid = {
                executor.submit(self.get_discussion_detail, stock_code, nid, stock_name): nid
                for nid in nid_list
            }

            # Collect results as they complete
            completed = 0
            for future in as_completed(future_to_nid):
                nid = future_to_nid[future]
                completed += 1

                try:
                    result = future.result()
                    if result:
                        results.append(result)
                        logger.info(f"Progress: {completed}/{len(nid_list)} completed")
                except Exception as e:
                    logger.error(f"Error processing nid={nid}: {e}")

                # Add delay between requests
                time.sleep(self.request_delay)

        logger.info(f"Completed crawling {len(results)} discussions for stock {stock_code}")
        return results

    # ============================================================================
    # ASYNC VERSION - High Performance
    # ============================================================================

    async def get_discussion_detail_async(self, session, stock_code, nid, stock_name=None):
        """
        Async version of get_discussion_detail
        Fetch discussion detail using aiohttp
        """
        url = f"{self.mobile_url}/pc/domestic/stock/{stock_code}/discussion/{nid}"

        for attempt in range(self.max_retries):
            try:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as response:
                    if response.status != 200:
                        logger.warning(f"HTTP {response.status} for nid={nid}")
                        return None

                    html = await response.text()
                    soup = BeautifulSoup(html, 'html.parser')

                    # Find __NEXT_DATA__ script
                    script = soup.find('script', {'id': '__NEXT_DATA__'})
                    if not script:
                        logger.warning(f"No __NEXT_DATA__ found for nid={nid}")
                        return None

                    data = json.loads(script.string)
                    queries = data.get('props', {}).get('pageProps', {}).get('dehydratedState', {}).get('queries', [])

                    # Find discussion data
                    discussion_data = None
                    for query in queries:
                        query_key = query.get('queryKey', [{}])[0]
                        if isinstance(query_key, dict) and query_key.get('url') == '/discussion/detail':
                            discussion_data = query.get('state', {}).get('data', {}).get('result', {})
                            break

                    if not discussion_data:
                        logger.warning(f"No discussion data for nid={nid}")
                        return None

                    # Parse discussion data
                    title = discussion_data.get('title') or discussion_data.get('subject', '')
                    writer = discussion_data.get('writer', {})
                    author_name = writer.get('nickname', '')

                    # Parse content
                    content_html = discussion_data.get('contentHtml', '')
                    content_text = ''

                    if content_html:
                        content_soup = BeautifulSoup(content_html, 'html.parser')
                        content_text = content_soup.get_text(separator='\n', strip=True)
                    else:
                        content_json = discussion_data.get('contentJsonSwReplaced', '')
                        if content_json:
                            try:
                                content_data = json.loads(content_json)
                                summary_html = content_data.get('contentSummary', '')
                                if summary_html:
                                    summary_soup = BeautifulSoup(summary_html, 'html.parser')
                                    content_text = summary_soup.get_text(separator='\n', strip=True)
                            except:
                                content_text = content_json

                    full_content = f"{title}\n\n{content_text}"

                    # Parse date (API uses 'writtenAt' field, not 'regDate')
                    written_at = discussion_data.get('writtenAt', '')
                    post_date = self.parse_date(written_at)

                    likes_count = discussion_data.get('recommendCount', 0)
                    dislikes_count = discussion_data.get('notRecommendCount', 0)

                    # Get comments via API (async)
                    comments = await self.get_comments_via_api_async(session, nid, stock_code)

                    return {
                        'stock_code': stock_code,
                        'stock_name': stock_name,
                        'comment_id': int(nid),
                        'author_name': author_name,
                        'date': post_date,
                        'content': full_content,
                        'likes_count': likes_count,
                        'dislikes_count': dislikes_count,
                        'comment_data': comments
                    }

            except asyncio.TimeoutError:
                logger.warning(f"Timeout for nid={nid}, attempt {attempt + 1}/{self.max_retries}")
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(1)
                continue
            except Exception as e:
                logger.error(f"Error fetching detail for nid={nid}: {e}")
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(1)
                continue

        return None

    async def get_comments_via_api_async(self, session, nid, stock_code, page=1, page_size=100):
        """
        Async version of get_comments_via_api
        """
        url = "https://apis.naver.com/commentBox/cbox/web_naver_list_jsonp.json"

        params = {
            'ticket': 'finance',
            'templateId': 'community',
            'pool': 'cbox12',
            'lang': 'ko',
            'country': 'KR',
            'objectId': str(nid),
            'categoryId': '',
            'pageSize': page_size,
            'indexSize': 10,
            'groupId': '',
            'listType': 'OBJECT',
            'pageType': 'more',
            'page': page,
            'initialize': 'true',
            'followSize': 5,
            'useAltSort': 'true',
            'replyPageSize': 5,
            '_callback': 'jQuery',
            '_': str(int(time.time() * 1000))
        }

        headers = {
            'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 13_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.0.4 Mobile/15E148 Safari/604.1',
            'Referer': f'https://m.stock.naver.com/domestic/stock/{stock_code}/discussion/{nid}'
        }

        try:
            async with session.get(url, params=params, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as response:
                if response.status != 200:
                    return []

                jsonp_text = await response.text()
                json_text = re.sub(r'^[^(]*\(', '', jsonp_text)
                json_text = re.sub(r'\);?\s*$', '', json_text)

                data = json.loads(json_text)

                if data.get('success'):
                    comment_list = data.get('result', {}).get('commentList', [])
                    comments = []

                    for idx, comment in enumerate(comment_list, 1):
                        reg_time = comment.get('regTime', '')
                        comment_date = self.parse_date(reg_time)

                        comments.append({
                            'index': idx,
                            'author': comment.get('userName', ''),
                            'text': comment.get('contents', ''),
                            'date': comment_date,
                            'likes': comment.get('sympathyCount', 0),
                            'dislikes': comment.get('antipathyCount', 0)
                        })

                    return comments
        except Exception as e:
            logger.debug(f"Comment API error for nid={nid}: {e}")
            return []

        return []

    async def crawl_stock_discussions_async(self, stock_code, max_posts=50, max_concurrent=50):
        """
        Async version of crawl_stock_discussions
        Much faster than ThreadPool version

        Args:
            stock_code: Stock code
            max_posts: Maximum number of posts to crawl
            max_concurrent: Maximum concurrent requests (semaphore limit)

        Returns:
            list: List of discussion dictionaries
        """
        logger.info(f"[ASYNC] Starting to crawl stock {stock_code}")

        # Create aiohttp session with optimized connection pooling
        connector = aiohttp.TCPConnector(
            limit=150,                      # Total connection limit (increased from 50)
            limit_per_host=100,             # Per-host limit (increased from 50)
            ttl_dns_cache=300,              # DNS cache for 5 minutes
            enable_cleanup_closed=True,     # Auto-cleanup closed connections
            force_close=False,              # Enable keep-alive
            keepalive_timeout=60            # Keep-alive timeout 60s
        )
        timeout = aiohttp.ClientTimeout(total=None, connect=30, sock_read=30)

        async with aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'},
            cookies={'hide_cleanbot_contents': 'off'}
        ) as session:
            # Step 1: Get discussion list (ASYNC - 10 pages concurrently)
            nid_list, stock_name = await self.get_discussion_list_async(session, stock_code, max_pages=10)

            if not nid_list:
                logger.warning(f"No discussions found for stock {stock_code}")
                return []

            # Limit to max_posts
            nid_list = nid_list[:max_posts]

            # Step 2: Fetch details in parallel with async
            logger.info(f"[ASYNC] Fetching {len(nid_list)} discussions with max {max_concurrent} concurrent requests...")

            results = []
            semaphore = asyncio.Semaphore(max_concurrent)

            async def fetch_with_semaphore(nid):
                async with semaphore:
                    return await self.get_discussion_detail_async(session, stock_code, nid, stock_name)

            tasks = [fetch_with_semaphore(nid) for nid in nid_list]

            # Gather all results
            completed_results = await asyncio.gather(*tasks, return_exceptions=True)

            for idx, result in enumerate(completed_results):
                if isinstance(result, Exception):
                    logger.error(f"Task {idx+1} failed: {result}")
                elif result:
                    results.append(result)
                    logger.info(f"✅ Crawled nid={result['comment_id']}: {result['author_name']} ({len(result['comment_data'])} comments)")

        logger.info(f"[ASYNC] Completed crawling {len(results)}/{len(nid_list)} discussions for stock {stock_code}")
        return results


if __name__ == "__main__":
    # Test crawler
    crawler = NaverStockCrawlerPC()
    results = crawler.crawl_stock_discussions('005930', max_posts=10)

    logger.info(f"\nCrawled {len(results)} discussions")
    if results:
        logger.info(f"Sample: {results[0]}")

        # Save to JSON for inspection
        with open('v2_sample.json', 'w', encoding='utf-8') as f:
            json.dump({"results": results}, f, ensure_ascii=False, indent=2)
        logger.info("Sample saved to v2_sample.json")

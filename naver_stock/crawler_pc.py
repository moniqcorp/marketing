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
from datetime import datetime
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

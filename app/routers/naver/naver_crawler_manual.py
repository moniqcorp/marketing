import os
import json
import time
import traceback
from datetime import datetime
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
from dotenv import load_dotenv

from app.common.logger import naver_logger
from app.common.errors import NaverError
from app.common.gcs_uploader import upload_by_partition

load_dotenv()

BASE_DIR = Path(__file__).resolve().parents[3]
LOCAL_SAVE_DIR = BASE_DIR / "naver_discussions"
LOCAL_SAVE_DIR.mkdir(exist_ok=True)


def _log_and_print(message: str):
    """로그와 print 동시 출력"""
    naver_logger.info(message)
    print(message)


class NaverStockCrawler:
    """네이버 증권 토론 게시판 크롤러"""

    def __init__(self):
        self.base_url = "https://finance.naver.com"
        self.mobile_url = "https://m.stock.naver.com"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        self.session.cookies.set('hide_cleanbot_contents', 'off', domain='.naver.com')
        self.request_delay = float(os.getenv('REQUEST_DELAY', '0.3'))
        self.max_retries = int(os.getenv('MAX_RETRIES', '3'))

    def reset_session(self):
        """세션 초기화"""
        self.session.close()
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        self.session.cookies.set('hide_cleanbot_contents', 'off', domain='.naver.com')
        _log_and_print("세션 초기화 완료")

    def get_discussion_list(self, stock_code, max_pages=5):
        """토론 게시물 목록 가져오기"""
        nid_list = []
        stock_name = None

        _log_and_print(f"[{stock_code}] 게시물 목록 수집 시작 (최대 {max_pages}페이지)")

        for page in range(1, max_pages + 1):
            try:
                url = f"{self.base_url}/item/board.naver?code={stock_code}&page={page}"
                response = self.session.get(url)
                response.raise_for_status()
                soup = BeautifulSoup(response.text, 'html.parser')

                # 종목명 추출 (첫 페이지에서만)
                if page == 1:
                    stock_name_elem = soup.select_one('.wrap_company h2 a')
                    if stock_name_elem:
                        stock_name = stock_name_elem.text.strip()
                        _log_and_print(f"[{stock_code}] 종목명: {stock_name}")

                # 게시물 테이블 파싱
                table = soup.select_one('table.type2')
                if not table:
                    _log_and_print(f"[{stock_code}] 페이지 {page}: 테이블 없음")
                    break

                rows = table.select('tbody tr')
                page_nids = 0

                for row in rows:
                    if row.get('class') and 'blank_row' in row.get('class'):
                        continue
                    if 'u_cbox_cleanbot' in str(row):
                        continue

                    cells = row.select('td')
                    if len(cells) < 6:
                        continue

                    title_link = cells[1].select_one('a')
                    if not title_link:
                        continue

                    href = title_link.get('href', '')
                    nid_match = re.search(r'nid=(\d+)', href)
                    if not nid_match:
                        continue

                    nid = nid_match.group(1)
                    if nid not in nid_list:
                        nid_list.append(nid)
                        page_nids += 1

                _log_and_print(f"[{stock_code}] 페이지 {page}: {page_nids}개 발견")

                if page < max_pages:
                    time.sleep(self.request_delay)

            except Exception as e:
                _log_and_print(f"[{stock_code}] 페이지 {page} 수집 실패: {e}")
                break

        _log_and_print(f"[{stock_code}] 총 {len(nid_list)}개 게시물 발견")
        return nid_list, stock_name

    def get_discussion_detail(self, stock_code, nid, stock_name=None, retry_count=0):
        """게시물 상세 정보 가져오기"""
        mobile_url = f"{self.mobile_url}/pc/domestic/stock/{stock_code}/discussion/{nid}"

        try:
            response = self.session.get(mobile_url)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')

            # __NEXT_DATA__ 추출
            script = soup.find('script', {'id': '__NEXT_DATA__'})
            if not script:
                _log_and_print(f"[{stock_code}] nid={nid}: __NEXT_DATA__ 없음")
                return None

            data = json.loads(script.string)
            queries = data.get('props', {}).get('pageProps', {}).get('dehydratedState', {}).get('queries', [])

            # 토론 데이터 추출
            discussion_data = None
            for query in queries:
                query_key = query.get('queryKey', [{}])
                if len(query_key) > 0 and query_key[0].get('url') == '/discussion/detail':
                    discussion_data = query.get('state', {}).get('data', {}).get('result', {})
                    break

            if not discussion_data:
                _log_and_print(f"[{stock_code}] nid={nid}: 토론 데이터 없음")
                return None

            # 데이터 파싱
            title = discussion_data.get('title') or discussion_data.get('subject', '')
            writer = discussion_data.get('writer', {})
            author_name = writer.get('nickname', '')

            # 내용 추출
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

            full_content = f"{title}\n\n{content_text}" if title else content_text

            likes_count = discussion_data.get('recommendCount', 0)
            dislikes_count = discussion_data.get('notRecommendCount', 0)

            # 날짜 파싱
            written_at = discussion_data.get('writtenAt', '')
            date = self.parse_date(written_at)

            # 댓글 수집
            comments = []
            try:
                comments = self.get_comments_via_api(nid, stock_code)
            except Exception as comment_error:
                _log_and_print(f"[{stock_code}] nid={nid}: 댓글 수집 실패 - {comment_error}")

            _log_and_print(f"[{stock_code}] nid={nid}: 수집 완료 ({len(comments)}개 댓글)")

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
                _log_and_print(f"[{stock_code}] nid={nid}: 재시도 {retry_count + 1}/{self.max_retries}")
                self.reset_session()
                time.sleep(1)
                return self.get_discussion_detail(stock_code, nid, stock_name, retry_count + 1)
            else:
                _log_and_print(f"[{stock_code}] nid={nid}: 최대 재시도 초과 - {e}")
                return None

    def get_comments_via_api(self, nid, stock_code, page=1, page_size=100):
        """네이버 댓글 API로 댓글 수집"""
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

        response = self.session.get(url, params=params, headers=headers)

        if response.status_code == 200:
            jsonp_text = response.text
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
        return []

    def parse_date(self, date_str):
        """ISO 8601 날짜 파싱"""
        if not date_str:
            return None

        try:
            if '+' in date_str:
                date_str = date_str.split('+')[0]
            elif date_str.endswith('Z'):
                date_str = date_str[:-1]

            if '.' in date_str:
                date_str = date_str.split('.')[0]

            dt = datetime.fromisoformat(date_str)
            return dt.strftime("%Y-%m-%d %H:%M:%S")

        except Exception as e:
            _log_and_print(f"날짜 파싱 실패: {date_str} - {e}")
            return None

    def crawl_stock_discussions(self, stock_code, max_posts=50, max_workers=10):
        """전체 크롤링 프로세스"""
        _log_and_print(f"[{stock_code}] 크롤링 시작")

        # 1. 게시물 목록 가져오기
        nid_list, stock_name = self.get_discussion_list(stock_code, max_pages=10)

        if not nid_list:
            _log_and_print(f"[{stock_code}] 게시물 없음")
            return []

        # 최대 게시물 수 제한
        nid_list = nid_list[:max_posts]

        # 2. 병렬로 상세 정보 수집
        _log_and_print(f"[{stock_code}] {len(nid_list)}개 게시물 상세 수집 시작 (워커: {max_workers})")
        results = []

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_nid = {
                executor.submit(self.get_discussion_detail, stock_code, nid, stock_name): nid
                for nid in nid_list
            }

            completed = 0
            for future in as_completed(future_to_nid):
                nid = future_to_nid[future]
                completed += 1

                try:
                    result = future.result()
                    if result:
                        results.append(result)
                        _log_and_print(f"[{stock_code}] 진행: {completed}/{len(nid_list)}")
                except Exception as e:
                    _log_and_print(f"[{stock_code}] nid={nid} 처리 실패: {e}")

                time.sleep(self.request_delay)

        _log_and_print(f"[{stock_code}] 크롤링 완료: {len(results)}개 수집")
        return results


async def main(body: dict):
    """메인 크롤링 함수"""
    try:
        stock_code = body.get("stock_code")
        max_posts = body.get("max_posts", 50)

        crawler = NaverStockCrawler()

        # 1. 데이터 수집
        discussions = crawler.crawl_stock_discussions(
            stock_code=stock_code,
            max_posts=max_posts,
            max_workers=10
        )

        if not discussions:
            _log_and_print(f"[{stock_code}] 수집된 게시물 없음")
            return {
                "code": 204,
                "message": f"[{stock_code}] 수집된 게시물 없음",
                "stock_code": stock_code,
                "total_discussions": 0,
            }

        # 2. DataFrame 변환 및 dt 컬럼 추가
        df = pd.DataFrame(discussions)
        df['dt'] = df['date'].apply(lambda x: x.split()[0] if x else None)
        df['isin_code'] = stock_code  # ISIN 코드 추가
        df['source'] = 'naver'

        # comment_data를 JSON 문자열로 변환 (BigQuery 호환)
        df['comment_data'] = df['comment_data'].apply(lambda x: json.dumps(x, ensure_ascii=False) if x else '[]')

        # 3. GCS 업로드
        parquet_urls = upload_by_partition(
            df=df,
            identifier=stock_code,
            base_gcs_path="marketing/stock_discussion",
            local_save_dir=LOCAL_SAVE_DIR,
            log_func=_log_and_print
        )

        return {
            "code": 200,
            "message": "네이버 토론 게시물 수집 및 업로드 완료",
            "stock_code": stock_code,
            "total_discussions": len(df),
            "partitions": len(parquet_urls),
            "parquet_urls": parquet_urls,
        }

    except NaverError as e:
        naver_logger.error(f"Naver 에러 (종목: {body.get('stock_code')}): {e}")
        return e.to_dict()
    except Exception as e:
        naver_logger.error(f"예상치 못한 에러: {traceback.format_exc()}")
        return {
            "code": 500,
            "message": f"알 수 없는 내부 서버 오류: {str(e)}",
        }

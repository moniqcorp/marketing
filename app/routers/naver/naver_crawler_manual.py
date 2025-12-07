import os
import json
import time
import asyncio
import traceback
from datetime import datetime, date, timedelta
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
from app.common.bigquery_client import get_stock_by_code, get_stock_list
from app.common.request_function import browser_manager

load_dotenv()

# Playwright 전환 페이지 (이 페이지 이후부터 Playwright 사용)
PLAYWRIGHT_SWITCH_PAGE = 100

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
        self.request_delay = float(os.getenv('REQUEST_DELAY', '1.0'))  # 0.3 → 1.0으로 증가 (차단 방지)
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

    async def collect_pages_with_playwright(self, stock_code: str, start_page: int, start_dt, end_dt, existing_nids: list):
        """
        Playwright를 사용하여 100페이지부터 "다음" 버튼 클릭 기반으로 수집
        - 테스트 스크립트에서 검증된 로직 적용
        - table.Nnavi td.pgR a 셀렉터 사용 (10페이지씩 이동)
        - Alert 핸들러로 네이버 차단 감지
        - 페이지 제한 없음 (날짜 기반으로 종료)
        """
        browser = browser_manager.browsers.get("chromium")
        if not browser:
            raise Exception("Playwright 브라우저가 초기화되지 않았습니다")

        nid_list = []
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={"width": 1920, "height": 1080},
            locale="ko-KR",
            timezone_id="Asia/Seoul",
        )

        # Alert 감지 변수
        alert_detected = False
        alert_message = ""

        try:
            page = await context.new_page()

            # Alert 다이얼로그 핸들러 설정
            async def handle_dialog(dialog):
                nonlocal alert_detected, alert_message
                alert_detected = True
                alert_message = dialog.message
                _log_and_print(f"[{stock_code}] Playwright: Alert 감지 - {dialog.message}")
                await dialog.dismiss()

            page.on("dialog", handle_dialog)

            # 시작 페이지로 이동
            start_url = f"{self.base_url}/item/board.naver?code={stock_code}&page={start_page}"
            _log_and_print(f"[{stock_code}] Playwright: 페이지 {start_page}부터 '다음' 버튼 클릭 수집 시작")

            # 재시도 설정
            max_retries = 3
            retry_delay = 30  # 초

            # 초기 페이지 로드 (재시도 포함)
            for retry in range(max_retries):
                try:
                    await page.goto(start_url, wait_until="domcontentloaded", timeout=30000)
                    # 테이블 로드 대기
                    await page.wait_for_selector('table.type2', timeout=10000)
                    # 게시물 행 로드 대기
                    await page.wait_for_selector('table.type2 tbody tr td.title a', timeout=10000)
                    break  # 성공 시 루프 탈출
                except Exception as e:
                    if retry < max_retries - 1:
                        _log_and_print(f"[{stock_code}] Playwright: 초기 로드 실패 ({retry + 1}/{max_retries}), {retry_delay}초 후 재시도 - {e}")
                        await asyncio.sleep(retry_delay)
                    else:
                        _log_and_print(f"[{stock_code}] Playwright: 초기 테이블 로드 {max_retries}회 실패, 수집 종료")
                        return nid_list

            current_page = start_page
            empty_page_count = 0
            max_empty_pages = 5
            should_stop = False
            click_count = 0
            # max_pages 제한 없음 - 날짜 기반으로 종료

            # 시작 페이지 수집
            html = await page.content()
            page_nids, should_stop_flag, has_valid_rows = self._parse_discussion_page(
                html, stock_code, current_page, start_dt, end_dt, existing_nids + nid_list
            )
            if page_nids:
                nid_list.extend(page_nids)
                _log_and_print(f"[{stock_code}] Playwright 페이지 {current_page}: {len(page_nids)}개 발견")
            else:
                _log_and_print(f"[{stock_code}] Playwright 페이지 {current_page}: 0개")

            if has_valid_rows:
                empty_page_count = 0
            else:
                empty_page_count += 1

            if should_stop_flag:
                _log_and_print(f"[{stock_code}] start_date 이전 도달, Playwright 수집 종료")
                await context.close()
                return nid_list

            # 메인 루프: "다음" 버튼(pgR)으로 10페이지씩 이동하며 수집 (페이지 제한 없음)
            while not should_stop and empty_page_count < max_empty_pages:
                # Alert 감지 확인
                if alert_detected:
                    _log_and_print(f"[{stock_code}] Playwright: Alert으로 인해 수집 중단 - {alert_message}")
                    break

                # "다음" 버튼 찾기 - pgR 클래스 (다음 10페이지)
                next_button = page.locator('table.Nnavi td.pgR a')

                # 버튼 존재 확인
                button_count = await next_button.count()
                if button_count == 0:
                    _log_and_print(f"[{stock_code}] Playwright: '다음' 버튼(pgR) 없음, 마지막 페이지 도달")
                    break

                # 버튼 가시성 확인
                try:
                    is_visible = await next_button.is_visible(timeout=3000)
                    if not is_visible:
                        _log_and_print(f"[{stock_code}] Playwright: '다음' 버튼 보이지 않음, 수집 종료")
                        break
                except:
                    _log_and_print(f"[{stock_code}] Playwright: '다음' 버튼 가시성 확인 실패")
                    break

                # 0.5초 딜레이 후 클릭
                await asyncio.sleep(0.5)

                try:
                    await next_button.click()
                    click_count += 1
                    current_page += 10  # 다음 버튼은 10페이지씩 이동

                    _log_and_print(f"[{stock_code}] Playwright: 클릭 #{click_count} - 페이지 {current_page}로 이동 중...")

                    # 페이지 로드 대기
                    try:
                        await page.wait_for_selector('table.type2', timeout=15000)
                        await page.wait_for_selector('table.type2 tbody tr td.title a', timeout=10000)
                    except:
                        _log_and_print(f"[{stock_code}] Playwright: 페이지 {current_page} 로드 지연, 추가 대기...")
                        await asyncio.sleep(2)

                    # Alert 재확인
                    if alert_detected:
                        _log_and_print(f"[{stock_code}] Playwright: Alert 감지로 수집 중단")
                        break

                    # 현재 페이지 수집
                    html = await page.content()
                    page_nids, should_stop_flag, has_valid_rows = self._parse_discussion_page(
                        html, stock_code, current_page, start_dt, end_dt, existing_nids + nid_list
                    )

                    if page_nids:
                        nid_list.extend(page_nids)
                        _log_and_print(f"[{stock_code}] Playwright 페이지 {current_page}: {len(page_nids)}개 발견")
                    else:
                        _log_and_print(f"[{stock_code}] Playwright 페이지 {current_page}: 0개")

                    # 빈 페이지 카운팅
                    if has_valid_rows:
                        empty_page_count = 0
                    else:
                        empty_page_count += 1
                        _log_and_print(f"[{stock_code}] Playwright: 빈 페이지 ({empty_page_count}/{max_empty_pages})")

                    if should_stop_flag:
                        _log_and_print(f"[{stock_code}] start_date 이전 도달, Playwright 수집 종료")
                        should_stop = True
                        break

                    if empty_page_count >= max_empty_pages:
                        _log_and_print(f"[{stock_code}] 연속 {max_empty_pages}페이지 빈 결과, Playwright 수집 종료")
                        break

                except Exception as e:
                    _log_and_print(f"[{stock_code}] Playwright: 클릭 #{click_count} 에러 - {e}")
                    break

        except Exception as e:
            _log_and_print(f"[{stock_code}] Playwright 수집 에러: {e}")
        finally:
            await context.close()

        _log_and_print(f"[{stock_code}] Playwright 수집 완료: 총 {len(nid_list)}개 (클릭 {click_count}회, 마지막 페이지 ~{current_page})")
        return nid_list

    def _parse_discussion_page(self, html: str, stock_code: str, page_num: int, start_dt, end_dt, existing_nids: list):
        """페이지 HTML에서 게시물 NID 추출 (공통 파싱 로직)
        Returns: (nid_list, should_stop, has_valid_rows) - has_valid_rows는 유효한 게시물 행이 있었는지 여부
        """
        soup = BeautifulSoup(html, 'html.parser')
        nid_list = []
        should_stop = False
        has_valid_rows = False  # 유효한 게시물 행이 있었는지

        table = soup.select_one('table.type2')
        if not table:
            return nid_list, should_stop, has_valid_rows

        rows = table.select('tbody tr')

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

            has_valid_rows = True  # 유효한 게시물 행 발견

            nid = nid_match.group(1)

            # 이미 수집된 NID인지 확인
            if nid in existing_nids:
                continue

            # 날짜 추출
            date_cell = cells[0].get_text(strip=True)
            try:
                if ':' in date_cell and '.' not in date_cell:
                    post_dt = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
                else:
                    post_date_str = date_cell.split()[0] if ' ' in date_cell else date_cell
                    post_dt = datetime.strptime(post_date_str, "%Y.%m.%d")

                if end_dt and post_dt > end_dt:
                    continue  # 미래 스킵 (빈 페이지로 카운트하면 안됨)

                if start_dt and post_dt < start_dt:
                    should_stop = True
                    break
            except:
                pass

            nid_list.append(nid)

        return nid_list, should_stop, has_valid_rows

    async def get_discussion_list(self, stock_code, start_date=None, end_date=None):
        """토론 게시물 목록 가져오기 (날짜 기반 필터링, 100페이지 이후 Playwright 클릭 기반 수집)"""
        nid_list = []
        stock_name = None
        empty_page_count = 0
        max_empty_pages = 5  # 연속 빈 페이지 허용 수

        # 날짜 파싱
        start_dt = datetime.strptime(start_date, "%Y-%m-%d") if start_date else None
        end_dt = datetime.strptime(end_date, "%Y-%m-%d") if end_date else None

        _log_and_print(f"[{stock_code}] 게시물 목록 수집 시작 (기간: {start_date} ~ {end_date})")

        page = 0
        block_retry_count = 0
        max_block_retries = 3  # IP 차단 시 최대 재시도 횟수
        page_error_count = 0  # 페이지 수집 에러 카운터
        max_page_errors = 3  # 최대 연속 페이지 에러 허용
        should_continue_with_playwright = False  # 100페이지 도달 후 Playwright로 계속할지 여부
        force_playwright_switch = False  # 에러 발생 시 Playwright로 강제 전환

        # Phase 1: requests로 1~100페이지 수집
        while page < PLAYWRIGHT_SWITCH_PAGE:
            page += 1

            try:
                url = f"{self.base_url}/item/board.naver?code={stock_code}&page={page}"
                response = self.session.get(url, timeout=30)
                response.raise_for_status()
                soup = BeautifulSoup(response.text, 'html.parser')

                # 종목명 추출 (첫 페이지에서만)
                if page == 1:
                    stock_name_elem = soup.select_one('.wrap_company h2 a')
                    if stock_name_elem:
                        stock_name = stock_name_elem.text.strip()
                        _log_and_print(f"[{stock_code}] 종목명: {stock_name}")

                # IP 차단 감지 (에러 페이지)
                page_html = response.text
                if 'error_content' in page_html or '페이지를 찾을 수 없습니다' in page_html:
                    block_retry_count += 1
                    if block_retry_count > max_block_retries:
                        _log_and_print(f"[{stock_code}] IP 차단 {max_block_retries}회 재시도 실패, 수집 중단")
                        break
                    _log_and_print(f"[{stock_code}] 페이지 {page}: IP 차단 감지, 60초 대기 후 재시도 ({block_retry_count}/{max_block_retries})")
                    await asyncio.sleep(60)  # 60초 백오프
                    self.reset_session()
                    page -= 1  # 같은 페이지 재시도를 위해 감소
                    continue

                # 정상 응답 시 차단 카운터 리셋
                block_retry_count = 0

                # 게시물 테이블 파싱
                table = soup.select_one('table.type2')
                if not table:
                    _log_and_print(f"[{stock_code}] 페이지 {page}: 테이블 없음")
                    break

                rows = table.select('tbody tr')
                page_nids = 0
                should_stop = False
                valid_rows_count = 0  # 실제 유효한 행 수
                skipped_future_count = 0  # end_date 이후라서 스킵된 수

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

                    valid_rows_count += 1

                    # 날짜 추출
                    date_cell = cells[0].get_text(strip=True)
                    try:
                        if ':' in date_cell and '.' not in date_cell:
                            post_dt = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
                        else:
                            post_date_str = date_cell.split()[0] if ' ' in date_cell else date_cell
                            post_dt = datetime.strptime(post_date_str, "%Y.%m.%d")

                        if end_dt and post_dt > end_dt:
                            skipped_future_count += 1
                            continue

                        if start_dt and post_dt < start_dt:
                            should_stop = True
                            break
                    except:
                        pass

                    nid = nid_match.group(1)
                    if nid not in nid_list:
                        nid_list.append(nid)
                        page_nids += 1

                _log_and_print(f"[{stock_code}] 페이지 {page}: {page_nids}개 발견 (유효 행: {valid_rows_count}, 미래 스킵: {skipped_future_count})")

                # 연속 빈 페이지 감지
                if valid_rows_count == 0:
                    empty_page_count += 1
                    if empty_page_count >= max_empty_pages:
                        _log_and_print(f"[{stock_code}] 연속 {max_empty_pages}페이지 빈 결과, 수집 중단")
                        break
                else:
                    empty_page_count = 0

                if should_stop:
                    _log_and_print(f"[{stock_code}] start_date({start_date}) 이전 게시물 도달, 탐색 종료")
                    break

                # 100페이지에 도달하면 Playwright로 계속
                if page == PLAYWRIGHT_SWITCH_PAGE:
                    should_continue_with_playwright = True
                    _log_and_print(f"[{stock_code}] {PLAYWRIGHT_SWITCH_PAGE}페이지 도달, Playwright 클릭 기반 수집으로 전환")

                await asyncio.sleep(self.request_delay)

            except Exception as e:
                page_error_count += 1
                _log_and_print(f"[{stock_code}] 페이지 {page} 수집 실패 ({page_error_count}/{max_page_errors}): {e}")

                if page_error_count < max_page_errors:
                    # 재시도: 30초 대기 후 세션 리셋하고 같은 페이지 재시도
                    _log_and_print(f"[{stock_code}] 30초 대기 후 페이지 {page} 재시도...")
                    await asyncio.sleep(30)
                    self.reset_session()
                    page -= 1  # 같은 페이지 재시도
                    continue
                else:
                    # 최대 재시도 초과 시 Playwright로 강제 전환
                    _log_and_print(f"[{stock_code}] 연속 {max_page_errors}회 실패, Playwright로 강제 전환")
                    force_playwright_switch = True
                    break

        # Phase 2: Playwright 클릭 기반으로 수집 (정상 100페이지 도달 또는 에러 시 강제 전환)
        if should_continue_with_playwright or force_playwright_switch:
            # 강제 전환 시에는 에러 발생 페이지부터 시작, 정상 전환 시에는 100페이지부터
            start_from_page = page if force_playwright_switch else PLAYWRIGHT_SWITCH_PAGE
            _log_and_print(f"[{stock_code}] Playwright 수집 시작 (페이지 {start_from_page}부터)")

            playwright_nids = await self.collect_pages_with_playwright(
                stock_code=stock_code,
                start_page=start_from_page,
                start_dt=start_dt,
                end_dt=end_dt,
                existing_nids=nid_list
            )
            nid_list.extend(playwright_nids)

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

    async def crawl_stock_discussions(self, stock_code, start_date=None, end_date=None, max_workers=10):
        """전체 크롤링 프로세스 (날짜 기반, 100페이지 이후 Playwright 사용)"""
        _log_and_print(f"[{stock_code}] 크롤링 시작 (기간: {start_date} ~ {end_date})")

        # 1. 게시물 목록 가져오기 (날짜 필터링 적용, 100페이지 이후 Playwright)
        nid_list, stock_name = await self.get_discussion_list(
            stock_code,
            start_date=start_date,
            end_date=end_date
        )

        if not nid_list:
            _log_and_print(f"[{stock_code}] 게시물 없음")
            return []

        # 2. 병렬로 상세 정보 수집 (상세 페이지는 기존 requests 사용)
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
        start_date = body.get("start_date")
        end_date = body.get("end_date")

        # 잘못된 값 방어 (Swagger "string" 기본값 등)
        if not start_date or start_date == "string":
            start_date = (date.today() - timedelta(days=7)).strftime("%Y-%m-%d")
        if not end_date or end_date == "string":
            end_date = date.today().strftime("%Y-%m-%d")

        # BigQuery에서 종목 정보 조회
        stock_info = get_stock_by_code(stock_code)
        if stock_info:
            isin_code = stock_info.get("isin_code", "")
            _log_and_print(f"[{stock_code}] BigQuery 종목 정보: isin_code={isin_code}")
        else:
            isin_code = ""
            _log_and_print(f"[{stock_code}] BigQuery에서 종목 정보를 찾을 수 없음")

        crawler = NaverStockCrawler()

        # 1. 데이터 수집 (날짜 기반, 100페이지 이후 Playwright 사용)
        discussions = await crawler.crawl_stock_discussions(
            stock_code=stock_code,
            start_date=start_date,
            end_date=end_date,
            max_workers=10
        )

        if not discussions:
            _log_and_print(f"[{stock_code}] 수집된 게시물 없음")
            return {
                "code": 204,
                "message": f"[{stock_code}] 수집된 게시물 없음",
                "stock_code": stock_code,
                "start_date": start_date,
                "end_date": end_date,
                "total_discussions": 0,
            }

        # 2. DataFrame 변환 및 dt 컬럼 추가
        df = pd.DataFrame(discussions)
        df['dt'] = df['date'].apply(lambda x: x.split()[0] if x else None)
        df['isin_code'] = isin_code  # BigQuery에서 조회한 ISIN 코드
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
            "start_date": start_date,
            "end_date": end_date,
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


async def main_batch(body: dict):
    """배치 크롤링 함수 - BigQuery stocks 테이블의 전체 종목 수집"""
    try:
        start_date = body.get("start_date")
        end_date = body.get("end_date")

        # 잘못된 값 방어 (Swagger "string" 기본값 등)
        if not start_date or start_date == "string":
            start_date = (date.today() - timedelta(days=7)).strftime("%Y-%m-%d")
        if not end_date or end_date == "string":
            end_date = date.today().strftime("%Y-%m-%d")

        # BigQuery에서 전체 종목 목록 조회
        stocks = get_stock_list()
        if not stocks:
            _log_and_print("BigQuery에서 종목 목록을 가져올 수 없음")
            return {
                "code": 204,
                "message": "BigQuery에서 종목 목록을 가져올 수 없음",
                "total_stocks": 0,
            }

        _log_and_print(f"배치 수집 시작: {len(stocks)}개 종목 (기간: {start_date} ~ {end_date})")

        results = []
        success_count = 0
        fail_count = 0

        for idx, stock in enumerate(stocks, 1):
            stock_code = stock.get("stock_code")
            isin_code = stock.get("isin_code", "")
            stock_name = stock.get("stock_name", "")

            _log_and_print(f"[{idx}/{len(stocks)}] {stock_code} ({stock_name}) 수집 시작")

            try:
                crawler = NaverStockCrawler()
                discussions = await crawler.crawl_stock_discussions(
                    stock_code=stock_code,
                    start_date=start_date,
                    end_date=end_date,
                    max_workers=10
                )

                if not discussions:
                    _log_and_print(f"[{stock_code}] 수집된 게시물 없음")
                    results.append({
                        "stock_code": stock_code,
                        "status": "no_data",
                        "total_discussions": 0,
                    })
                    continue

                # DataFrame 변환 및 저장
                df = pd.DataFrame(discussions)
                df['dt'] = df['date'].apply(lambda x: x.split()[0] if x else None)
                df['isin_code'] = isin_code
                df['source'] = 'naver'
                df['comment_data'] = df['comment_data'].apply(
                    lambda x: json.dumps(x, ensure_ascii=False) if x else '[]'
                )

                parquet_urls = upload_by_partition(
                    df=df,
                    identifier=stock_code,
                    base_gcs_path="marketing/stock_discussion",
                    local_save_dir=LOCAL_SAVE_DIR,
                    log_func=_log_and_print
                )

                success_count += 1
                results.append({
                    "stock_code": stock_code,
                    "status": "success",
                    "total_discussions": len(df),
                    "parquet_urls": parquet_urls,
                })

            except Exception as e:
                fail_count += 1
                _log_and_print(f"[{stock_code}] 수집 실패: {e}")
                results.append({
                    "stock_code": stock_code,
                    "status": "failed",
                    "error": str(e),
                })

        _log_and_print(f"배치 수집 완료: 성공 {success_count}, 실패 {fail_count}")

        return {
            "code": 200,
            "message": "배치 수집 완료",
            "start_date": start_date,
            "end_date": end_date,
            "total_stocks": len(stocks),
            "success_count": success_count,
            "fail_count": fail_count,
            "results": results,
        }

    except Exception as e:
        naver_logger.error(f"배치 수집 에러: {traceback.format_exc()}")
        return {
            "code": 500,
            "message": f"배치 수집 오류: {str(e)}",
        }

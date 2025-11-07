import json
import traceback
from datetime import datetime, timedelta, timezone
from pathlib import Path
import zoneinfo
from app.common.request_function import AsyncCurlClient
from app.common.errors import TossError
from app.common.logger import toss_logger
from app.routers.toss.toss_cookies import fetch_cookies
from fastapi import BackgroundTasks

KST = zoneinfo.ZoneInfo("Asia/Seoul")


def _log_and_print(message: str):
    """로그와 print 동시 출력"""
    toss_logger.info(message)
    print(message)


async def fetch_comments_by_date(
    stock_code: str,
    cookies: dict,
    session: AsyncCurlClient,
    sort_type: str = "RECENT",
    max_pages: int = 10000,
):
    """어제 ~ 30일 전 댓글 수집 (메모리에 저장)"""

    now = datetime.now(KST)
    end_time = now.replace(hour=23, minute=59, second=59, microsecond=0) - timedelta(
        days=1
    )
    start_time = end_time.replace(hour=0, minute=0, second=0) - timedelta(days=2)

    url = "https://wts-cert-api.tossinvest.com/api/v3/comments"
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:143.0) Gecko/20100101 Firefox/143.0",
        "Accept": "application/json",
        "Accept-Language": "ko-KR,ko;q=0.8,en-US;q=0.5,en;q=0.3",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Referer": "https://www.tossinvest.com/stocks/A000660/community",
        "content-type": "application/json",
        "X-XSRF-TOKEN": "860ca56f-e9cf-4a13-a30d-e74b189c67eb",
        "browser-tab-id": "browser-tab-330af8be23c94da7a5eff21ae217d2e0",
        "App-Version": "v251024.1930",
        "Origin": "https://www.tossinvest.com",
        "Connection": "keep-alive",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-site",
        "Priority": "u=4",
    }

    page_num = 0
    total_comments = 0
    next_comment_id = None
    collected_comments = []  # 수집한 댓글 저장

    _log_and_print(f"[{stock_code}] 댓글 수집 시작 (범위: {start_time} ~ {end_time})")

    for _ in range(max_pages):
        page_num += 1

        payload = {
            "subjectId": stock_code,
            "subjectType": "STOCK",
            "commentSortType": sort_type,
        }
        if next_comment_id:
            payload["commentId"] = next_comment_id

        response, _ = await session.post(
            url,
            headers=headers,
            json_data=payload,
            cookies=cookies,
            body_type="JSON",
        )

        # 응답에서 comments 배열 추출
        comments = response.get("result", {}).get("comments", {})
        if isinstance(comments, dict):
            comments = comments.get("body", [])

        if not comments:
            _log_and_print(f"[{stock_code}] 페이지 {page_num}: 데이터 없음. 수집 종료")
            break

        # 시간 범위 필터링
        page_valid_count = 0
        for comment in comments:
            updated_at_str = comment.get("updatedAt")
            if not updated_at_str:
                continue

            try:
                updated_at = datetime.fromisoformat(updated_at_str).astimezone(KST)
            except (ValueError, TypeError):
                continue

            # 어제 이후 데이터 스킵
            if updated_at > end_time:
                continue

            # 30일 전 이전 데이터면 종료
            if updated_at < start_time:
                _log_and_print(
                    f"[{stock_code}] 페이지 {page_num}: 시간 범위 초과. "
                    f"총 {page_num}페이지, {total_comments}개 댓글"
                )
                return collected_comments

            collected_comments.append(comment)  # 메모리에 저장
            total_comments += 1
            page_valid_count += 1

        _log_and_print(
            f"[{stock_code}] 페이지 {page_num}: {len(comments)}개 조회, {page_valid_count}개 필터링, "
            f"누적 {total_comments}개"
        )

        # 다음 페이지 준비
        next_comment_id = comments[-1].get("id")
        if not next_comment_id:
            _log_and_print(
                f"[{stock_code}] 페이지 {page_num}: 다음 댓글 ID 없음. "
                f"총 {page_num}페이지, {total_comments}개 댓글"
            )
            break

    _log_and_print(f"[{stock_code}] 수집 완료: 총 {total_comments}개 댓글")
    return collected_comments


# async def _background_fetch_comments(
#     stock_code: str,
#     cookies: dict,
#     session: AsyncCurlClient,
#     output_file: str = None,
# ):
#     """백그라운드 작업용 래퍼"""
#     try:
#         await fetch_comments_by_date(stock_code, cookies, session, output_file)
#     except Exception as e:
#         toss_logger.error(
#             f"백그라운드 작업 실패 ({stock_code}): {traceback.format_exc()}"
#         )
#         print(f"백그라운드 작업 실패 ({stock_code}): {traceback.format_exc()}")
#     finally:
#         if session:
#             try:
#                 await session.close()
#             except Exception as e:
#                 toss_logger.error(f"세션 종료 중 에러: {e}")


async def main(body: dict, background_tasks: BackgroundTasks):
    """즉시 응답 + 수집한 댓글 데이터 반환"""
    try:
        session = AsyncCurlClient()
        stock_code = body.get("stock_code")
        cookies = await fetch_cookies(body)

        # 댓글 수집
        comments = await fetch_comments_by_date(stock_code, cookies, session)

        # 수집한 댓글 반환
        return {
            "code": 200,
            "message": f"댓글 수집 완료",
            "stock_code": stock_code,
            "total_comments": len(comments),
            "comments": comments,  # 수집한 댓글 데이터
        }

    except TossError as e:
        toss_logger.error(f"초기화 실패 (종목: {body.get('id')}): {e}")
        return e.to_dict()
    except Exception as e:
        toss_logger.error(f"예상치 못한 에러: {traceback.format_exc()}")
        return {
            "code": 500,
            "message": f"알 수 없는 내부 서버 오류: {str(e)}",
        }

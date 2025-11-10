import asyncio
import json
import os
import traceback
from datetime import datetime, timedelta, timezone
from pathlib import Path
import zoneinfo
from dotenv import load_dotenv
import pandas as pd
from google.cloud import storage
from app.common.request_function import AsyncCurlClient
from app.common.errors import TossError
from app.common.logger import toss_logger
from app.routers.toss.toss_cookies import fetch_cookies
from fastapi import BackgroundTasks


load_dotenv()


KST = zoneinfo.ZoneInfo("Asia/Seoul")
BASE_DIR = Path(__file__).resolve().parents[3]
LOCAL_SAVE_DIR = BASE_DIR / "toss_comments"
LOCAL_SAVE_DIR.mkdir(exist_ok=True)


def _log_and_print(message: str):
    """로그와 print 동시 출력"""
    toss_logger.info(message)
    print(message)


async def fetch_comments_by_date(
    stock_code: str,
    cookies: dict,
    session: AsyncCurlClient,
    start_time: datetime,
    end_time: datetime,
    sort_type: str = "RECENT",
    max_pages: int = 100000000,
):
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


async def fetch_comments_reply(
    cookies, comment, session=AsyncCurlClient, max_pages: int = 10
):
    url = f"https://wts-cert-api.tossinvest.com/api/v1/comments/{comment}/replies"
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:144.0) Gecko/20100101 Firefox/144.0",
        "Accept": "application/json",
        "Accept-Language": "ko-KR,ko;q=0.8,en-US;q=0.5,en;q=0.3",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Referer": "https://www.tossinvest.com/stocks/US20170803003/community",
        "browser-tab-id": "browser-tab-150929dbf77e45e795f9af3303c3247e",
        "App-Version": "v251110.1623",
        "Origin": "https://www.tossinvest.com",
        "Connection": "keep-alive",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-site",
        "Priority": "u=4",
    }
    all_replies = []
    next_reply_id = None

    for page in range(max_pages):
        if next_reply_id:  # 다음 페이지 기준 id
            url = f"https://wts-cert-api.tossinvest.com/api/v1/comments/{comment}/replies?lastReplyId={next_reply_id}"

        response, _ = await session.get(
            url=url,
            headers=headers,
            cookies=cookies,
            body_type="JSON",
        )

        results = response.get("result", {}).get("replies", {})
        replies = results.get("body", [])
        all_replies.extend(replies)

        # 페이지네이션 제어
        has_next = results.get("hasNext")
        if not has_next or not replies:
            break

        # 다음 요청용 reply_id 갱신
        next_reply_id = replies[-1].get("id")

    return all_replies


async def merge_comments_and_replies(comments, cookies, session):
    """
    댓글 리스트에 replyCount > 0 인 항목의 대댓글을 병렬로 수집 후 병합한다.
    최종 결과는 DB 스키마에 맞는 구조로 반환한다.
    """
    tasks = []
    for comment in comments:
        if int(comment.get("replyCount", 0)) > 0:
            tasks.append(
                fetch_comments_reply(cookies, comment["id"], session, max_pages=10)
            )
        else:
            tasks.append(asyncio.sleep(0, result=[]))  # 대댓글 없을 시 빈 배열

    replies_list = await asyncio.gather(*tasks)

    merged = []
    for comment, replies in zip(comments, replies_list):
        updated = comment.get("updatedAt", "")
        updated_fmt = datetime.fromisoformat(updated.replace("T", " ").split("+")[0])
        merged.append(
            {
                "stock_code": comment.get("stockCode", "").replace("A", ""),
                "isin_code": comment.get("subjectId", ""),
                "stock_name": comment.get("topic", ""),
                "comment_id": comment.get("id"),
                "author_name": comment.get("author", {}).get("nickname", "unknown"),
                "date": updated_fmt.strftime("%Y-%m-%d %H:%M:%S"),
                "content": comment.get("message", ""),
                "likes_count": int(comment.get("likeCount", 0)),
                "dislikes_count": int(comment.get("dislikeCount", 0)),
                "comment_data": json.dumps(replies, ensure_ascii=False),
                "dt": datetime.fromisoformat(comment.get("updatedAt", "")).strftime(
                    "%Y-%m-%d"
                ),
            }
        )

    return merged


def upload_to_gcs(local_path: str, bucket_name: str, gcs_path: str) -> str:
    """로컬 파일을 GCS로 업로드 후 gs:// 또는 https URL 반환"""
    credentials_path = os.getenv("GCS_CREDENTIALS_PATH")
    storage_client = storage.Client.from_service_account_json(credentials_path)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(gcs_path)
    blob.upload_from_filename(local_path)
    return f"gs://{bucket_name}/{gcs_path}"


def upload_by_partition(df: pd.DataFrame, stock_code: str) -> list:
    """
    dt 컬럼 기준으로 날짜별 parquet 분리 저장 후 GCS 업로드
    - dt는 YYYY-MM-DD 형식
    - GCS 경로 예: gs://bucket/marketing/stock_discussion/dt=2025-11-09/{stock_code}_2025-11-09.parquet
    """
    bucket_name = os.getenv("GCS_BUCKET_NAME")
    if "dt" not in df.columns:
        raise ValueError("DataFrame에 dt 컬럼이 없습니다. (날짜 파티션 키 필요)")

    uploaded = []
    for date_value in df["dt"].unique():
        df_day = df[df["dt"] == date_value]

        # 로컬 저장 경로
        local_dir = LOCAL_SAVE_DIR / f"dt={date_value}"
        local_dir.mkdir(parents=True, exist_ok=True)
        filename = f"{stock_code}_{date_value}.parquet"
        local_path = local_dir / filename

        df_day.to_parquet(local_path, engine="pyarrow", index=False)

        # GCS 업로드 경로 (Hive-style partition)
        gcs_path = f"marketing/stock_discussion/dt={date_value}/{filename}"
        parquet_url = upload_to_gcs(str(local_path), bucket_name, gcs_path)
        uploaded.append(parquet_url)

        _log_and_print(f"[{stock_code}] {date_value} 업로드 완료 → {parquet_url}")

    return uploaded


async def main(body: dict):
    """댓글 + 대댓글 수집 후 날짜별로 GCS 업로드"""
    try:
        session = AsyncCurlClient()
        stock_code = body.get("stock_code")
        cookies = await fetch_cookies(body)

        # 1️⃣ 입력값에서 날짜 변환
        start_time = datetime.fromisoformat(body["start"].replace("/", "-")).astimezone(
            KST
        )
        end_time = datetime.fromisoformat(body["end"].replace("/", "-")).astimezone(KST)

        # 2️⃣ 댓글 수집
        comments = await fetch_comments_by_date(
            stock_code, cookies, session, start_time, end_time
        )
        if not comments:
            _log_and_print(f"[{stock_code}] 게시물 없음 → 업로드 스킵")
            return {
                "code": 204,
                "message": f"[{stock_code}] 해당 기간 내 게시물 없음",
                "stock_code": stock_code,
                "total_comments": 0,
            }

        # 3️⃣ 대댓글 병합
        comments_and_replies = await merge_comments_and_replies(
            comments, cookies, session
        )

        # 4️⃣ DataFrame 변환
        df = pd.DataFrame(comments_and_replies)

        # 5️⃣ 날짜별 parquet 저장 및 업로드
        parquet_urls = upload_by_partition(df, stock_code)

        return {
            "code": 200,
            "message": "댓글 + 대댓글 수집 및 업로드 완료",
            "stock_code": stock_code,
            "total_comments": len(df),
            "partitions": len(parquet_urls),
            "parquet_urls": parquet_urls,
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

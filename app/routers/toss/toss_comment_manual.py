from datetime import date
import json
import traceback
from app.common.request_function import AsyncCurlClient
from app.common.errors import TossError
from app.common.logger import toss_logger
from app.routers.toss.toss_cookies import fetch_cookies


async def fetch_first_post_comment(
    stock_code: str, cookies: dict, session: AsyncCurlClient
):
    url = "https://wts-cert-api.tossinvest.com/api/v3/comments"
    payload = {
        "subjectId": "KR7005930003",
        "subjectType": "STOCK",
        "commentSortType": "RECENT",  # POPULAR
    }
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
    response, status_code = await session.post(
        url, headers=headers, json_data=payload, cookies=cookies, body_type="JSON"
    )
    data = response.get("result", {}).get("comments", {}).get("body", [])
    return data


async def _next_post_comment(body: dict, cookies: dict, session: AsyncCurlClient):
    url = "https://wts-cert-api.tossinvest.com/api/v3/comments"
    payload = {
        "subjectId": "KR7000660001",
        "subjectType": "STOCK",
        "commentSortType": "POPULAR",
        "commentId": "",
    }
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
    response, status_code = await session.post(
        url, headers=headers, json_data=payload, cookies=cookies, body_type="JSON"
    )
    return response


async def main(body: dict):
    try:
        session = AsyncCurlClient()
        start = date.today()
        end = body.get("end", None)
        stock_code = body.get("stock_code")
        cookies = await fetch_cookies(body)
        posts = await fetch_first_post_comment(stock_code, cookies, session)
        return {"code": 200, "message": "success", "data": posts}
    except TossError as e:
        # YogiyoError를 dict로 변환하여 반환
        toss_logger.error(f"스크래핑 실패 (증권 종목: {body.get('id')}): {e}")
        return e.to_dict()

    except Exception as e:
        toss_logger.error(
            f"예상치 못한 에러 발생 (요청: {body}): {traceback.format_exc()}"
        )
        return {
            "code": 500,
            "message": f"알 수 없는 내부 서버 오류가 발생했습니다: {str(e)}",
        }

    finally:
        if session:
            try:
                await session.close()
            except Exception as e:
                toss_logger.error(f"세션 종료 중 에러: {e}")

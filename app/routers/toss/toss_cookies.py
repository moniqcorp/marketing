import json
import traceback
from app.common.request_function import AsyncBrowserClient, AsyncCurlClient
from app.common.errors import TossError
from app.common.logger import toss_logger


async def fetch_cookies(body: dict):
    try:
        async with AsyncBrowserClient(headless=False, browser_type="firefox") as client:
            cookies = await client.get_cookies(
                url="https://www.tossinvest.com/",
                wait_for_cookies="XSRF-TOKEN",
            )

            if "XSRF-TOKEN" not in cookies:
                raise TossError(
                    code=500,
                    message="토스 증권 쿠키 획득에 실패했습니다. 사이트 구조 변경을 확인해주세요.",
                )

            cookie = cookies.get("data")
            return cookie

    except TossError:
        raise
    except Exception as e:
        toss_logger.error(f"=== 로그인 처리 중 예외 발생 ===")
        toss_logger.error(f"에러 타입: {type(e).__name__}")
        toss_logger.error(f"에러 메시지: {str(e)}")
        toss_logger.error(f"상세 트레이스:\n{traceback.format_exc()}")
        raise TossError(code=500, message=f"로그인 처리 중 에러 발생: {str(e)}") from e


async def main(body: dict) -> None:
    cookies = await fetch_cookies(body)
    return {"code": 200, "message": "success", "data": cookies}

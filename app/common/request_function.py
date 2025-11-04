import random
import asyncio
import traceback
from typing import List, Literal
from app.common.logger import main_logger


from playwright.async_api import (
    async_playwright,
    Page,
    BrowserContext,
    Browser,
    Playwright,
    TimeoutError as PlaywrightTimeoutError,
)
from playwright_stealth import Stealth
from aiolimiter import AsyncLimiter
from curl_cffi.requests import AsyncSession

NO_COOKIE = "NO_COOKIE"
DEFAULT_CHROMIUM_ARGS = [
    "--disable-blink-features=AutomationControlled",
    "--disable-gpu",
    "--no-first-run",
    "--no-sandbox",  # 추가: Linux 서버 환경에서 필요
    "--disable-setuid-sandbox",  # 추가: Linux 서버 환경에서 필요
    "--disable-dev-shm-usage",  # 추가: 메모리 부족 방지
    "--lang=ko-KR,ko",
]
DEFAULT_FIREFOX_PREFS = {
    # 자동화 탐지 비활성화 (가장 중요)
    "dom.webdriver.enabled": False,
    "useAutomationExtension": False,
    # 플랫폼 위장
    "general.platform.override": "Win32",
    # Navigator 속성 조작 (Chromium의 AutomationControlled와 동일 효과)
    "privacy.resistFingerprinting": False,  # 핑거프린트 저항 끄기
    # WebGL 및 Canvas 핑거프린트
    "webgl.disabled": False,
    "privacy.trackingprotection.enabled": False,
    # 추가: 자동화 흔적 제거
    "dom.disable_beforeunload": False,
    "browser.tabs.remote.autostart": True,
    "browser.tabs.remote.autostart.2": True,
}
DEFAULT_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
BrowserType = Literal["chromium", "firefox"]


class AsyncBrowserClient:
    """
    브라우저 타입에 따라 최적화된 설정을 자동으로 적용하는 비동기 클라이언트.
    """

    def __init__(
        self,
        browser_type: BrowserType = "chromium",
        headless: bool = True,
        user_agent: str | None = None,
        init_script: str | None = None,
        viewport: dict | None = None,
        enable_stealth: bool = True,
        channel: str | None = None,
    ):
        self.browser_type = browser_type
        self.headless = headless
        self.user_agent = user_agent or DEFAULT_USER_AGENT
        self.init_script = init_script
        self.viewport = viewport or {"width": 1920, "height": 1080}
        self.enable_stealth = enable_stealth
        self.channel = channel
        self.chromium_args = DEFAULT_CHROMIUM_ARGS
        self.firefox_prefs = DEFAULT_FIREFOX_PREFS
        self._playwright: Playwright | None = None
        self._browser: Browser | None = None
        self._context: BrowserContext | None = None
        self._page: Page | None = None

    async def __aenter__(self):
        self._playwright = await async_playwright().start()
        if self.browser_type == "chromium":
            launcher = self._playwright.chromium
            self._browser = await launcher.launch(
                headless=self.headless,
                args=self.chromium_args,
                channel=self.channel,
            )
        elif self.browser_type == "firefox":
            launcher = self._playwright.firefox
            self._browser = await launcher.launch(
                headless=self.headless, firefox_user_prefs=self.firefox_prefs
            )
        self._context = await self._browser.new_context(
            user_agent=self.user_agent,
            viewport=self.viewport,
            locale="ko-KR",
            timezone_id="Asia/Seoul",
            permissions=["geolocation"],
        )

        if self.enable_stealth:
            self._stealth = Stealth(
                navigator_languages_override=("ko-KR", "ko", "en-US", "en"),
            )
            await self._stealth.apply_stealth_async(self._context)

        if self.init_script:
            await self._context.add_init_script(self.init_script)

        self._page = await self._context.new_page()

        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._page:
            await self._page.close()
        if self._context:
            await self._context.close()
        if self._browser:
            await self._browser.close()
        if self._playwright:
            await self._playwright.stop()

    async def click_for_duration(
        self,
        selector: str,
        duration_seconds: float = 10,
        delay_between_clicks: float = 0.1,
    ) -> int:
        """
        지정된 시간 동안 계속 클릭합니다.

        Args:
            selector: 클릭할 요소의 selector
            duration_seconds: 클릭 지속 시간 (초)
            delay_between_clicks: 클릭 간의 딜레이 (초)

        Returns:
            int: 총 클릭 횟수
        """
        import time

        if not self._page:
            raise Exception("페이지가 초기화되지 않았습니다.")

        start_time = time.time()
        click_count = 0

        main_logger.info(f"🔄 {duration_seconds}초 동안 '{selector}' 클릭 시작...")

        while (time.time() - start_time) < duration_seconds:
            try:
                await self._page.click(selector, timeout=500)
                click_count += 1
                elapsed = time.time() - start_time
                main_logger.debug(f"  클릭 #{click_count} ({elapsed:.2f}초)")
            except Exception as e:
                elapsed = time.time() - start_time
                main_logger.debug(
                    f"  ⚠️ 클릭 실패 ({elapsed:.2f}초): {type(e).__name__}"
                )

            await asyncio.sleep(delay_between_clicks)

        elapsed = time.time() - start_time
        main_logger.info(f"✅ 클릭 완료: 총 {click_count}회 ({elapsed:.2f}초)")
        return click_count

    async def get_cookies(
        self,
        url: str,
        id_selector: str | None = None,
        pw_selector: str | None = None,
        btn_selector: str | None = None,
        user_id: str | None = None,
        user_pw: str | None = None,
        wait_for_cookies: List[str] | str | None = None,
        wait_for_url: str | None = None,
        wait_timeout: int = 10000,
        btn_click_duration: float = 5,
    ) -> dict:
        """
        지정된 URL에 접속하여 쿠키를 반환합니다.
        로그인 정보가 없으면 로그인 단계를 건너뜁니다.
        """
        if not self._page or not self._context:
            error_msg = "클라이언트가 초기화되지 않았습니다."
            main_logger.error(error_msg)
            raise Exception(error_msg)

        try:
            # 페이지 이동
            await self._page.goto(url, timeout=30000, wait_until="domcontentloaded")

            # 로그인 (모든 필드가 제공된 경우에만)
            if id_selector and pw_selector and btn_selector and user_id and user_pw:
                # 입력 필드 대기
                await self._page.wait_for_selector(
                    id_selector, timeout=wait_timeout, state="visible"
                )

                # 랜덤 대기
                await self._page.wait_for_timeout(random.randint(200, 1000))

                # ID 입력
                await self._page.type(
                    id_selector, user_id, delay=random.uniform(80, 150)
                )
                await self._page.wait_for_timeout(random.randint(150, 400))

                # 비밀번호 입력
                await self._page.type(
                    pw_selector, user_pw, delay=random.uniform(100, 200)
                )
                await self._page.wait_for_timeout(random.randint(250, 500))

                # 버튼 클릭
                await self.click_for_duration(
                    btn_selector,
                    duration_seconds=btn_click_duration,
                    delay_between_clicks=0.3,
                )

            # 쿠키 대기
            if wait_for_cookies:
                cookies_to_wait = (
                    [wait_for_cookies]
                    if isinstance(wait_for_cookies, str)
                    else wait_for_cookies
                )
                js_conditions = [
                    f"document.cookie.includes('{cookie}')"
                    for cookie in cookies_to_wait
                ]
                wait_js_str = f"() => {' && '.join(js_conditions)}"
                await self._page.wait_for_function(wait_js_str, timeout=15000)
            elif wait_for_url:
                await self._page.wait_for_url(wait_for_url, timeout=15000)
            else:
                await self._page.wait_for_timeout(wait_timeout)

            # 네트워크 안정화 대기
            try:
                await self._page.wait_for_load_state("networkidle", timeout=5000)
            except:
                pass

            # 쿠키 추출
            cookies = await self._context.cookies()
            cookie_dict = {cookie["name"]: cookie["value"] for cookie in cookies}

            return cookie_dict

        except PlaywrightTimeoutError as e:
            main_logger.error(f"쿠키 획득 실패 (Timeout)")
            raise Exception(f"로그인 타임아웃: {str(e)}")

        except Exception as e:
            main_logger.error(f"쿠키 획득 중 에러")
            raise


class AsyncCurlClient:
    """
    curl_cffi를 사용하는 비동기 HTTP 클라이언트
    기존 AsyncRequestClient와 동일한 인터페이스 제공
    """

    def __init__(
        self,
        timeout: float = 30,
        impersonate: str = "firefox133",
    ):
        """
        Args:
            timeout (float): 요청 제한 시간(초 단위)
            impersonate (str): 브라우저 impersonate 버전
        """
        self.timeout = timeout
        self.impersonate = impersonate
        self._session: AsyncSession | None = None
        self._request_count = 0

    async def start(self):
        """세션 시작"""
        if self._session is None:
            self._session = AsyncSession(impersonate=self.impersonate)
        return self

    async def close(self):
        """세션 종료"""
        if self._session:
            await self._session.close()
            self._session = None

    async def _request(
        self,
        method: str,
        url: str,
        params=None,
        json_data=None,
        str_data=None,
        headers=None,
        body_type="TEXT",
        cookies=None,
    ):
        """HTTP 요청을 수행합니다."""
        if self._session is None:
            await self.start()

        try:
            response = await self._session.request(
                method=method,
                url=url,
                params=params,
                json=json_data,
                data=str_data,
                headers=headers,
                cookies=cookies,
                timeout=self.timeout,
                impersonate=self.impersonate,
            )

            self._request_count += 1

            if body_type.upper() == "TEXT":
                return response.text, response.status_code
            elif body_type.upper() == "JSON":
                return response.json(), response.status_code
            else:
                return {}, response.status_code

        except Exception as e:
            main_logger.error(
                f"ERROR in AsyncCurlClient: {traceback.format_exc()}, "
                f"url: {url}, method: {method}, params: {params}"
            )
            data = "" if body_type.upper() == "TEXT" else {}
            return data, 500

    async def get(
        self,
        url,
        params=None,
        headers=None,
        body_type="TEXT",
        cookies=None,
    ):
        """GET 요청"""
        return await self._request(
            "GET",
            url,
            params=params,
            headers=headers,
            body_type=body_type,
            cookies=cookies,
        )

    async def post(
        self,
        url,
        json_data=None,
        str_data=None,
        headers=None,
        body_type="TEXT",
        cookies=None,
    ):
        """POST 요청"""
        return await self._request(
            "POST",
            url,
            json_data=json_data,
            str_data=str_data,
            headers=headers,
            body_type=body_type,
            cookies=cookies,
        )

    async def patch(
        self,
        url,
        json_data=None,
        str_data=None,
        headers=None,
        body_type="TEXT",
        cookies=None,
    ):
        """PATCH 요청"""
        return await self._request(
            "PATCH",
            url,
            json_data=json_data,
            str_data=str_data,
            headers=headers,
            body_type=body_type,
            cookies=cookies,
        )

    async def delete(
        self,
        url,
        json_data=None,
        str_data=None,
        headers=None,
        body_type="TEXT",
        cookies=None,
    ):
        """DELETE 요청"""
        return await self._request(
            "DELETE",
            url,
            json_data=json_data,
            str_data=str_data,
            headers=headers,
            body_type=body_type,
            cookies=cookies,
        )

    async def put(
        self,
        url,
        json_data=None,
        str_data=None,
        headers=None,
        body_type="TEXT",
        cookies=None,
    ):
        """PUT 요청"""
        return await self._request(
            "PUT",
            url,
            json_data=json_data,
            str_data=str_data,
            headers=headers,
            body_type=body_type,
            cookies=cookies,
        )

    async def get_request_count(self):
        """현재까지의 총 요청 횟수 반환"""
        return self._request_count


class BrowserManager:
    """FastAPI 앱의 생명주기 동안 여러 브라우저 인스턴스를 관리하는 클래스"""

    playwright: Playwright = None
    # --- MODIFIED: 단일 브라우저에서 여러 브라우저를 담는 딕셔너리로 변경 ---
    browsers: dict[str, Browser] = {}

    async def startup(self):
        """애플리케이션 시작 시 Playwright를 시작하고 필요한 모든 브라우저를 실행합니다."""
        main_logger.info("🚀 Playwright와 브라우저들을 시작합니다...")
        self.playwright = await async_playwright().start()

        # --- MODIFIED: Chromium과 Firefox를 모두 실행하고 딕셔너리에 저장 ---
        self.browsers["chromium"] = await self.playwright.chromium.launch(
            headless=True,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-gpu",
                "--no-first-run",
            ],
        )
        main_logger.info("✅ Chromium 브라우저가 준비되었습니다.")

        self.browsers["firefox"] = await self.playwright.firefox.launch(
            headless=True,
            firefox_user_prefs={
                "dom.webdriver.enabled": False,
                "use.multiprocess": False,
            },
        )
        main_logger.info("✅ Firefox 브라우저가 준비되었습니다.")

        main_logger.info("👍 모든 브라우저가 준비되었습니다.")

    async def shutdown(self):
        """애플리케이션 종료 시 모든 브라우저를 닫고 Playwright를 중지합니다."""
        main_logger.info("🌙 모든 브라우저와 Playwright를 종료합니다...")
        for browser in self.browsers.values():
            await browser.close()

        if self.playwright:
            await self.playwright.stop()
        main_logger.info("✅ 종료되었습니다.")


browser_manager = BrowserManager()

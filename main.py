import logging
import time
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, Response

# from dotenv import load_dotenv

from app.common.logger import main_logger
from app.common.request_function import browser_manager

from app.routers.toss import toss_router
from app.routers.naver import naver_router

# load_dotenv()
os.makedirs("log", exist_ok=True)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    애플리케이션 시작 시 브라우저를 실행하고, 종료 시 안전하게 닫습니다.
    """
    main_logger.info("✅ 애플리케이션 시작...", extra={"route": "/startup"})
    await browser_manager.startup()  # 브라우저 실행 및 준비

    yield

    await browser_manager.shutdown()  # 브라우저 종료
    main_logger.info(
        "🛑 애플리케이션 종료. 로그를 flush합니다.", extra={"route": "/shutdown"}
    )
    logging.shutdown()


app = FastAPI(
    title="Stock Market Data Collection API",
    description="주식 시장 데이터 수집 서버",
    version="1.0.0",
    lifespan=lifespan,
)


@app.middleware("http")
async def log_requests_middleware(request: Request, call_next):
    """
    모든 HTTP 요청/응답을 구조화된 로그로 main_logger에 기록합니다.
    """
    start_time = time.time()
    main_logger.info(
        f"--> {request.method} from {request.client.host}",
        extra={"route": request.url.path},
    )

    try:
        response = await call_next(request)
        process_time = (time.time() - start_time) * 1000
        log_level = logging.INFO if response.status_code < 400 else logging.ERROR
        main_logger.log(
            log_level,
            f"<-- {response.status_code} after {process_time:.2f}ms",
            extra={"route": request.url.path},
        )
        return response

    except Exception as e:
        process_time = (time.time() - start_time) * 1000
        main_logger.error(
            f"<-- 500 Internal Server Error after {process_time:.2f}ms | Error: {e}",
            exc_info=True,
            extra={"route": request.url.path},
        )
        return Response("Internal Server Error", status_code=500)


@app.get("/")
def root():
    return {"message": "Welcome to Stock Market Data Collection API"}


app.include_router(toss_router.router)
app.include_router(naver_router.router)

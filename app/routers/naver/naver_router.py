from typing import Optional
from datetime import date, timedelta
from fastapi import APIRouter
from pydantic import BaseModel, Field
from app.routers.naver import naver_crawler_manual

router = APIRouter(prefix="/api/naver", tags=["naver"])


def get_default_start_date() -> str:
    return (date.today() - timedelta(days=7)).strftime("%Y-%m-%d")


def get_default_end_date() -> str:
    return date.today().strftime("%Y-%m-%d")


class NaverDiscussionBody(BaseModel):
    stock_code: str = Field(
        default="005930",
        description="네이버 종목 코드를 넣어주세요. (예: 005930)",
    )
    stock_name: Optional[str] = Field(
        default=None,
        description="종목 이름 (선택사항, 크롤링 시 자동 추출됩니다)",
    )
    start_date: str = Field(
        default_factory=get_default_start_date,
        description="수집 시작 날짜 (YYYY-MM-DD 형식, 기본: 7일 전)",
    )
    end_date: str = Field(
        default_factory=get_default_end_date,
        description="수집 종료 날짜 (YYYY-MM-DD 형식, 기본: 오늘)",
    )


class NaverBatchBody(BaseModel):
    start_date: str = Field(
        default_factory=get_default_start_date,
        description="수집 시작 날짜 (YYYY-MM-DD 형식, 기본: 7일 전)",
    )
    end_date: str = Field(
        default_factory=get_default_end_date,
        description="수집 종료 날짜 (YYYY-MM-DD 형식, 기본: 오늘)",
    )


@router.post(
    "/discussions/manual",
    summary="수동 토론 게시물 수집 (단일 종목)",
    description="특정 종목의 네이버 증권 토론 게시물을 수집하여 GCS에 저장합니다.",
)
async def collect_discussions_manual(body: NaverDiscussionBody):
    result = await naver_crawler_manual.main(body.model_dump())
    return result


@router.post(
    "/discussions/batch",
    summary="배치 토론 게시물 수집 (전체 종목)",
    description="BigQuery stocks 테이블의 전체 종목에 대해 네이버 증권 토론 게시물을 수집합니다.",
)
async def collect_discussions_batch(body: NaverBatchBody):
    result = await naver_crawler_manual.main_batch(body.model_dump())
    return result

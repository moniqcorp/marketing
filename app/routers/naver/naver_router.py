from typing import Optional
from fastapi import APIRouter
from pydantic import BaseModel, Field
from app.routers.naver import naver_crawler_manual

router = APIRouter(prefix="/api/naver", tags=["naver"])


class NaverDiscussionBody(BaseModel):
    stock_code: str = Field(
        default="005930",
        description="네이버 종목 코드를 넣어주세요. (예: 005930)",
    )
    stock_name: Optional[str] = Field(
        default=None,
        description="종목 이름 (선택사항, 크롤링 시 자동 추출됩니다)",
    )
    max_posts: int = Field(
        default=50,
        description="수집할 최대 게시물 수 (기본: 50)",
    )


@router.post(
    "/discussions/manual",
    summary="수동 토론 게시물 수집 (비정기)",
    description="특정 종목의 네이버 증권 토론 게시물을 수집하여 GCS에 저장합니다.",
)
async def collect_discussions_manual(body: NaverDiscussionBody):
    result = await naver_crawler_manual.main(body.model_dump())
    return result

from datetime import date, datetime
from typing import Optional
from fastapi import APIRouter
from pydantic import BaseModel, field_validator
from app.routers.toss import toss_cookies

router = APIRouter(prefix="/api/toss", tags=["toss"])


class TossPostCommentBody(BaseModel):
    start: str
    end: str


@router.post(
    "/post-comment",
    summary="post-comment",
    description="토스 증권의 댓글 데이터를 수집 하는 API",
)
async def get_order(body: TossPostCommentBody):
    """
    # 요기요(Yogiyo) > 주문 조회
    """
    result = await toss_cookies.main(body.model_dump())
    return result

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
async def get_comments(body: TossPostCommentBody):
    """
    # 토스 증권(toss) > 증권별 게시물 댓글 조회
    """
    result = await toss_cookies.main(body.model_dump())
    return result

from datetime import date, datetime
from typing import Optional
from fastapi import APIRouter
from pydantic import BaseModel, field_validator
from app.routers.toss import toss_comment_manual

router = APIRouter(prefix="/api/toss", tags=["toss"])


class TossPostCommentBody(BaseModel):
    start: Optional[str]
    end: Optional[str]
    stock_code: str


@router.post(
    "/post-comments/manual",
    summary="수동 댓글 수집 (비정기)",
    description="특정 기간(예: 1년치)을 직접 지정하여 토스 증권 댓글 데이터를 수집합니다. - 현재 시작으로부터 1년전으로 고정",
)
async def collect_comments_manual(body: TossPostCommentBody):
    result = await toss_comment_manual.main(body.model_dump())
    return result


@router.post(
    "/post-comments/scheduled",
    summary="정기 댓글 수집 (배치)",
    description="매일 또는 매주 자동 실행되는 토스 증권 댓글 데이터 정기 수집 API입니다.",
)
async def collect_comments_scheduled(body: TossPostCommentBody):
    result = await toss_comment_manual.main(body.model_dump())
    return result

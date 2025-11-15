import json
from datetime import date, datetime
from typing import Optional
from fastapi import APIRouter, BackgroundTasks
from pydantic import BaseModel, Field, field_validator
from app.routers.toss import toss_comment_manual

router = APIRouter(prefix="/api/toss", tags=["toss"])


class TossPostCommentBody(BaseModel):
    start: Optional[str] = Field(
        default="2025/11/05T00:00:00",
        description="배치 수집인 경우에만 넣어주세요. 1년 수집인 경우에는 비워주세요.",
    )
    end: Optional[str] = Field(
        default="2025/11/05T04:00:00",
        description="배치 수집인 경우에만 넣어주세요. 1년 수집인 경우에는 비워주세요.",
    )
    stock_code: str = Field(
        default="KR7005930003",
        description="ISIN 종목 코드를 넣어주세요.",
    )
    corp_name: str = Field(
        default="광현컴패니",
        description="회사 이름을 넣어주세요.",
    )


@router.post(
    "/post-comments/manual",
    summary="수동 댓글 수집 (비정기)",
    description="특정 기간(예: 1년치)을 직접 지정하여 토스 증권 댓글 데이터를 수집합니다.",
)
async def collect_comments_manual(body: TossPostCommentBody):
    result = await toss_comment_manual.main(body.model_dump())
    return result

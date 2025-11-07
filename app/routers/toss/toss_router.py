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
    description="특정 기간(예: 1년치)을 직접 지정하여 토스 증권 댓글 데이터를 수집합니다. - 현재 시작으로부터 1년전으로 고정",
)
async def collect_comments_manual(
    body: TossPostCommentBody, background_tasks: BackgroundTasks  # ← 추가
):
    result = await toss_comment_manual.main(
        body.model_dump(), background_tasks
    )  # ← background_tasks 전달
    return result


@router.get(
    "/post-comments/status/latest/{stock_code}",
    summary="최신 수집 상태 조회",
    description="가장 최근 수집의 상태를 조회합니다.",
)
async def get_latest_collection_status(stock_code: str):
    """최신 수집 상태 조회"""
    from pathlib import Path
    import glob
    import os

    # 패턴: comments_[stock_code]_*.status.json
    status_files = glob.glob(f"comments_{stock_code}_*.json")

    if not status_files:
        return {"code": 404, "message": "수집 상태가 없습니다"}

    # 파일 수정 시간 기준으로 정렬 (가장 최근이 first)
    latest_file = max(status_files, key=os.path.getmtime)

    with open(latest_file, "r", encoding="utf-8") as f:
        status_data = json.load(f)

    return {"code": 200, "data": status_data}


# @router.post(
#     "/post-comments/scheduled",
#     summary="정기 댓글 수집 (배치)",
#     description="매일 또는 매주 자동 실행되는 토스 증권 댓글 데이터 정기 수집 API입니다.",
# )
# async def collect_comments_scheduled(body: TossPostCommentBody):
#     result = await toss_comment_scheduled.main(body.model_dump())
#     return result

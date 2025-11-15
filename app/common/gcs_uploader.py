"""
GCS Uploader Module
공통 GCS 업로드 로직을 제공하는 모듈
Toss, Naver 등 여러 서비스에서 재사용 가능
"""

import os
from pathlib import Path
from google.cloud import storage
import pandas as pd
import logging

logger = logging.getLogger(__name__)


def upload_to_gcs(local_path: str, bucket_name: str, gcs_path: str) -> str:
    """
    로컬 파일을 GCS로 업로드 후 gs:// URL 반환

    Args:
        local_path: 업로드할 로컬 파일 경로
        bucket_name: GCS 버킷 이름
        gcs_path: GCS 내 저장 경로

    Returns:
        str: gs://{bucket_name}/{gcs_path} 형식의 URL
    """
    credentials_path = os.getenv("GCS_CREDENTIALS_PATH")
    storage_client = storage.Client.from_service_account_json(credentials_path)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(gcs_path)
    blob.upload_from_filename(local_path)
    return f"gs://{bucket_name}/{gcs_path}"


def upload_by_partition(
    df: pd.DataFrame,
    identifier: str,
    base_gcs_path: str,
    local_save_dir: Path,
    log_func=None,
) -> list:
    """
    dt 컬럼 기준으로 날짜별 parquet 분리 저장 후 GCS 업로드

    Args:
        df: 업로드할 DataFrame (dt 컬럼 필수)
        identifier: 파일명에 사용할 식별자 (예: stock_code, isin_code)
        base_gcs_path: GCS 기본 경로 (예: "marketing/stock_discussion")
        local_save_dir: 로컬 임시 저장 디렉토리
        log_func: 로그 함수 (기본: logger.info)

    Returns:
        list: 업로드된 GCS URL 리스트

    Example:
        >>> df = pd.DataFrame({
        ...     'stock_code': ['005930', '005930'],
        ...     'dt': ['2025-11-15', '2025-11-15'],
        ...     'content': ['text1', 'text2']
        ... })
        >>> upload_by_partition(
        ...     df,
        ...     identifier='005930',
        ...     base_gcs_path='marketing/stock_discussion',
        ...     local_save_dir=Path('./temp')
        ... )
        ['gs://bucket/marketing/stock_discussion/dt=2025-11-15/005930_2025-11-15.parquet']
    """
    if log_func is None:
        log_func = logger.info

    bucket_name = os.getenv("GCS_BUCKET_NAME")

    if "dt" not in df.columns:
        raise ValueError("DataFrame에 dt 컬럼이 없습니다. (날짜 파티션 키 필요)")

    uploaded = []

    for date_value in df["dt"].unique():
        df_day = df[df["dt"] == date_value]

        # 로컬 저장 경로
        local_dir = local_save_dir / f"dt={date_value}"
        local_dir.mkdir(parents=True, exist_ok=True)
        filename = f"{identifier}_{date_value}.parquet"
        local_path = local_dir / filename

        df_day.to_parquet(local_path, engine="pyarrow", index=False)

        # GCS 업로드 경로 (Hive-style partition)
        gcs_path = f"{base_gcs_path}/dt={date_value}/{filename}"
        parquet_url = upload_to_gcs(str(local_path), bucket_name, gcs_path)
        uploaded.append(parquet_url)

        log_func(f"[{identifier}] {date_value} 업로드 완료 → {parquet_url}")

    return uploaded

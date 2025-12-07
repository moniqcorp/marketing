import os
from typing import List, Dict
from google.cloud import bigquery
from dotenv import load_dotenv

load_dotenv()


def get_stock_list() -> List[Dict[str, str]]:
    """
    BigQuery stocks 테이블에서 종목 목록을 조회합니다.

    Returns:
        List[Dict]: [{"stock_code": "005930", "stock_name": "삼성전자", "isin_code": "KR7005930003"}, ...]
    """
    credentials_path = os.getenv("GCS_CREDENTIALS_PATH")
    project_id = os.getenv("GCP_PROJECT_ID")
    dataset_id = os.getenv("BQ_DATASET_ID")
    table_id = os.getenv("BQ_STOCK_TABLE_ID")
    limit = int(os.getenv("BQ_LIMIT", "0"))

    client = bigquery.Client.from_service_account_json(credentials_path)

    query = f"""
        SELECT
            stock_code,
            stock_name,
            isin_code
        FROM `{project_id}.{dataset_id}.{table_id}`
        WHERE stock_code IS NOT NULL
        AND target_stock = 1
    """

    if limit > 0:
        query += f" LIMIT {limit}"

    result = client.query(query).result()

    stocks = []
    for row in result:
        stocks.append({
            "stock_code": row.stock_code,
            "stock_name": row.stock_name,
            "isin_code": row.isin_code,
        })

    return stocks


def get_stock_by_code(stock_code: str) -> Dict[str, str] | None:
    """
    특정 종목 코드로 종목 정보를 조회합니다.

    Args:
        stock_code: 종목 코드 (예: "005930")

    Returns:
        Dict: {"stock_code": "005930", "stock_name": "삼성전자", "isin_code": "KR7005930003"}
        또는 None (종목이 없는 경우)
    """
    credentials_path = os.getenv("GCS_CREDENTIALS_PATH")
    project_id = os.getenv("GCP_PROJECT_ID")
    dataset_id = os.getenv("BQ_DATASET_ID")
    table_id = os.getenv("BQ_STOCK_TABLE_ID")

    client = bigquery.Client.from_service_account_json(credentials_path)

    query = f"""
        SELECT
            stock_code,
            stock_name,
            isin_code
        FROM `{project_id}.{dataset_id}.{table_id}`
        WHERE stock_code = @stock_code
        LIMIT 1
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("stock_code", "STRING", stock_code)
        ]
    )

    result = client.query(query, job_config=job_config).result()

    for row in result:
        return {
            "stock_code": row.stock_code,
            "stock_name": row.stock_name,
            "isin_code": row.isin_code,
        }

    return None

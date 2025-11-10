"""
Stock data loader from BigQuery or CSV
"""

import os
import logging
import pandas as pd
from .client import BigQueryClient

logger = logging.getLogger(__name__)


def load_stocks_from_bigquery(dataset_id, table_id, project_id=None, credentials_path=None,
                               code_column='stock_code', name_column='stock_name',
                               filters=None, limit=None):
    """
    Load stock codes and names from BigQuery

    Args:
        dataset_id: BigQuery dataset ID
        table_id: Table name containing stock codes
        project_id: GCP project ID (optional, reads from env if None)
        credentials_path: Path to service account JSON (optional)
        code_column: Column name for stock code (default: 'stock_code')
        name_column: Column name for stock name (default: 'stock_name')
        filters: Optional SQL WHERE clause (e.g., "market_type = 'KOSPI'")
        limit: Optional limit on number of stocks (for testing)

    Returns:
        tuple: (list of stock codes, dict of {code: name})
    """
    try:
        client = BigQueryClient(project_id, credentials_path)

        # Build query
        sql = f"""
        SELECT DISTINCT {code_column} as stock_code, {name_column} as stock_name
        FROM `{client.project_id}.{dataset_id}.{table_id}`
        """

        if filters:
            sql += f" WHERE {filters}"

        sql += f" ORDER BY {code_column}"

        if limit and limit > 0:
            sql += f" LIMIT {limit}"

        # Execute query
        logger.info(f"Loading stocks from BigQuery: {dataset_id}.{table_id}")
        if limit:
            logger.info(f"  Limit: {limit} stocks")

        results = client.query(sql)

        stock_codes = []
        stock_name_map = {}

        for row in results:
            code = row['stock_code']
            name = row.get('stock_name', '')
            stock_codes.append(code)
            stock_name_map[code] = name

        logger.info(f"Loaded {len(stock_codes)} stocks from BigQuery")

        return stock_codes, stock_name_map

    except Exception as e:
        logger.error(f"Failed to load stocks from BigQuery: {e}")
        raise


def load_stocks_from_csv(filename='Market Data.csv', code_column='isu_cd', name_column='isu_nm'):
    """
    Load stock codes and names from CSV file

    Args:
        filename: Path to CSV file
        code_column: Column name for stock code (default: 'isu_cd')
        name_column: Column name for stock name (default: 'isu_nm')

    Returns:
        tuple: (list of stock codes, dict of {code: name})
    """
    try:
        df = pd.read_csv(filename)

        # Pad stock codes with leading zeros to 6 digits
        stock_codes = df[code_column].astype(str).str.zfill(6).tolist()

        # Create name mapping
        stock_name_map = dict(zip(
            df[code_column].astype(str).str.zfill(6),
            df[name_column]
        ))

        logger.info(f"Loaded {len(stock_codes)} stocks from {filename}")
        return stock_codes, stock_name_map

    except FileNotFoundError:
        logger.error(f"CSV file not found: {filename}")
        raise
    except KeyError as e:
        logger.error(f"Column not found in {filename}: {e}")
        raise
    except Exception as e:
        logger.error(f"Error reading CSV file: {e}")
        raise


def load_stocks(source='csv', **kwargs):
    """
    Load stock codes and names from specified source

    Args:
        source: 'csv' or 'bigquery'
        **kwargs: Additional arguments for specific loader

    CSV kwargs:
        - filename: CSV file path (default: 'Market Data.csv')
        - code_column: Stock code column name (default: 'isu_cd')
        - name_column: Stock name column name (default: 'isu_nm')

    BigQuery kwargs:
        - dataset_id: BigQuery dataset ID (required)
        - table_id: Table name (required)
        - project_id: GCP project ID (optional)
        - credentials_path: Service account JSON path (optional)
        - code_column: Stock code column name (default: 'stock_code')
        - name_column: Stock name column name (default: 'stock_name')
        - filters: SQL WHERE clause (optional)
        - limit: Limit number of stocks (optional, for testing)

    Returns:
        tuple: (list of stock codes, dict of {code: name})
    """
    if source == 'csv':
        return load_stocks_from_csv(
            filename=kwargs.get('filename', 'Market Data.csv'),
            code_column=kwargs.get('code_column', 'isu_cd'),
            name_column=kwargs.get('name_column', 'isu_nm')
        )
    elif source == 'bigquery':
        if 'dataset_id' not in kwargs or 'table_id' not in kwargs:
            raise ValueError("dataset_id and table_id are required for BigQuery source")

        return load_stocks_from_bigquery(
            dataset_id=kwargs['dataset_id'],
            table_id=kwargs['table_id'],
            project_id=kwargs.get('project_id'),
            credentials_path=kwargs.get('credentials_path'),
            code_column=kwargs.get('code_column', 'stock_code'),
            name_column=kwargs.get('name_column', 'stock_name'),
            filters=kwargs.get('filters'),
            limit=kwargs.get('limit')
        )
    else:
        raise ValueError(f"Unknown source: {source}. Must be 'csv' or 'bigquery'")

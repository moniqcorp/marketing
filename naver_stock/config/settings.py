"""
Configuration settings loader
"""

import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


class Settings:
    """Application settings loaded from environment variables"""

    # GCS Configuration
    GCS_BUCKET_NAME = os.getenv('GCS_BUCKET_NAME')
    GCS_CREDENTIALS_PATH = os.getenv('GCS_CREDENTIALS_PATH')
    GCS_PREFIX = os.getenv('GCS_PREFIX', '')

    # BigQuery Configuration
    GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID')
    BQ_DATASET_ID = os.getenv('BQ_DATASET_ID')
    BQ_STOCK_TABLE_ID = os.getenv('BQ_STOCK_TABLE_ID')
    BQ_STOCK_CODE_COLUMN = os.getenv('BQ_STOCK_CODE_COLUMN', 'stock_code')
    BQ_STOCK_NAME_COLUMN = os.getenv('BQ_STOCK_NAME_COLUMN', 'stock_name')
    BQ_STOCK_ISIN_COLUMN = os.getenv('BQ_STOCK_ISIN_COLUMN', 'isin_code')
    BQ_LIMIT = int(os.getenv('BQ_LIMIT', '0')) or None

    # Stock Data Source
    STOCK_SOURCE = os.getenv('STOCK_SOURCE', 'csv')  # 'csv' or 'bigquery'
    STOCK_CSV_FILE = os.getenv('STOCK_CSV_FILE', 'Market Data_top10.csv')
    CSV_ISIN_COLUMN = os.getenv('CSV_ISIN_COLUMN')

    # Crawler Configuration
    REQUEST_DELAY = float(os.getenv('REQUEST_DELAY', '0.3'))
    MAX_RETRIES = int(os.getenv('MAX_RETRIES', '3'))
    MAX_CONCURRENT_REQUESTS = int(os.getenv('MAX_CONCURRENT_REQUESTS', '10'))

    @classmethod
    def validate(cls):
        """
        Validate required settings

        Raises:
            ValueError: If required settings are missing
        """
        errors = []

        # Validate GCS settings
        if not cls.GCS_BUCKET_NAME:
            errors.append("GCS_BUCKET_NAME is required")

        # Validate BigQuery settings (if using BigQuery as source)
        if cls.STOCK_SOURCE == 'bigquery':
            if not cls.GCP_PROJECT_ID:
                errors.append("GCP_PROJECT_ID is required for BigQuery source")
            if not cls.BQ_DATASET_ID:
                errors.append("BQ_DATASET_ID is required for BigQuery source")
            if not cls.BQ_STOCK_TABLE_ID:
                errors.append("BQ_STOCK_TABLE_ID is required for BigQuery source")

        # Validate CSV settings (if using CSV as source)
        if cls.STOCK_SOURCE == 'csv':
            if not os.path.exists(cls.STOCK_CSV_FILE):
                errors.append(f"Stock CSV file not found: {cls.STOCK_CSV_FILE}")

        if errors:
            raise ValueError("Configuration errors:\n" + "\n".join(f"  - {e}" for e in errors))

    @classmethod
    def get_stock_loader_config(cls):
        """
        Get configuration for stock loader

        Returns:
            dict: Configuration dictionary
        """
        if cls.STOCK_SOURCE == 'bigquery':
            return {
                'source': 'bigquery',
                'dataset_id': cls.BQ_DATASET_ID,
                'table_id': cls.BQ_STOCK_TABLE_ID,
                'project_id': cls.GCP_PROJECT_ID,
                'credentials_path': cls.GCS_CREDENTIALS_PATH,
                'code_column': cls.BQ_STOCK_CODE_COLUMN,
                'name_column': cls.BQ_STOCK_NAME_COLUMN,
                'isin_column': cls.BQ_STOCK_ISIN_COLUMN,
                'filters': 'target_stock = 1',
                'limit': cls.BQ_LIMIT
            }
        else:
            return {
                'source': 'csv',
                'filename': cls.STOCK_CSV_FILE,
                'code_column': 'isu_cd',
                'name_column': 'isu_nm',
                'isin_column': cls.CSV_ISIN_COLUMN
            }


# Create singleton instance
settings = Settings()

"""
GCS Writer for streaming Parquet files
Handles date-based partitioning and batch numbering
"""

import os
import logging
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import storage as gcs_storage
from datetime import datetime, timedelta
from io import BytesIO

logger = logging.getLogger(__name__)


class GCSParquetWriter:
    """Streams data to GCS in Parquet format with date-based partitioning"""

    def __init__(self, bucket_name, credentials_path=None, gcs_prefix=''):
        """
        Initialize GCS writer

        Args:
            bucket_name: GCS bucket name
            credentials_path: Path to service account JSON (optional, uses default credentials if None)
            gcs_prefix: Prefix path in GCS bucket (e.g., 'marketing/stock_discussion')
        """
        self.bucket_name = bucket_name
        self.gcs_prefix = gcs_prefix.rstrip('/') if gcs_prefix else ''

        # Initialize GCS client
        if credentials_path and os.path.exists(credentials_path):
            self.client = gcs_storage.Client.from_service_account_json(credentials_path)
            logger.info(f"GCS client initialized with credentials: {credentials_path}")
        else:
            self.client = gcs_storage.Client()
            logger.info("GCS client initialized with default credentials")

        self.bucket = self.client.bucket(bucket_name)

        # Track batch numbers per date
        self.batch_counters = {}  # {date_key: batch_number}

    def _get_next_batch_number(self, date_key):
        """Get next batch number for a date"""
        if date_key not in self.batch_counters:
            self.batch_counters[date_key] = 1
        else:
            self.batch_counters[date_key] += 1
        return self.batch_counters[date_key]

    def save_batch(self, date_key, data, source='naver', stock_code=None, schema=None):
        """
        Save a batch of data to GCS with Hive-style partitioning

        Args:
            date_key: Date in YYYYMMDD format (e.g., '20251101')
            data: List of dictionaries (discussion data)
            source: Data source name ('naver' or 'toss')
            stock_code: Stock code for filename (optional, extracted from data if None)
            schema: PyArrow schema (optional, auto-detected if None)

        Returns:
            str: GCS file path
        """
        if not data:
            logger.warning("No data to save")
            return None

        try:
            # Convert to DataFrame
            df = pd.DataFrame(data)

            # Extract stock_code if not provided
            if stock_code is None and 'stock_code' in df.columns:
                stock_code = df['stock_code'].iloc[0]

            # Add partition column (dt = YYYY-MM-DD format for BigQuery)
            if len(date_key) == 8:  # YYYYMMDD
                dt_value = f"{date_key[:4]}-{date_key[4:6]}-{date_key[6:8]}"
            else:
                dt_value = date_key

            df['dt'] = dt_value

            # Use provided schema or create default one
            if schema is None:
                schema = self._create_default_schema()

            # Convert DataFrame to PyArrow Table with explicit schema
            table = pa.Table.from_pandas(df, schema=schema)

            # Convert to Parquet in memory
            buffer = BytesIO()
            pq.write_table(table, buffer, compression='snappy')
            buffer.seek(0)

            # Generate file path with Hive partitioning
            # Format: [prefix/]dt=YYYY-MM-DD/{stock_code}_{source}_batchN.parquet
            batch_key = f"{date_key}_{stock_code}" if stock_code else date_key
            batch_num = self._get_next_batch_number(batch_key)

            if stock_code:
                filename = f"{stock_code}_{source}_batch{batch_num}.parquet"
            else:
                filename = f"{source}_batch{batch_num}.parquet"

            if self.gcs_prefix:
                blob_path = f"{self.gcs_prefix}/dt={dt_value}/{filename}"
            else:
                blob_path = f"dt={dt_value}/{filename}"

            # Upload to GCS
            blob = self.bucket.blob(blob_path)
            blob.upload_from_file(buffer, content_type='application/octet-stream')

            logger.info(f"✅ Saved {len(data)} rows to gs://{self.bucket_name}/{blob_path}")
            return f"gs://{self.bucket_name}/{blob_path}"

        except Exception as e:
            logger.error(f"Failed to save batch to GCS: {e}")
            raise

    def _create_default_schema(self):
        """Create default PyArrow schema for stock discussion data"""
        return pa.schema([
            ('stock_code', pa.string()),
            ('stock_name', pa.string()),
            ('isin_code', pa.string()),
            ('comment_id', pa.int64()),
            ('author_name', pa.string()),
            ('date', pa.string()),
            ('content', pa.string()),
            ('likes_count', pa.int64()),
            ('dislikes_count', pa.int64()),
            ('comment_data', pa.string()),
            ('source', pa.string()),
            ('dt', pa.string())
        ])

    def save_batch_stream(self, date_key, data, source='naver', max_rows_per_file=1000, schema=None):
        """
        Save data in streaming fashion (splits into multiple files if needed)

        Args:
            date_key: Date in YYYYMMDD format
            data: List of dictionaries
            source: Data source name
            max_rows_per_file: Maximum rows per file
            schema: PyArrow schema (optional)

        Returns:
            list: List of GCS file paths
        """
        if not data:
            return []

        saved_files = []

        # Split data into chunks
        for i in range(0, len(data), max_rows_per_file):
            chunk = data[i:i + max_rows_per_file]
            file_path = self.save_batch(date_key, chunk, source, schema=schema)
            if file_path:
                saved_files.append(file_path)

        return saved_files

    def list_files(self, date_key=None, source=None):
        """
        List files in GCS

        Args:
            date_key: Filter by date (optional)
            source: Filter by source (optional)

        Returns:
            list: List of blob names
        """
        prefix = self.gcs_prefix + '/' if self.gcs_prefix else ''
        if date_key:
            prefix += f"dt={date_key}/"
            if source:
                prefix += f"{source}_"

        blobs = self.bucket.list_blobs(prefix=prefix)
        return [blob.name for blob in blobs]

    def delete_files_by_stock(self, stock_code, start_date, end_date, source='naver'):
        """
        Delete all files for a specific stock within a date range

        Args:
            stock_code: Stock code (e.g., '005930')
            start_date: Start date (datetime object)
            end_date: End date (datetime object)
            source: Data source name (e.g., 'naver', 'toss')

        Returns:
            int: Number of files deleted
        """
        deleted_count = 0
        current_date = start_date

        while current_date <= end_date:
            date_key = current_date.strftime('%Y-%m-%d')
            prefix = self.gcs_prefix + '/' if self.gcs_prefix else ''
            prefix += f"dt={date_key}/{stock_code}_{source}_"

            # List and delete blobs with this prefix
            blobs = list(self.bucket.list_blobs(prefix=prefix))

            if blobs:
                for blob in blobs:
                    blob.delete()
                    logger.debug(f"Deleted: {blob.name}")
                    deleted_count += 1

            current_date += timedelta(days=1)

        if deleted_count > 0:
            logger.info(f"🗑️  Deleted {deleted_count} existing files for stock {stock_code}")

        return deleted_count

    def delete_files_by_date_range(self, start_date, end_date, source='naver'):
        """
        Delete all files for a specific source within a date range

        Args:
            start_date: Start date (datetime object)
            end_date: End date (datetime object)
            source: Data source name (e.g., 'naver', 'toss')

        Returns:
            int: Number of files deleted
        """
        deleted_count = 0
        current_date = start_date

        while current_date <= end_date:
            date_key = current_date.strftime('%Y-%m-%d')
            prefix = self.gcs_prefix + '/' if self.gcs_prefix else ''
            prefix += f"dt={date_key}/{source}_"

            logger.info(f"Checking for existing files: {prefix}*")

            # List and delete blobs with this prefix
            blobs = list(self.bucket.list_blobs(prefix=prefix))

            if blobs:
                logger.info(f"  Found {len(blobs)} files to delete")
                for blob in blobs:
                    blob.delete()
                    logger.info(f"  🗑️  Deleted: {blob.name}")
                    deleted_count += 1
            else:
                logger.info(f"  No existing files found")

            current_date += timedelta(days=1)

        if deleted_count > 0:
            logger.info(f"✅ Total deleted: {deleted_count} files")

        return deleted_count

"""
GCS Writer for streaming Parquet files
Handles date-based partitioning and batch numbering
"""

import os
import logging
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import storage
from datetime import datetime
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
            self.client = storage.Client.from_service_account_json(credentials_path)
            logger.info(f"GCS client initialized with credentials: {credentials_path}")
        else:
            self.client = storage.Client()
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

    def save_batch(self, date_key, data, source='naver', stock_code=None):
        """
        Save a batch of data to GCS with Hive-style partitioning

        Args:
            date_key: Date in YYYYMMDD format (e.g., '20251101')
            data: List of dictionaries (discussion data)
            source: Data source name ('naver' or 'toss')
            stock_code: Stock code for filename (optional, extracted from data if None)

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

            # Define explicit PyArrow schema for BigQuery compatibility
            schema = pa.schema([
                ('stock_code', pa.string()),
                ('stock_name', pa.string()),
                ('isin_code', pa.string()),  # ISIN code
                ('comment_id', pa.int64()),
                ('author_name', pa.string()),
                ('date', pa.string()),  # Store as string for BigQuery compatibility
                ('content', pa.string()),
                ('likes_count', pa.int64()),
                ('dislikes_count', pa.int64()),
                ('comment_data', pa.string()),  # JSON stored as string
                ('source', pa.string()),  # Data source (e.g., 'naver', 'toss')
                ('dt', pa.string())  # Partition column
            ])

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

    def save_batch_stream(self, date_key, data, source='naver', max_rows_per_file=1000):
        """
        Save data in streaming fashion (splits into multiple files if needed)

        Args:
            date_key: Date in YYYYMMDD format
            data: List of dictionaries
            source: Data source name
            max_rows_per_file: Maximum rows per file

        Returns:
            list: List of GCS file paths
        """
        if not data:
            return []

        saved_files = []

        # Split data into chunks
        for i in range(0, len(data), max_rows_per_file):
            chunk = data[i:i + max_rows_per_file]
            file_path = self.save_batch(date_key, chunk, source)
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
        from datetime import timedelta

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
        from datetime import timedelta

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


class DatePartitionedBuffer:
    """
    Buffer that accumulates data by date and stock_code, auto-flushes to GCS
    """

    def __init__(self, gcs_writer, buffer_size=1000, source='naver'):
        """
        Initialize buffer

        Args:
            gcs_writer: GCSParquetWriter instance
            buffer_size: Number of rows before auto-flush (per stock)
            source: Data source name
        """
        self.gcs_writer = gcs_writer
        self.buffer_size = buffer_size
        self.source = source

        # Date and stock-based buffers: {(date_key, stock_code): [data]}
        self.buffers = {}

        # Track which stock+date combinations have been deleted: {(stock_code, date_key)}
        self.deleted_stocks = set()

        # Track all saved files (including auto-flushed files)
        self.all_saved_files = []

    def add(self, post_date, data):
        """
        Add data to buffer

        Args:
            post_date: datetime object or YYYYMMDD string
            data: Dictionary of post data (must contain 'stock_code')
        """
        # Convert datetime to date_key
        if isinstance(post_date, datetime):
            date_key = post_date.strftime('%Y%m%d')
        else:
            date_key = post_date

        # Get stock_code from data
        stock_code = data.get('stock_code')
        if not stock_code:
            logger.warning("Data missing stock_code, skipping")
            return

        # Create buffer key
        buffer_key = (date_key, stock_code)

        # Initialize buffer if needed
        if buffer_key not in self.buffers:
            self.buffers[buffer_key] = []

        # Add data
        self.buffers[buffer_key].append(data)

        # Auto-flush if buffer is full
        if len(self.buffers[buffer_key]) >= self.buffer_size:
            saved = self.flush(date_key, stock_code)
            self.all_saved_files.extend(saved)

    def flush(self, date_key=None, stock_code=None):
        """
        Flush buffer to GCS (deletes existing files for stock+date before saving)

        Args:
            date_key: Specific date to flush (None = flush all)
            stock_code: Specific stock to flush (None = flush all for date)

        Returns:
            list: List of saved file paths (only from this flush, not cumulative)
        """
        saved_files = []

        if date_key and stock_code:
            # Flush specific date + stock
            buffer_key = (date_key, stock_code)
            if buffer_key in self.buffers and self.buffers[buffer_key]:
                # Delete existing files for this stock+date combination only
                delete_key = (stock_code, date_key)
                if delete_key not in self.deleted_stocks:
                    self._delete_files_for_stock_date(stock_code, date_key)
                    self.deleted_stocks.add(delete_key)

                # Save new data
                file_path = self.gcs_writer.save_batch(
                    date_key, self.buffers[buffer_key], self.source, stock_code
                )
                if file_path:
                    saved_files.append(file_path)
                    self.all_saved_files.append(file_path)  # Track all files
                self.buffers[buffer_key].clear()
        elif date_key:
            # Flush all stocks for specific date
            keys_to_flush = [k for k in self.buffers.keys() if k[0] == date_key]
            for buffer_key in keys_to_flush:
                if self.buffers[buffer_key]:
                    dk, sc = buffer_key

                    # Delete existing files for this stock+date combination only
                    delete_key = (sc, dk)
                    if delete_key not in self.deleted_stocks:
                        self._delete_files_for_stock_date(sc, dk)
                        self.deleted_stocks.add(delete_key)

                    # Save new data
                    file_path = self.gcs_writer.save_batch(dk, self.buffers[buffer_key], self.source, sc)
                    if file_path:
                        saved_files.append(file_path)
                        self.all_saved_files.append(file_path)  # Track all files
                    self.buffers[buffer_key].clear()
        else:
            # Flush all dates and stocks
            for buffer_key in list(self.buffers.keys()):
                if self.buffers[buffer_key]:
                    dk, sc = buffer_key

                    # Delete existing files for this stock+date combination only
                    delete_key = (sc, dk)
                    if delete_key not in self.deleted_stocks:
                        self._delete_files_for_stock_date(sc, dk)
                        self.deleted_stocks.add(delete_key)

                    # Save new data
                    file_path = self.gcs_writer.save_batch(dk, self.buffers[buffer_key], self.source, sc)
                    if file_path:
                        saved_files.append(file_path)
                        self.all_saved_files.append(file_path)  # Track all files
                    self.buffers[buffer_key].clear()

        return saved_files

    def _delete_files_for_stock_date(self, stock_code, date_key):
        """Delete files for specific stock and date only"""
        from datetime import datetime

        # Convert date_key (YYYYMMDD) to datetime
        if len(date_key) == 8:
            date_str = f"{date_key[:4]}-{date_key[4:6]}-{date_key[6:8]}"
        else:
            date_str = date_key

        date_obj = datetime.strptime(date_str, '%Y-%m-%d')

        # Delete files for this specific date only
        prefix = self.gcs_writer.gcs_prefix + '/' if self.gcs_writer.gcs_prefix else ''
        prefix += f"dt={date_str}/{stock_code}_{self.source}_"

        blobs = list(self.gcs_writer.bucket.list_blobs(prefix=prefix))

        if blobs:
            for blob in blobs:
                blob.delete()
            logger.info(f"🗑️  Deleted {len(blobs)} existing files for {stock_code} on {date_str}")

    def get_buffer_stats(self):
        """Get current buffer statistics"""
        return {
            f"{date_key}_{stock_code}": len(data)
            for (date_key, stock_code), data in self.buffers.items()
            if data
        }

    def get_all_saved_files(self):
        """
        Get all saved files including auto-flushed files

        Returns:
            list: All file paths saved during this session
        """
        return self.all_saved_files.copy()

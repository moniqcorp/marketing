"""
Buffer management for date-partitioned data
"""

import logging
from datetime import datetime

logger = logging.getLogger(__name__)


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

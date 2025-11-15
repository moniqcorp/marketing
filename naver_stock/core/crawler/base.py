"""
Base crawler class with common functionality
"""

import logging
import time
import asyncio
from abc import ABC, abstractmethod
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple

logger = logging.getLogger(__name__)


class BaseCrawler(ABC):
    """
    Abstract base class for all crawlers
    Provides common functionality like retry logic, session management, date parsing
    """

    def __init__(self, request_delay=0.3, max_retries=3):
        """
        Initialize base crawler

        Args:
            request_delay: Delay between requests in seconds
            max_retries: Maximum number of retry attempts
        """
        self.request_delay = request_delay
        self.max_retries = max_retries
        self.logger = logging.getLogger(self.__class__.__name__)

    @abstractmethod
    def get_discussion_list(self, identifier: str, max_pages: int = 5) -> Tuple[List[str], Optional[str]]:
        """
        Get list of discussion IDs

        Args:
            identifier: Stock code or other identifier
            max_pages: Maximum pages to crawl

        Returns:
            tuple: (list of IDs, metadata)
        """
        pass

    @abstractmethod
    def get_discussion_detail(self, identifier: str, discussion_id: str, **kwargs) -> Optional[Dict[str, Any]]:
        """
        Get discussion detail

        Args:
            identifier: Stock code or other identifier
            discussion_id: Discussion ID
            **kwargs: Additional parameters

        Returns:
            dict: Discussion data or None
        """
        pass

    @abstractmethod
    async def get_discussion_detail_async(self, session, identifier: str, discussion_id: str, **kwargs) -> Optional[Dict[str, Any]]:
        """
        Async version of get_discussion_detail

        Args:
            session: aiohttp session
            identifier: Stock code or other identifier
            discussion_id: Discussion ID
            **kwargs: Additional parameters

        Returns:
            dict: Discussion data or None
        """
        pass

    def parse_date(self, date_str: str) -> Optional[str]:
        """
        Parse date string to DB format (YYYY-MM-DD HH:MM:SS)
        Override this method if crawler needs custom date parsing

        Args:
            date_str: ISO 8601 format or custom format

        Returns:
            str: "YYYY-MM-DD HH:MM:SS" format or None
        """
        if not date_str:
            return None

        try:
            # Remove timezone info (everything after + or Z)
            if '+' in date_str:
                date_str = date_str.split('+')[0]
            elif date_str.endswith('Z'):
                date_str = date_str[:-1]

            # Remove milliseconds if present
            if '.' in date_str:
                date_str = date_str.split('.')[0]

            # Parse ISO format
            dt = datetime.fromisoformat(date_str)
            return dt.strftime("%Y-%m-%d %H:%M:%S")

        except Exception as e:
            self.logger.warning(f"Date parsing error: {e} for date_str={date_str}")
            return None

    def retry_with_backoff(self, func, *args, max_retries=None, **kwargs):
        """
        Retry a function with exponential backoff

        Args:
            func: Function to retry
            *args: Function arguments
            max_retries: Max retries (uses self.max_retries if None)
            **kwargs: Function keyword arguments

        Returns:
            Function result or None
        """
        max_retries = max_retries or self.max_retries

        for attempt in range(max_retries):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt  # Exponential backoff: 1, 2, 4, 8...
                    self.logger.warning(
                        f"Attempt {attempt + 1}/{max_retries} failed: {e}. "
                        f"Retrying in {wait_time}s..."
                    )
                    time.sleep(wait_time)
                else:
                    self.logger.error(f"All {max_retries} attempts failed: {e}")
                    return None

    async def retry_with_backoff_async(self, func, *args, max_retries=None, **kwargs):
        """
        Async version of retry_with_backoff

        Args:
            func: Async function to retry
            *args: Function arguments
            max_retries: Max retries (uses self.max_retries if None)
            **kwargs: Function keyword arguments

        Returns:
            Function result or None
        """
        max_retries = max_retries or self.max_retries

        for attempt in range(max_retries):
            try:
                return await func(*args, **kwargs)
            except asyncio.TimeoutError:
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt
                    self.logger.warning(
                        f"Timeout on attempt {attempt + 1}/{max_retries}. "
                        f"Retrying in {wait_time}s..."
                    )
                    await asyncio.sleep(wait_time)
                else:
                    self.logger.error(f"All {max_retries} attempts timed out")
                    return None
            except Exception as e:
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt
                    self.logger.warning(
                        f"Attempt {attempt + 1}/{max_retries} failed: {e}. "
                        f"Retrying in {wait_time}s..."
                    )
                    await asyncio.sleep(wait_time)
                else:
                    self.logger.error(f"All {max_retries} attempts failed: {e}")
                    return None

    def crawl_with_rate_limit(self, items: List[Any], process_func, delay: Optional[float] = None):
        """
        Crawl items with rate limiting

        Args:
            items: List of items to process
            process_func: Function to process each item
            delay: Delay between requests (uses self.request_delay if None)

        Returns:
            list: Results
        """
        delay = delay or self.request_delay
        results = []

        for idx, item in enumerate(items):
            try:
                result = process_func(item)
                if result:
                    results.append(result)

                # Add delay between requests (except for last item)
                if idx < len(items) - 1:
                    time.sleep(delay)

            except Exception as e:
                self.logger.error(f"Error processing item {idx}: {e}")

        return results

    def validate_data(self, data: Dict[str, Any], required_fields: List[str]) -> bool:
        """
        Validate that data contains required fields

        Args:
            data: Data dictionary to validate
            required_fields: List of required field names

        Returns:
            bool: True if valid, False otherwise
        """
        for field in required_fields:
            if field not in data or data[field] is None:
                self.logger.warning(f"Missing required field: {field}")
                return False
        return True

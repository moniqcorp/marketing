"""
BigQuery client wrapper for common operations
"""

import os
import logging
from google.cloud import bigquery
from google.oauth2 import service_account

logger = logging.getLogger(__name__)


class BigQueryClient:
    """Wrapper for BigQuery operations"""

    def __init__(self, project_id=None, credentials_path=None):
        """
        Initialize BigQuery client

        Args:
            project_id: GCP project ID (optional, reads from env if None)
            credentials_path: Path to service account JSON (optional, uses default if None)
        """
        self.project_id = project_id or os.getenv('GCP_PROJECT_ID')

        if not self.project_id:
            raise ValueError("GCP_PROJECT_ID must be set in environment or passed as argument")

        # Initialize client
        if credentials_path and os.path.exists(credentials_path):
            credentials = service_account.Credentials.from_service_account_file(credentials_path)
            self.client = bigquery.Client(credentials=credentials, project=self.project_id)
            logger.info(f"BigQuery client initialized with credentials: {credentials_path}")
        else:
            self.client = bigquery.Client(project=self.project_id)
            logger.info(f"BigQuery client initialized with default credentials")

    def query(self, sql, params=None):
        """
        Execute a query and return results as list of dictionaries

        Args:
            sql: SQL query string
            params: Query parameters (optional)

        Returns:
            list: List of row dictionaries
        """
        try:
            job_config = bigquery.QueryJobConfig()
            if params:
                job_config.query_parameters = params

            query_job = self.client.query(sql, job_config=job_config)
            results = query_job.result()

            # Convert to list of dicts
            rows = []
            for row in results:
                rows.append(dict(row))

            logger.info(f"Query returned {len(rows)} rows")
            return rows

        except Exception as e:
            logger.error(f"BigQuery query failed: {e}")
            raise

    def create_external_table(self, dataset_id, table_id, gcs_uri, schema, partition_field=None, source_format='PARQUET'):
        """
        Create an external table pointing to GCS

        Args:
            dataset_id: BigQuery dataset ID
            table_id: Table name
            gcs_uri: GCS URI pattern (e.g., 'gs://bucket/dt=*/naver_*.parquet')
            schema: List of bigquery.SchemaField objects
            partition_field: Field to use for partitioning (e.g., 'dt')
            source_format: Source format (default: PARQUET)

        Returns:
            google.cloud.bigquery.Table: Created table
        """
        try:
            table_ref = f"{self.project_id}.{dataset_id}.{table_id}"

            external_config = bigquery.ExternalConfig(source_format)
            external_config.source_uris = [gcs_uri]
            external_config.autodetect = True  # Auto-detect schema from Parquet

            # Hive partitioning options
            if partition_field:
                hive_partitioning = bigquery.HivePartitioningOptions()
                hive_partitioning.mode = "AUTO"
                hive_partitioning.source_uri_prefix = gcs_uri.split('*')[0].rstrip('/')
                external_config.hive_partitioning = hive_partitioning

            table = bigquery.Table(table_ref)
            table.external_data_configuration = external_config

            # Create or update table
            table = self.client.create_table(table, exists_ok=True)
            logger.info(f"✅ External table created: {table_ref}")

            return table

        except Exception as e:
            logger.error(f"Failed to create external table: {e}")
            raise

    def load_table_from_dataframe(self, df, dataset_id, table_id, write_disposition='WRITE_APPEND'):
        """
        Load a pandas DataFrame into BigQuery

        Args:
            df: pandas DataFrame
            dataset_id: BigQuery dataset ID
            table_id: Table name
            write_disposition: WRITE_APPEND, WRITE_TRUNCATE, or WRITE_EMPTY

        Returns:
            google.cloud.bigquery.LoadJob: Load job
        """
        try:
            table_ref = f"{self.project_id}.{dataset_id}.{table_id}"

            job_config = bigquery.LoadJobConfig()
            job_config.write_disposition = write_disposition
            job_config.autodetect = True

            job = self.client.load_table_from_dataframe(df, table_ref, job_config=job_config)
            job.result()  # Wait for completion

            logger.info(f"✅ Loaded {len(df)} rows into {table_ref}")
            return job

        except Exception as e:
            logger.error(f"Failed to load table from DataFrame: {e}")
            raise

    def table_exists(self, dataset_id, table_id):
        """
        Check if a table exists

        Args:
            dataset_id: BigQuery dataset ID
            table_id: Table name

        Returns:
            bool: True if table exists
        """
        try:
            table_ref = f"{self.project_id}.{dataset_id}.{table_id}"
            self.client.get_table(table_ref)
            return True
        except Exception:
            return False

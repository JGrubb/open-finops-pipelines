from pathlib import Path
from typing import List, Dict
from datetime import datetime

from google.cloud import bigquery
from google.oauth2 import service_account

class BigQueryLoader:
    """Service for loading Parquet files to BigQuery."""

    def __init__(self, bigquery_config, parquet_dir: str):
        self.bigquery_config = bigquery_config
        self.parquet_dir = Path(parquet_dir)

        # Initialize BigQuery client
        credentials = service_account.Credentials.from_service_account_file(
            bigquery_config.credentials_path
        )
        self.client = bigquery.Client(
            credentials=credentials,
            project=bigquery_config.project_id
        )

        # Construct table reference
        self.table_ref = f"{bigquery_config.project_id}.{bigquery_config.dataset_id}.{bigquery_config.table_id}"

    def _parse_billing_timestamp(self, billing_period: str) -> datetime:
        """Convert billing period (YYYY-MM) to timestamp (YYYY-MM-01 00:00:00)."""
        return datetime.strptime(f"{billing_period}-01", "%Y-%m-%d")

    def _delete_partition(self, billing_period: str) -> int:
        """Delete existing data for a billing period using optimized partition pruning.

        Returns number of rows deleted.
        """
        billing_timestamp = self._parse_billing_timestamp(billing_period)

        delete_query = f"""
        DELETE FROM `{self.table_ref}`
        WHERE DATE_TRUNC(bill_billing_period_start_date, MONTH) = TIMESTAMP('{billing_timestamp.isoformat()}')
        """

        delete_job = self.client.query(delete_query)
        delete_job.result()

        return delete_job.num_dml_affected_rows or 0

    def _create_load_config(self) -> bigquery.LoadJobConfig:
        """Create standard BigQuery load job configuration."""
        return bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            schema_update_options=[
                bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
            ]
        )

    def _load_parquet_file(self, parquet_file: Path) -> int:
        """Load a Parquet file to BigQuery and return number of rows loaded."""
        with open(parquet_file, "rb") as source_file:
            job = self.client.load_table_from_file(
                source_file,
                self.table_ref,
                job_config=self._create_load_config()
            )

        job.result()

        if job.errors:
            raise Exception(f"BigQuery load job failed: {job.errors}")

        return job.output_rows or 0

    def load_billing_data_by_execution(
        self,
        manifests: List,
        vendor: str = "aws",
        overwrite: bool = False
    ) -> Dict[str, str]:
        """Load billing data to BigQuery from Parquet files.

        Each manifest represents a unique execution of billing data.
        Only loads executions with new execution_ids not already in BigQuery.

        Returns dict mapping execution_key (billing_period:execution_id) to load status.
        """
        # Ensure table exists first
        self._ensure_table_exists()

        # Get currently loaded execution_ids from BigQuery
        loaded_execution_ids = self.get_loaded_execution_ids()

        results = {}
        executions_to_load = []

        for manifest in manifests:
            execution_key = f"{manifest.billing_period}:{manifest.id}"

            # Check if this execution_id is already loaded
            if not overwrite and loaded_execution_ids.get(manifest.billing_period) == manifest.id:
                results[execution_key] = "skipped"
                print(f"  Skipping {manifest.billing_period} ({manifest.id[:8]}...) - already loaded")
            else:
                executions_to_load.append(manifest)

        if not executions_to_load:
            print("  All billing data already loaded in BigQuery")
            return results

        print(f"  Loading {len(executions_to_load)} new execution(s)\n")

        for manifest in executions_to_load:
            execution_key = f"{manifest.billing_period}:{manifest.id}"
            try:
                result = self._load_single_execution(
                    manifest.billing_period, manifest.id, vendor
                )
                results[execution_key] = result
                print(f"✓ {manifest.billing_period} ({manifest.id[:8]}...): {result}")
            except Exception as e:
                results[execution_key] = "failed"
                print(f"✗ {manifest.billing_period} ({manifest.id[:8]}...): {str(e)}")

        return results

    def get_loaded_execution_ids(self) -> Dict[str, str]:
        """Query BigQuery to get currently loaded execution_ids by billing period.

        Returns dict mapping billing_period -> execution_id.
        """
        try:
            query = f"""
                SELECT DISTINCT
                    FORMAT_TIMESTAMP('%Y-%m', bill_billing_period_start_date) as billing_period,
                    execution_id
                FROM `{self.table_ref}`
                WHERE execution_id IS NOT NULL
                ORDER BY billing_period DESC
            """

            query_job = self.client.query(query)
            results = query_job.result()

            period_map = {}
            for row in results:
                billing_period = row['billing_period']
                execution_id = row['execution_id']
                if billing_period not in period_map:
                    period_map[billing_period] = execution_id

            return period_map

        except Exception:
            # Table doesn't exist or query failed - return empty
            return {}

    def _delete_and_load_partition(self, parquet_file: Path, billing_period: str) -> None:
        """Delete existing partition data and load new Parquet file to BigQuery."""
        # Delete existing data for this billing period
        rows_deleted = self._delete_partition(billing_period)

        if rows_deleted:
            print(f"   Deleted {rows_deleted:,} existing rows for {billing_period}")

        # Load the Parquet file
        print(f"   Loading {parquet_file.name} to BigQuery...")
        rows_loaded = self._load_parquet_file(parquet_file)

        if rows_loaded:
            print(f"   Loaded {rows_loaded:,} rows")

    def _load_single_execution(
        self,
        billing_period: str,
        execution_id: str,
        vendor: str
    ) -> str:
        """Load billing data for a single execution to BigQuery from Parquet file."""
        parquet_file = self.parquet_dir / f"{billing_period}_{execution_id}_{vendor}_billing.parquet"
        if not parquet_file.exists():
            raise ValueError(f"Parquet file not found: {parquet_file}")

        self._delete_and_load_partition(parquet_file, billing_period)
        return "loaded"

    def load_billing_periods(
        self,
        billing_periods: List[str],
        vendor: str = "aws"
    ) -> Dict[str, str]:
        """Load multiple billing periods to BigQuery from Parquet files.

        Returns dict mapping billing_period to load status ('loaded', 'skipped', 'failed').
        """
        results = {}

        for billing_period in billing_periods:
            try:
                result = self._load_single_period(billing_period, vendor)
                results[billing_period] = result
                print(f"  {billing_period}: {result}")
            except Exception as e:
                results[billing_period] = "failed"
                print(f"  {billing_period}: failed - {str(e)}")

        return results

    def _load_single_period(
        self,
        billing_period: str,
        vendor: str
    ) -> str:
        """Load a single billing period to BigQuery from Parquet file."""
        # Ensure table exists with proper partitioning and clustering
        self._ensure_table_exists()

        parquet_file = self.parquet_dir / f"{billing_period}_{vendor}_billing.parquet"
        if not parquet_file.exists():
            raise ValueError(f"Parquet file not found: {parquet_file}")

        self._delete_and_load_partition(parquet_file, billing_period)
        return "loaded"

    def _ensure_table_exists(self) -> None:
        """Ensure the BigQuery table exists with proper partitioning and clustering."""
        try:
            # Check if table already exists
            self.client.get_table(self.table_ref)
            print(f"  BigQuery table {self.table_ref} already exists")
            return
        except Exception:
            # Table doesn't exist, create it
            print(f"  Creating BigQuery table {self.table_ref}...")

        # Find the first available Parquet file to infer schema
        parquet_files = list(self.parquet_dir.glob("*_aws_billing.parquet"))
        if not parquet_files:
            raise ValueError("No Parquet files found to infer schema")

        first_parquet_file = sorted(parquet_files)[0]
        print(f"  Inferring schema from {first_parquet_file.name}")

        # Create a temporary table to get the schema
        temp_table_id = f"{self.table_ref}_temp_schema"

        # Load first file to temporary table with auto-detection
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            autodetect=True
        )

        with open(first_parquet_file, "rb") as source_file:
            job = self.client.load_table_from_file(
                source_file,
                temp_table_id,
                job_config=job_config
            )

        # Wait for the job to complete
        job.result()

        if job.errors:
            raise Exception(f"Schema inference failed: {job.errors}")

        # Get the schema from the temporary table
        temp_table = self.client.get_table(temp_table_id)
        schema = temp_table.schema

        # Delete the temporary table
        self.client.delete_table(temp_table_id)

        # Create the actual table with partitioning and clustering
        table = bigquery.Table(self.table_ref, schema=schema)

        # Set up time partitioning on bill_billing_period_start_date
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.MONTH,
            field="bill_billing_period_start_date"
        )

        # Set up clustering on line_item_usage_start_date
        table.clustering_fields = ["line_item_usage_start_date"]

        # Create the table
        self.client.create_table(table)
        print(f"  Created BigQuery table {self.table_ref} with monthly partitioning and clustering")

    def get_available_billing_periods(self, vendor: str = "aws") -> List[str]:
        """Get list of billing periods that have exported Parquet files available for BigQuery load."""
        # Scan parquet directory for available files
        parquet_files = list(self.parquet_dir.glob(f"*_{vendor}_billing.parquet"))

        billing_periods = []
        for file in parquet_files:
            # Extract billing period from filename: YYYY-MM_aws_billing.parquet
            parts = file.stem.split("_")
            if len(parts) >= 2:
                billing_period = parts[0]  # YYYY-MM
                billing_periods.append(billing_period)

        # Remove duplicates and sort
        return sorted(list(set(billing_periods)), reverse=True)

    def validate_bigquery_connection(self) -> bool:
        """Validate BigQuery connection and dataset access."""
        try:
            self.client.get_dataset(
                f"{self.bigquery_config.project_id}.{self.bigquery_config.dataset_id}"
            )
            return True
        except Exception:
            return False
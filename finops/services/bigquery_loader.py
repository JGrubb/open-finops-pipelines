import uuid
from pathlib import Path
from typing import List, Dict

from google.cloud import bigquery
from google.oauth2 import service_account

from finops.services.state_db import StateDB


class BigQueryLoader:
    """Service for loading Parquet files to BigQuery."""

    def __init__(self, state_db: StateDB, bigquery_config, parquet_dir: str):
        self.state_db = state_db
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

    def load_billing_periods(
        self,
        billing_periods: List[str],
        vendor: str = "aws",
        overwrite: bool = False
    ) -> Dict[str, str]:
        """Load multiple billing periods to BigQuery from Parquet files.

        Returns dict mapping billing_period to load status ('loaded', 'skipped', 'failed').
        """
        results = {}

        for billing_period in billing_periods:
            try:
                result = self._load_single_period(
                    billing_period, vendor, overwrite
                )
                results[billing_period] = result
                print(f"✓ {billing_period}: {result}")
            except Exception as e:
                results[billing_period] = "failed"
                print(f"✗ {billing_period}: failed - {str(e)}")

        return results

    def _load_single_period(
        self,
        billing_period: str,
        vendor: str,
        overwrite: bool
    ) -> str:
        """Load a single billing period to BigQuery from Parquet file."""
        load_id = str(uuid.uuid4())
        load_type = "bigquery"

        # Check if load already exists and overwrite is False
        existing_load = self.state_db.get_export_status(vendor, billing_period, load_type)
        if existing_load and existing_load["state"] == "exported" and not overwrite:
            return "skipped"

        # Check if Parquet file exists for this billing period
        parquet_file = self.parquet_dir / f"{billing_period}_{vendor}_billing.parquet"
        if not parquet_file.exists():
            raise ValueError(f"Parquet file not found: {parquet_file}")

        # Save load record as pending
        self.state_db.save_export(
            load_id, vendor, billing_period, load_type, self.table_ref, "pending"
        )

        try:
            # Update state to loading
            self.state_db.update_export_state(load_id, "exporting")

            # Load to BigQuery from Parquet file
            self._load_parquet_to_bigquery(parquet_file, overwrite)

            # Update state to loaded
            self.state_db.update_export_state(load_id, "exported")
            return "loaded"

        except Exception as e:
            # Update state to failed
            self.state_db.update_export_state(load_id, "failed", str(e))
            raise

    def _load_parquet_to_bigquery(self, parquet_file: Path, overwrite: bool) -> None:
        """Load a Parquet file to BigQuery, replacing the partition for that billing period."""
        # Ensure table exists with proper partitioning and clustering
        self._ensure_table_exists()

        # Extract billing period from filename for partition replacement
        billing_period = parquet_file.stem.split('_')[0]  # Extract "2025-09" from "2025-09_aws_billing.parquet"
        year, month = billing_period.split('-')

        print(f"✓ Replacing partition {billing_period} in BigQuery table")

        # First, delete existing data for this billing period
        delete_query = f"""
        DELETE FROM `{self.table_ref}`
        WHERE EXTRACT(YEAR FROM bill_billing_period_start_date) = {year}
          AND EXTRACT(MONTH FROM bill_billing_period_start_date) = {int(month)}
        """

        delete_job = self.client.query(delete_query)
        delete_job.result()  # Wait for deletion to complete

        # Configure the load job to append the new data
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            autodetect=True
        )

        # Load the Parquet file
        with open(parquet_file, "rb") as source_file:
            job = self.client.load_table_from_file(
                source_file,
                self.table_ref,
                job_config=job_config
            )

        # Wait for the job to complete
        job.result()

        if job.errors:
            raise Exception(f"BigQuery load job failed: {job.errors}")

    def _ensure_table_exists(self) -> None:
        """Ensure the BigQuery table exists with proper partitioning and clustering."""
        try:
            # Check if table already exists
            table = self.client.get_table(self.table_ref)
            print(f"✓ BigQuery table {self.table_ref} already exists")
            return  # Table exists, nothing to do
        except Exception:
            # Table doesn't exist, create it
            print(f"✓ Creating BigQuery table {self.table_ref}...")

        # Find the first available Parquet file to infer schema
        parquet_files = list(self.parquet_dir.glob("*_aws_billing.parquet"))
        if not parquet_files:
            raise ValueError("No Parquet files found to infer schema")

        first_parquet_file = sorted(parquet_files)[0]
        print(f"✓ Inferring schema from {first_parquet_file.name}")

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
        table = self.client.create_table(table)
        print(f"✓ Created BigQuery table {self.table_ref} with monthly partitioning and clustering")

    def get_available_billing_periods(self, vendor: str = "aws") -> List[str]:
        """Get list of billing periods that have exported Parquet files available for BigQuery load."""
        # Get exported parquet files from state database
        all_exports = self.state_db.get_exports_by_state("exported", vendor)
        exported_parquet = [exp for exp in all_exports if exp["export_type"] == "parquet"]

        billing_periods = []
        for export in exported_parquet:
            billing_periods.append(export["billing_period"])

        # Remove duplicates and sort
        return sorted(list(set(billing_periods)), reverse=True)

    def get_load_summary(self, vendor: str = "aws") -> Dict:
        """Get summary of BigQuery load operations by state."""
        loads_by_state = {}
        for state in ["pending", "exporting", "exported", "failed"]:
            all_exports = self.state_db.get_exports_by_state(state, vendor)
            bigquery_loads = [exp for exp in all_exports if exp["export_type"] == "bigquery"]
            loads_by_state[state] = len(bigquery_loads)

        return loads_by_state

    def validate_bigquery_connection(self) -> bool:
        """Validate BigQuery connection and dataset access."""
        try:
            # Try to get dataset info
            dataset = self.client.get_dataset(
                f"{self.bigquery_config.project_id}.{self.bigquery_config.dataset_id}"
            )
            return True
        except Exception:
            return False
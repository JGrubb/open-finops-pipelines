"""Service for checking state by querying destination databases."""

from typing import Dict, Optional, Set
from pathlib import Path
import duckdb
from google.cloud import bigquery
from google.oauth2 import service_account


class StateChecker:
    """Query destination databases to determine what data has been loaded."""

    def __init__(self, config):
        """Initialize with configuration."""
        self.config = config
        self.duckdb_path = config.duckdb.database_path if hasattr(config, 'duckdb') else None
        self.bigquery_config = config.bigquery if hasattr(config, 'bigquery') else None

    def get_loaded_execution_ids(self, vendor: str = "aws") -> Dict[str, str]:
        """
        Query destination database to get loaded execution_ids by billing period.

        Returns dict mapping billing_period -> execution_id.
        Checks remote BigQuery first (if configured), otherwise checks local DuckDB.
        """
        # Try BigQuery first if configured
        if self.bigquery_config and hasattr(self.bigquery_config, 'credentials_path'):
            try:
                return self._query_bigquery(vendor)
            except Exception as e:
                # Only show warning if it's not a "table not found" error
                error_msg = str(e).lower()
                if "not found" not in error_msg and "does not exist" not in error_msg:
                    print(f"Warning: Failed to query BigQuery: {e}")
                    print("Falling back to local DuckDB...")

        # Fall back to DuckDB
        if self.duckdb_path:
            return self._query_duckdb(vendor)

        # No database configured
        return {}

    def _query_bigquery(self, vendor: str) -> Dict[str, str]:
        """Query BigQuery for loaded execution_ids."""
        # Initialize BigQuery client
        credentials = service_account.Credentials.from_service_account_file(
            self.bigquery_config.credentials_path
        )
        client = bigquery.Client(
            credentials=credentials,
            project=self.bigquery_config.project_id
        )

        # Construct table reference
        table_ref = f"{self.bigquery_config.project_id}.{self.bigquery_config.dataset_id}.{self.bigquery_config.table_id}"

        # Query for distinct billing_period and execution_id combinations
        query = f"""
            SELECT DISTINCT
                FORMAT_TIMESTAMP('%Y-%m', bill_billing_period_start_date) as billing_period,
                execution_id
            FROM `{table_ref}`
            WHERE execution_id IS NOT NULL
            ORDER BY billing_period DESC
        """

        query_job = client.query(query)
        results = query_job.result()

        # Build map: billing_period -> execution_id
        # If multiple execution_ids exist for a period, use the most recent one
        period_map = {}
        for row in results:
            billing_period = row['billing_period']
            execution_id = row['execution_id']
            if billing_period not in period_map:
                period_map[billing_period] = execution_id

        return period_map

    def _query_duckdb(self, vendor: str) -> Dict[str, str]:
        """Query DuckDB for loaded execution_ids."""
        duckdb_path = Path(self.duckdb_path)

        if not duckdb_path.exists():
            return {}

        table_name = f"{vendor}_billing_data"

        try:
            conn = duckdb.connect(str(duckdb_path), read_only=True)

            # Query for distinct billing_period and execution_id combinations
            query = f"""
                SELECT DISTINCT
                    PRINTF('%04d-%02d',
                           EXTRACT(YEAR FROM bill_billing_period_start_date),
                           EXTRACT(MONTH FROM bill_billing_period_start_date)
                    ) as billing_period,
                    execution_id
                FROM {table_name}
                WHERE execution_id IS NOT NULL
                ORDER BY billing_period DESC
            """

            results = conn.execute(query).fetchall()
            conn.close()

            # Build map: billing_period -> execution_id
            # If multiple execution_ids exist for a period, use the first one found
            period_map = {}
            for row in results:
                billing_period = row[0]
                execution_id = row[1]
                if billing_period not in period_map:
                    period_map[billing_period] = execution_id

            return period_map

        except Exception as e:
            # Table doesn't exist or other error
            return {}

    def check_execution_id_loaded(self, billing_period: str, execution_id: str, vendor: str = "aws") -> bool:
        """
        Check if a specific execution_id for a billing period is already loaded.

        Returns True if the execution_id is found in the destination database.
        """
        loaded_map = self.get_loaded_execution_ids(vendor)
        return loaded_map.get(billing_period) == execution_id
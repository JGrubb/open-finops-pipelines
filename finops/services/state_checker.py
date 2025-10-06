"""Service for checking state by querying destination databases."""

from typing import Dict
from pathlib import Path
from finops.services.bigquery_loader import BigQueryLoader
from finops.services.duckdb_loader import DuckDBLoader


class StateChecker:
    """Query destination databases to determine what data has been loaded."""

    def __init__(self, config):
        """Initialize with configuration."""
        self.config = config

    def get_loaded_execution_ids(self, vendor: str = "aws") -> Dict[str, str]:
        """
        Query destination database to get loaded execution_ids by billing period.

        Returns dict mapping billing_period -> execution_id.
        Checks remote BigQuery first (if configured), otherwise checks local DuckDB.
        """
        # Try BigQuery first if configured
        if hasattr(self.config, 'bigquery') and self.config.bigquery:
            try:
                loader = BigQueryLoader(self.config.bigquery, self.config.parquet_dir)
                return loader.get_loaded_execution_ids()
            except Exception as e:
                # Only show warning if it's not a "table not found" error
                error_msg = str(e).lower()
                if "not found" not in error_msg and "does not exist" not in error_msg:
                    print(f"Warning: Failed to query BigQuery: {e}")
                    print("Falling back to local DuckDB...")

        # Fall back to DuckDB
        if hasattr(self.config, 'duckdb_path') and self.config.duckdb_path:
            duckdb_path = Path(self.config.duckdb_path)

            # Skip in-memory databases and non-existent files
            if self.config.duckdb_path == ":memory:" or not duckdb_path.exists():
                return {}

            try:
                with DuckDBLoader(self.config.duckdb_path) as loader:
                    return loader.get_loaded_execution_ids(f"{vendor}_billing_data")
            except Exception:
                return {}

        # No database configured
        return {}
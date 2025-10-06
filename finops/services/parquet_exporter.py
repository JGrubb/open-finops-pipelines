import duckdb
from pathlib import Path
from typing import List, Dict

class ParquetExporter:
    """Service for exporting DuckDB data to Parquet files."""

    def __init__(self, duckdb_path: str, parquet_dir: str, table_name: str = "aws_billing_data", connection=None):
        self.duckdb_path = Path(duckdb_path) if duckdb_path != ":memory:" else duckdb_path
        self.parquet_dir = Path(parquet_dir)
        self.table_name = table_name
        self.parquet_dir.mkdir(parents=True, exist_ok=True)
        self.conn = connection
        self._owns_connection = connection is None

    def __enter__(self):
        """Context manager entry - open connection if needed."""
        if self.conn is None:
            db_path = str(self.duckdb_path) if isinstance(self.duckdb_path, Path) else self.duckdb_path
            self.conn = duckdb.connect(db_path)
        return self

    def __exit__(self, _exc_type, _exc_val, _exc_tb):
        """Context manager exit - close connection if we own it."""
        if self.conn and self._owns_connection:
            self.conn.close()
            self.conn = None

    def export_billing_data_by_execution(
        self,
        manifests: List,
        vendor: str = "aws",
        overwrite: bool = False,
        compression: str = "snappy"
    ) -> Dict[str, str]:
        """Export billing data for multiple executions to Parquet files.

        Each manifest represents a unique execution of billing data.
        Filename format: {billing_period}_{execution_id}_{vendor}_billing.parquet

        Returns dict mapping execution_key (billing_period:execution_id) to export status.
        """
        results = {}

        for manifest in manifests:
            execution_key = f"{manifest.billing_period}:{manifest.id}"
            try:
                result = self._export_single_execution(
                    manifest.billing_period, manifest.id, vendor, overwrite, compression
                )
                results[execution_key] = result
                print(f"✓ {manifest.billing_period} ({manifest.id[:8]}...): {result}")
            except Exception as e:
                results[execution_key] = "failed"
                print(f"✗ {manifest.billing_period} ({manifest.id[:8]}...): failed - {str(e)}")

        return results

    def _export_single_execution(
        self,
        billing_period: str,
        execution_id: str,
        vendor: str,
        overwrite: bool,
        compression: str
    ) -> str:
        """Export billing data for a single execution to Parquet."""
        filename = f"{billing_period}_{execution_id}_{vendor}_billing.parquet"
        file_path = self.parquet_dir / filename

        # Check if export already exists and overwrite is False
        if file_path.exists() and not overwrite:
            return "skipped"

        # Check if data exists in DuckDB for this execution_id
        if not self._has_data_for_execution(billing_period, execution_id):
            raise ValueError(f"No data found in DuckDB for {billing_period} execution {execution_id}")

        # Export to Parquet using DuckDB
        self._export_execution_to_parquet(billing_period, execution_id, file_path, compression)
        return "exported"

    def _has_data_for_execution(self, billing_period: str, execution_id: str) -> bool:
        """Check if DuckDB has data for the specified execution_id."""
        if not self.conn:
            raise RuntimeError("Connection not established. Use within context manager.")

        try:
            year, month = billing_period.split('-')
            result = self.conn.execute(f"""
                SELECT COUNT(*)
                FROM {self.table_name}
                WHERE execution_id = ?
                  AND EXTRACT(YEAR FROM bill_billing_period_start_date) = ?
                  AND EXTRACT(MONTH FROM bill_billing_period_start_date) = ?
            """, (execution_id, int(year), int(month))).fetchone()

            return result[0] > 0 if result else False
        except Exception:
            return False

    def _export_execution_to_parquet(
        self,
        billing_period: str,
        execution_id: str,
        file_path: Path,
        compression: str
    ) -> None:
        """Export execution data to Parquet file using DuckDB COPY command."""
        if not self.conn:
            raise RuntimeError("Connection not established. Use within context manager.")

        year, month = billing_period.split('-')

        query = f"""
            COPY (
                SELECT *
                FROM {self.table_name}
                WHERE execution_id = '{execution_id}'
                  AND EXTRACT(YEAR FROM bill_billing_period_start_date) = {year}
                  AND EXTRACT(MONTH FROM bill_billing_period_start_date) = {month}
                ORDER BY line_item_usage_start_date, line_item_usage_account_id, line_item_product_code
            ) TO '{file_path}' (FORMAT PARQUET, COMPRESSION '{compression}')
        """

        self.conn.execute(query)

    def export_billing_periods(
        self,
        billing_periods: List[str],
        vendor: str = "aws",
        overwrite: bool = False,
        compression: str = "snappy"
    ) -> Dict[str, str]:
        """Export multiple billing periods to Parquet files.

        Returns dict mapping billing_period to export status ('exported', 'skipped', 'failed').
        """
        results = {}

        for billing_period in billing_periods:
            try:
                result = self._export_single_period(
                    billing_period, vendor, overwrite, compression
                )
                results[billing_period] = result
                print(f"✓ {billing_period}: {result}")
            except Exception as e:
                results[billing_period] = "failed"
                print(f"✗ {billing_period}: failed - {str(e)}")

        return results

    def _export_single_period(
        self,
        billing_period: str,
        vendor: str,
        overwrite: bool,
        compression: str
    ) -> str:
        """Export a single billing period to Parquet."""
        filename = f"{billing_period}_{vendor}_billing.parquet"
        file_path = self.parquet_dir / filename

        # Check if export already exists and overwrite is False
        if file_path.exists() and not overwrite:
            return "skipped"

        # Check if data exists in DuckDB for this billing period
        if not self._has_data_for_period(billing_period, vendor):
            raise ValueError(f"No data found in DuckDB for billing period {billing_period}")

        # Export to Parquet using DuckDB
        self._export_to_parquet(billing_period, file_path, compression)
        return "exported"

    def _has_data_for_period(self, billing_period: str, _vendor: str) -> bool:
        """Check if DuckDB has data for the specified billing period."""
        if not self.conn:
            raise RuntimeError("Connection not established. Use within context manager.")

        try:
            # Check if table exists and has data for this billing period
            # billing_period format is "YYYY-MM", convert to date for comparison
            year, month = billing_period.split('-')
            result = self.conn.execute(f"""
                SELECT COUNT(*)
                FROM {self.table_name}
                WHERE EXTRACT(YEAR FROM bill_billing_period_start_date) = ?
                  AND EXTRACT(MONTH FROM bill_billing_period_start_date) = ?
            """, (int(year), int(month))).fetchone()

            return result[0] > 0 if result else False
        except Exception:
            # Table doesn't exist or other error
            return False

    def _export_to_parquet(
        self,
        billing_period: str,
        file_path: Path,
        compression: str
    ) -> None:
        """Export billing data to Parquet file using DuckDB COPY command."""
        if not self.conn:
            raise RuntimeError("Connection not established. Use within context manager.")

        # billing_period format is "YYYY-MM", convert to date for filtering
        year, month = billing_period.split('-')

        # Build the SQL query to filter by billing period
        query = f"""
            COPY (
                SELECT *
                FROM {self.table_name}
                WHERE EXTRACT(YEAR FROM bill_billing_period_start_date) = {year}
                  AND EXTRACT(MONTH FROM bill_billing_period_start_date) = {month}
                ORDER BY line_item_usage_start_date, line_item_usage_account_id, line_item_product_code
            ) TO '{file_path}' (FORMAT PARQUET, COMPRESSION '{compression}')
        """

        self.conn.execute(query)

    def get_available_billing_periods(self, _vendor: str = "aws") -> List[str]:
        """Get list of billing periods that have loaded data available for export."""
        if not self.conn:
            raise RuntimeError("Connection not established. Use within context manager.")

        try:
            # Query actual DuckDB data to get available billing periods
            result = self.conn.execute(f"""
                SELECT DISTINCT
                    PRINTF('%04d-%02d',
                           EXTRACT(YEAR FROM bill_billing_period_start_date),
                           EXTRACT(MONTH FROM bill_billing_period_start_date)
                    ) as billing_period
                FROM {self.table_name}
                ORDER BY billing_period DESC
            """).fetchall()

            return [row[0] for row in result]
        except Exception:
            return []

    def validate_table_exists(self) -> bool:
        """Validate that the table exists and has data."""
        if not self.conn:
            raise RuntimeError("Connection not established. Use within context manager.")

        try:
            result = self.conn.execute(f"SELECT COUNT(*) FROM {self.table_name}").fetchone()
            return result[0] > 0 if result else False
        except Exception:
            return False
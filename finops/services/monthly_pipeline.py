"""Memory-optimized monthly pipeline orchestrator for billing data processing."""
from pathlib import Path
from typing import List, Dict
from collections import defaultdict
from finops.services.duckdb_loader import DuckDBLoader
from finops.services.parquet_exporter import ParquetExporter


class MonthlyPipelineOrchestrator:
    """Orchestrates month-by-month processing to minimize memory usage.

    For each billing month:
    1. Load CSVs into DuckDB
    2. Export to Parquet (streams from DuckDB)
    3. Truncate DuckDB table
    4. Repeat for next month
    """

    def __init__(
        self,
        duckdb_path: str,
        staging_dir: str,
        parquet_dir: str,
        table_name: str = "aws_billing_data"
    ):
        self.duckdb_path = duckdb_path
        self.staging_dir = Path(staging_dir)
        self.parquet_dir = Path(parquet_dir)
        self.table_name = table_name
        self.parquet_dir.mkdir(parents=True, exist_ok=True)

    def _group_manifests_by_month(self, manifests: List) -> Dict[str, List]:
        """Group manifests by billing period (month)."""
        monthly_groups = defaultdict(list)
        for manifest in manifests:
            monthly_groups[manifest.billing_period].append(manifest)
        return dict(monthly_groups)

    def process_monthly(
        self,
        manifests: List,
        vendor: str = "aws",
        compression: str = "snappy",
        overwrite_parquet: bool = False
    ) -> Dict:
        """Process manifests month-by-month to minimize memory footprint.

        Returns combined statistics from all monthly processing.
        """
        if not manifests:
            return {
                'total_months': 0,
                'successful_months': 0,
                'failed_months': 0,
                'total_rows_loaded': 0,
                'total_rows_exported': 0
            }

        # Group manifests by billing month
        monthly_groups = self._group_manifests_by_month(manifests)
        sorted_months = sorted(monthly_groups.keys())

        print(f"Processing {len(sorted_months)} billing month(s) sequentially to minimize memory usage")
        print(f"DuckDB: {self.duckdb_path}")
        print(f"Staging: {self.staging_dir}")
        print(f"Parquet output: {self.parquet_dir}")
        print()

        total_rows_loaded = 0
        total_rows_exported = 0
        successful_months = 0
        failed_months = 0
        monthly_results = []

        # Process each month sequentially
        for i, billing_month in enumerate(sorted_months, 1):
            month_manifests = monthly_groups[billing_month]
            print(f"{'='*60}")
            print(f"[{i}/{len(sorted_months)}] Processing {billing_month} ({len(month_manifests)} execution(s))")
            print(f"{'='*60}")

            try:
                result = self._process_single_month(
                    billing_month,
                    month_manifests,
                    vendor,
                    compression,
                    overwrite_parquet
                )

                monthly_results.append(result)

                if result['status'] == 'success':
                    successful_months += 1
                    total_rows_loaded += result['rows_loaded']
                    total_rows_exported += result['rows_exported']
                else:
                    failed_months += 1

            except Exception as e:
                print(f"✗ Failed to process {billing_month}: {str(e)}")
                failed_months += 1
                monthly_results.append({
                    'billing_month': billing_month,
                    'status': 'failed',
                    'error': str(e),
                    'rows_loaded': 0,
                    'rows_exported': 0
                })

            print()

        # Print final summary
        print(f"{'='*60}")
        print("Monthly Pipeline Summary:")
        print(f"  Total months processed: {len(sorted_months)}")
        print(f"  Successful: {successful_months}")
        print(f"  Failed: {failed_months}")
        print(f"  Total rows loaded: {total_rows_loaded:,}")
        print(f"  Total rows exported: {total_rows_exported:,}")
        print(f"{'='*60}")

        return {
            'total_months': len(sorted_months),
            'successful_months': successful_months,
            'failed_months': failed_months,
            'total_rows_loaded': total_rows_loaded,
            'total_rows_exported': total_rows_exported,
            'monthly_results': monthly_results
        }

    def _process_single_month(
        self,
        billing_month: str,
        manifests: List,
        vendor: str,
        compression: str,
        overwrite_parquet: bool
    ) -> Dict:
        """Process a single billing month: load -> export -> truncate.

        Returns processing statistics for the month.
        """
        rows_loaded = 0
        rows_exported = 0

        # Open DuckDB connection (shared between loader and exporter)
        with DuckDBLoader(self.duckdb_path) as loader:
            # Step 1: Load CSV data into DuckDB
            print(f"\n📥 Step 1/3: Loading CSVs into DuckDB...")

            load_stats = loader.load_billing_data_from_manifests(
                manifests=manifests,
                staging_dir=str(self.staging_dir),
                table_name=self.table_name
            )

            if load_stats['failed_executions'] > 0:
                raise Exception(f"Failed to load {load_stats['failed_executions']} execution(s)")

            rows_loaded = load_stats['total_rows']
            print(f"✓ Loaded {rows_loaded:,} rows into DuckDB")

            # Step 2: Export to Parquet (streams from DuckDB, minimal memory)
            print(f"\n📤 Step 2/3: Exporting to Parquet...")

            with ParquetExporter(
                self.duckdb_path,
                str(self.parquet_dir),
                self.table_name,
                connection=loader.connection  # Share the connection
            ) as exporter:
                export_results = exporter.export_billing_data_by_execution(
                    manifests=manifests,
                    vendor=vendor,
                    overwrite=overwrite_parquet,
                    compression=compression
                )

                exported_count = sum(1 for status in export_results.values() if status == "exported")
                failed_exports = sum(1 for status in export_results.values() if status == "failed")

                if failed_exports > 0:
                    raise Exception(f"Failed to export {failed_exports} execution(s)")

                # Estimate rows exported (same as loaded since we export everything we loaded)
                rows_exported = rows_loaded
                print(f"✓ Exported {exported_count} parquet file(s)")

            # Step 3: Truncate DuckDB to free memory
            print(f"\n🧹 Step 3/3: Truncating DuckDB to free memory...")
            deleted_rows = loader.truncate_table(self.table_name)
            print(f"✓ Cleared {deleted_rows:,} rows from DuckDB")

        print(f"\n✓ Completed {billing_month}")

        return {
            'billing_month': billing_month,
            'status': 'success',
            'rows_loaded': rows_loaded,
            'rows_exported': rows_exported,
            'executions_processed': len(manifests)
        }

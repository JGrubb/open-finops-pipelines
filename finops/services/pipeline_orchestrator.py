import uuid
from pathlib import Path
from typing import List, Optional

from finops.config import FinopsConfig
from finops.services.state_db import StateDB
from finops.services.manifest_discovery import ManifestDiscoveryService
from finops.services.billing_extractor import BillingExtractorService
from finops.services.duckdb_loader import DuckDBLoader
from finops.services.parquet_exporter import ParquetExporter
from finops.services.bigquery_loader import BigQueryLoader


class PipelineOrchestrator:
    """Orchestrates the complete billing data pipeline from discovery to warehouse loading."""

    def __init__(self, config: FinopsConfig):
        self.config = config
        self.state_db = StateDB(Path(config.state_db))

        # Initialize services
        self.manifest_discovery = ManifestDiscoveryService(config.aws, self.state_db)
        self.billing_extractor = BillingExtractorService(config.aws, self.state_db)

        # Initialize DuckDB and Parquet services (only if DuckDB is configured)
        if config.database.duckdb:
            self.duckdb_path = config.database.duckdb.database_path
        else:
            self.duckdb_path = "./data/finops.duckdb"  # fallback

        # Only initialize BigQuery loader if BigQuery is configured
        self.bigquery_loader = None
        if config.database.bigquery:
            self.bigquery_loader = BigQueryLoader(self.state_db, config.database.bigquery, config.parquet_dir)

    def run_full_pipeline(self, start_date: Optional[str] = None, end_date: Optional[str] = None,
                         dry_run: bool = False) -> dict:
        """
        Run the complete pipeline: discover → extract → load → export → load-remote.

        Args:
            start_date: Optional start date filter (YYYY-MM format)
            end_date: Optional end date filter (YYYY-MM format)
            dry_run: If True, show what would be processed without executing

        Returns:
            dict: Summary of pipeline execution results
        """
        run_id = str(uuid.uuid4())

        if dry_run:
            return self._dry_run_pipeline(start_date, end_date)

        try:
            # Initialize run tracking
            self.state_db.save_run(run_id, status="running")

            print(f"🚀 Starting pipeline run {run_id}")

            # Step 1: Discover manifests and mark stale ones
            print("📋 Discovering manifests...")
            self._discover_and_mark_stale(run_id)

            # Step 2: Get newly discovered manifests to process
            unprocessed_manifests = self._get_manifests_to_process(start_date, end_date)

            if not unprocessed_manifests:
                print("✅ No new manifests to process")
                self.state_db.update_run_status(run_id, "completed", 0, [])
                return self._build_summary(run_id, [], [])

            print(f"📦 Found {len(unprocessed_manifests)} manifests to process")

            # Step 3: Extract billing files
            print("⬇️  Extracting billing files...")
            staged_manifests = self._extract_billing_files(unprocessed_manifests, run_id)

            # Step 4: Load to DuckDB
            print("💾 Loading to DuckDB...")
            loaded_manifests = self._load_to_duckdb(staged_manifests, run_id)

            # Step 5: Export to Parquet (only newly loaded billing periods)
            loaded_periods = self._get_billing_periods_from_manifests(loaded_manifests)
            if loaded_periods:
                print(f"📤 Exporting {len(loaded_periods)} billing periods to Parquet...")
                self._export_to_parquet(loaded_periods)

                # Step 6: Load to BigQuery (only newly exported periods)
                print("☁️  Loading to BigQuery...")
                self._load_to_bigquery(loaded_periods)

            # Complete the run
            self.state_db.update_run_status(
                run_id, "completed",
                len(loaded_manifests),
                loaded_periods
            )

            print("✅ Pipeline completed successfully!")
            return self._build_summary(run_id, loaded_manifests, loaded_periods)

        except Exception as e:
            print(f"❌ Pipeline failed: {str(e)}")
            self.state_db.update_run_status(run_id, "failed", error_message=str(e))
            raise

    def _discover_and_mark_stale(self, run_id: str) -> List:
        """Discover manifests and mark any missing ones as stale."""
        # Discover current manifests
        discovered_manifests = self.manifest_discovery.discover_manifests()

        # Update discovered manifests with run_id
        for manifest in discovered_manifests:
            self.state_db.update_manifest_state(manifest.id, "discovered", run_id=run_id)

        # Mark stale manifests
        current_manifest_ids = [m.id for m in discovered_manifests]
        stale_count = self.state_db.mark_manifests_stale(current_manifest_ids)

        if stale_count > 0:
            print(f"⚠️  Marked {stale_count} manifests as stale")

        return discovered_manifests

    def _get_manifests_to_process(self, start_date: Optional[str], end_date: Optional[str]) -> List:
        """Get unprocessed manifests within the date range."""
        unprocessed = self.state_db.get_unprocessed_manifests()

        if not start_date and not end_date:
            return unprocessed

        # Filter by date range if specified
        filtered = []
        for manifest in unprocessed:
            billing_period = manifest['billing_period']

            if start_date and billing_period < start_date:
                continue
            if end_date and billing_period > end_date:
                continue

            filtered.append(manifest)

        return filtered

    def _extract_billing_files(self, manifests: List, run_id: str) -> List:
        """Extract billing files for the given manifests."""
        if not manifests:
            return []

        # Get date range from manifests for filtering
        billing_periods = [m['billing_period'] for m in manifests]
        start_date = min(billing_periods) if billing_periods else None
        end_date = max(billing_periods) if billing_periods else None

        try:
            # Update all manifests to run_id before extraction
            for manifest in manifests:
                self.state_db.update_manifest_state(
                    manifest['manifest_id'], "discovered", run_id=run_id
                )

            # Call the actual billing extractor
            stats = self.billing_extractor.extract_billing_files(
                start_date=start_date,
                end_date=end_date,
                staging_dir=self.config.staging_dir
            )

            print(f"   📦 Processed {stats['manifests_processed']} manifests")
            print(f"   📥 Downloaded {stats['files_downloaded']} files")
            if stats['errors'] > 0:
                print(f"   ❌ Errors: {stats['errors']}")

            # Return manifests that were successfully staged
            return self.state_db.get_manifests_by_state("staged")

        except Exception as e:
            # Mark all manifests as failed if extraction completely fails
            for manifest in manifests:
                self.state_db.update_manifest_state(
                    manifest['manifest_id'], "failed", str(e), run_id
                )
            print(f"❌ Billing extraction failed: {str(e)}")
            return []

    def _load_to_duckdb(self, manifests: List, run_id: str) -> List:
        """Load staged manifests to DuckDB."""
        if not manifests:
            return []

        try:
            # Use the DuckDBLoader with context manager
            with DuckDBLoader(self.duckdb_path, self.state_db) as loader:
                # Get date range from manifests for filtering
                billing_periods = [m['billing_period'] for m in manifests]
                start_date = min(billing_periods) if billing_periods else None
                end_date = max(billing_periods) if billing_periods else None

                # Load billing data
                stats = loader.load_billing_data(
                    staging_dir=self.config.staging_dir,
                    start_date=start_date,
                    end_date=end_date,
                    table_name="aws_billing_data"
                )

                print(f"   💾 Processed {stats['total_manifests']} manifests")
                print(f"   ✅ Successfully loaded {stats['loaded_manifests']} manifests")
                print(f"   📊 Total rows: {stats['total_rows']:,}")
                if stats['failed_manifests'] > 0:
                    print(f"   ❌ Failed: {stats['failed_manifests']} manifests")

                # Update all loaded manifests with run_id
                for manifest in manifests:
                    if manifest['manifest_id'] in [r['manifest_id'] for r in stats['results'] if r['status'] == 'loaded']:
                        self.state_db.update_manifest_state(
                            manifest['manifest_id'], "loaded", run_id=run_id
                        )

                # Return manifests that were successfully loaded
                return self.state_db.get_manifests_by_state("loaded")

        except Exception as e:
            # Mark all manifests as failed if loading completely fails
            for manifest in manifests:
                self.state_db.update_manifest_state(
                    manifest['manifest_id'], "failed", str(e), run_id
                )
            print(f"❌ DuckDB loading failed: {str(e)}")
            return []

    def _export_to_parquet(self, billing_periods: List[str]) -> None:
        """Export billing periods to Parquet files."""
        if not billing_periods:
            return

        try:
            # Use the ParquetExporter with context manager
            with ParquetExporter(self.duckdb_path, self.state_db, self.config.parquet_dir, "aws_billing_data") as exporter:
                # Validate table exists first
                if not exporter.validate_table_exists():
                    print("❌ No data found in DuckDB table")
                    return

                # Export billing periods
                results = exporter.export_billing_periods(
                    billing_periods,
                    vendor="aws",
                    overwrite=False,  # Don't overwrite by default
                    compression="snappy"
                )

                # Report results
                exported_count = sum(1 for status in results.values() if status == "exported")
                skipped_count = sum(1 for status in results.values() if status == "skipped")
                failed_count = sum(1 for status in results.values() if status == "failed")

                print(f"   📤 Exported: {exported_count} files")
                if skipped_count > 0:
                    print(f"   ⏭️  Skipped: {skipped_count} (already exist)")
                if failed_count > 0:
                    print(f"   ❌ Failed: {failed_count} exports")

                # Log individual failures
                for period, status in results.items():
                    if status == "failed":
                        print(f"      ❌ {period}")

        except Exception as e:
            print(f"❌ Parquet export failed: {str(e)}")

    def _load_to_bigquery(self, billing_periods: List[str]) -> None:
        """Load billing periods to BigQuery."""
        if not self.bigquery_loader:
            print("⏭️  Skipping BigQuery load - not configured")
            return

        if not billing_periods:
            return

        try:
            # Validate BigQuery connection first
            if not self.bigquery_loader.validate_bigquery_connection():
                print("❌ Cannot connect to BigQuery - check credentials and permissions")
                return

            # Load billing periods to BigQuery
            results = self.bigquery_loader.load_billing_periods(
                billing_periods,
                vendor="aws",
                overwrite=False  # Don't overwrite by default
            )

            # Report results
            loaded_count = sum(1 for status in results.values() if status == "loaded")
            skipped_count = sum(1 for status in results.values() if status == "skipped")
            failed_count = sum(1 for status in results.values() if status == "failed")

            print(f"   ☁️  Loaded: {loaded_count} billing periods")
            if skipped_count > 0:
                print(f"   ⏭️  Skipped: {skipped_count} (already exist)")
            if failed_count > 0:
                print(f"   ❌ Failed: {failed_count} loads")

            # Log individual failures
            for period, status in results.items():
                if status == "failed":
                    print(f"      ❌ {period}")

        except Exception as e:
            print(f"❌ BigQuery loading failed: {str(e)}")

    def _get_billing_periods_from_manifests(self, manifests: List) -> List[str]:
        """Extract unique billing periods from a list of manifests."""
        periods = set()
        for manifest in manifests:
            periods.add(manifest['billing_period'])
        return sorted(list(periods))

    def _dry_run_pipeline(self, start_date: Optional[str], end_date: Optional[str]) -> dict:
        """Show what would be processed without executing."""
        print("🔍 DRY RUN - No changes will be made")

        # Get unprocessed manifests
        unprocessed = self._get_manifests_to_process(start_date, end_date)
        billing_periods = self._get_billing_periods_from_manifests(unprocessed)

        print(f"📋 Would process {len(unprocessed)} manifests")
        print(f"📅 Billing periods: {', '.join(billing_periods) if billing_periods else 'None'}")

        for manifest in unprocessed:
            print(f"  - {manifest['billing_period']}: {manifest['manifest_id']}")

        return {
            'dry_run': True,
            'manifests_to_process': len(unprocessed),
            'billing_periods': billing_periods,
            'manifests': unprocessed
        }

    def _build_summary(self, run_id: str, manifests: List, billing_periods: List[str]) -> dict:
        """Build a summary of pipeline execution results."""
        return {
            'run_id': run_id,
            'manifests_processed': len(manifests),
            'billing_periods_processed': billing_periods,
            'manifests': [
                {
                    'id': m['manifest_id'],
                    'billing_period': m['billing_period'],
                    'state': m['state']
                } for m in manifests
            ]
        }

    def get_pipeline_status(self) -> dict:
        """Get current pipeline status summary."""
        manifest_summary = self.state_db.get_manifest_summary()

        # Get recent runs
        import sqlite3
        with sqlite3.connect(self.state_db.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("""
                SELECT * FROM runs
                ORDER BY started_at DESC
                LIMIT 5
            """)
            recent_runs = [dict(row) for row in cursor.fetchall()]

        return {
            'manifest_summary': manifest_summary,
            'recent_runs': recent_runs,
            'unprocessed_count': len(self.state_db.get_unprocessed_manifests())
        }
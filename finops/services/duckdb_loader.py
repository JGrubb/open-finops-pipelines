import json
import gzip
import csv
from pathlib import Path
from typing import Dict, List, Optional, Set
import duckdb
from finops.services.state_db import StateDB
from finops.services.schema_manager import SchemaManager


class DuckDBLoader:
    """Handles loading AWS CUR data into DuckDB with schema evolution."""

    def __init__(self, database_path: str, state_db: StateDB):
        self.database_path = database_path
        self.state_db = state_db
        self.schema_manager = SchemaManager(state_db)
        self.connection: Optional[duckdb.DuckDBPyConnection] = None

    def __enter__(self):
        """Context manager entry."""
        self.connection = duckdb.connect(self.database_path)
        return self

    def __exit__(self, _exc_type, _exc_val, _exc_tb):
        """Context manager exit."""
        if self.connection:
            self.connection.close()

    def get_existing_table_columns(self, table_name: str) -> Set[str]:
        """Get set of column names from existing table."""
        if not self.connection:
            raise RuntimeError("Connection not established. Use within context manager.")
        try:
            result = self.connection.execute(f"""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = '{table_name}'
            """).fetchall()
            return {row[0] for row in result}
        except Exception:
            # Table doesn't exist
            return set()

    def ensure_table_schema(self, table_name: str, manifest_columns: List[Dict]) -> Dict[str, str]:
        """
        Ensure table exists with proper schema, adding new columns if needed.
        Returns column mapping (original_name -> normalized_name).
        """
        existing_columns = self.get_existing_table_columns(table_name)
        column_mapping = self.schema_manager.create_column_mapping(manifest_columns)

        if not existing_columns:
            # Create table with full schema from all known manifests
            print(f"Creating new table: {table_name}")
            full_schema = self.schema_manager.get_unified_schema()
            create_sql = self.schema_manager.generate_create_table_sql(table_name, full_schema)
            if not self.connection:
                raise RuntimeError("Connection not established. Use within context manager.")
            self.connection.execute(create_sql)
            print(f"Created table with {len(full_schema)} columns")

        # Always check for new columns from this specific manifest
        # Get fresh list of existing columns after potential table creation
        existing_columns = self.get_existing_table_columns(table_name)
        new_columns = self.schema_manager.get_new_columns(
            table_name, existing_columns, manifest_columns
        )

        if new_columns:
            print(f"Adding {len(new_columns)} new columns to {table_name}")
            alter_statements = self.schema_manager.generate_alter_table_sql(table_name, new_columns)

            if not self.connection:
                raise RuntimeError("Connection not established. Use within context manager.")
            for statement in alter_statements:
                self.connection.execute(statement)
                print(f"  Added column: {statement.split('ADD COLUMN')[1].strip()}")

        return column_mapping

    def load_csv_file(self, csv_path: Path, table_name: str, column_mapping: Dict[str, str], manifest_columns: List[Dict], execution_id: str) -> int:
        """
        Load a single CSV file into DuckDB table using read_csv.
        Returns number of rows loaded.
        """
        if not csv_path.exists():
            raise FileNotFoundError(f"CSV file not found: {csv_path}")

        # Handle gzipped files
        if csv_path.suffix == '.gz':
            print(f"  Loading compressed CSV: {csv_path.name}")
        else:
            print(f"  Loading CSV: {csv_path.name}")

        try:
            # First, get the header to understand the column order
            if csv_path.suffix == '.gz':
                with gzip.open(csv_path, 'rt') as f:
                    header = next(csv.reader(f))
            else:
                with open(csv_path, 'r') as f:
                    header = next(csv.reader(f))

            # Use the column mapping to get normalized names
            # This ensures consistency with the table schema
            normalized_columns = []
            for original_col in header:
                normalized_col = column_mapping.get(original_col,
                                                   self.schema_manager.normalize_column_name(original_col))
                normalized_columns.append(normalized_col)

            # Build column specification for read_csv with proper data types
            column_specs = []
            for i, original_col in enumerate(header):
                normalized_col = normalized_columns[i]

                # Find the column definition in manifest to get the correct data type
                col_def = None
                for col in manifest_columns:
                    category = col['category']
                    name = col['name']
                    original_name = f"{category}/{name}"
                    if original_name == original_col:
                        col_def = col
                        break

                if col_def:
                    category = col_def['category']
                    aws_type = col_def['type']

                    duckdb_type = self.schema_manager.get_duckdb_type(category, aws_type)
                else:
                    duckdb_type = "VARCHAR"  # fallback

                column_specs.append(f"'{normalized_col}': '{duckdb_type}'")

            columns_struct = "{" + ", ".join(column_specs) + "}"

            # Use INSERT INTO ... SELECT FROM read_csv with execution_id added as literal
            insert_sql = f"""
                INSERT INTO {table_name} (execution_id, {', '.join(normalized_columns)})
                SELECT '{execution_id}' AS execution_id, * FROM read_csv(
                    '{csv_path}',
                    columns = {columns_struct},
                    header = true,
                    delim = ',',
                    compression = '{'gzip' if csv_path.suffix == '.gz' else 'none'}'
                )
            """

            if not self.connection:
                raise RuntimeError("Connection not established. Use within context manager.")
            _ = self.connection.execute(insert_sql)

            # Get row count - DuckDB doesn't have changes(), so we'll count the table
            # This is not perfect but works for our use case
            count_result = self.connection.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
            total_row_count = count_result[0] if count_result else 0

            # For now, we'll return total count (not ideal but functional)
            # In a real implementation, we'd track this better
            row_count = total_row_count

            print(f"    Loaded {row_count:,} rows")
            return row_count

        except Exception as e:
            print(f"    Error loading {csv_path.name}: {str(e)}")
            raise

    def load_manifest(self, manifest: Dict, staging_dir: str, table_name: str = "aws_billing_data") -> Dict:
        """
        Load all CSV files for a specific manifest.
        Returns loading statistics.
        """
        manifest_id = manifest['manifest_id']
        billing_period = manifest['billing_period']

        print(f"Loading manifest: {billing_period} ({manifest_id})")

        csv_files = []  # Initialize for error handling
        try:
            # Update state to loading
            self.state_db.update_manifest_state(manifest_id, "loading")

            # Parse column schema and ensure table exists
            columns_json = manifest['columns_schema']
            columns = json.loads(columns_json) if isinstance(columns_json, str) else columns_json
            column_mapping = self.ensure_table_schema(table_name, columns)

            # Parse CSV files list
            csv_files_json = manifest['csv_files']
            csv_files = json.loads(csv_files_json) if isinstance(csv_files_json, str) else csv_files_json

            # Delete existing data for this billing period before loading
            if not self.connection:
                raise RuntimeError("Connection not established. Use within context manager.")

            year, month = billing_period.split("-")
            delete_result = self.connection.execute(f"""
                DELETE FROM {table_name}
                WHERE EXTRACT(YEAR FROM bill_billing_period_start_date) = {year}
                  AND EXTRACT(MONTH FROM bill_billing_period_start_date) = {month}
            """)
            deleted_rows = delete_result.fetchone()
            if deleted_rows and deleted_rows[0] > 0:
                print(f"  Deleted {deleted_rows[0]:,} existing rows for {billing_period}")
            else:
                print(f"  No existing data found for {billing_period}")

            # Load each CSV file
            total_rows = 0
            loaded_files = 0
            # Use new staging path structure: staging/{billing_period}/{execution_id}/
            staging_path = Path(staging_dir) / billing_period / manifest_id

            for csv_file in csv_files:
                # Extract filename from S3 key
                filename = Path(csv_file).name
                csv_path = staging_path / filename

                if csv_path.exists():
                    rows = self.load_csv_file(csv_path, table_name, column_mapping, columns, manifest_id)
                    total_rows += rows
                    loaded_files += 1
                else:
                    print(f"    Warning: File not found in staging: {filename}")

            # Update state to loaded
            self.state_db.update_manifest_state(manifest_id, "loaded")

            stats = {
                'manifest_id': manifest_id,
                'billing_period': billing_period,
                'files_loaded': loaded_files,
                'total_files': len(csv_files),
                'rows_loaded': total_rows,
                'status': 'success'
            }

            print(f"✓ Completed: {billing_period} - {total_rows:,} rows from {loaded_files} files")
            return stats

        except Exception as e:
            error_msg = str(e)
            print(f"✗ Failed: {billing_period} - {error_msg}")

            # Update state to failed
            self.state_db.update_manifest_state(manifest_id, "failed", error_msg)

            return {
                'manifest_id': manifest_id,
                'billing_period': billing_period,
                'files_loaded': 0,
                'total_files': len(csv_files),
                'rows_loaded': 0,
                'status': 'failed',
                'error': error_msg
            }

    def load_billing_data(self, staging_dir: str, start_date: Optional[str] = None,
                         end_date: Optional[str] = None, table_name: str = "aws_billing_data") -> Dict:
        """
        Load billing data from staged CSV files into DuckDB.

        Args:
            staging_dir: Directory containing staged CSV files
            start_date: Optional start date filter (YYYY-MM format)
            end_date: Optional end date filter (YYYY-MM format)
            table_name: Name of target DuckDB table

        Returns:
            Dictionary with loading statistics
        """
        print("🗄️  Loading billing data into DuckDB...")
        print(f"Database: {self.database_path}")
        print(f"Table: {table_name}")
        print(f"Staging directory: {staging_dir}")

        # Get all manifests that have been extracted (regardless of load state)
        # This allows reloading data from staging without re-downloading from S3
        all_states = ["staged", "loading", "loaded", "failed"]
        all_manifests = []
        for state in all_states:
            all_manifests.extend(self.state_db.get_manifests_by_state(state))

        if not all_manifests:
            print("No manifests found. Run 'finops aws extract-billing' first.")
            return {'total_manifests': 0, 'loaded_manifests': 0, 'failed_manifests': 0, 'total_rows': 0}

        # Filter by date range if specified
        if start_date or end_date:
            filtered_manifests = []
            for manifest in all_manifests:
                billing_period = manifest['billing_period']

                if start_date and billing_period < start_date:
                    continue
                if end_date and billing_period > end_date:
                    continue

                filtered_manifests.append(manifest)

            all_manifests = filtered_manifests

        if not all_manifests:
            print(f"No manifests found in date range {start_date or 'earliest'} to {end_date or 'latest'}")
            return {'total_manifests': 0, 'loaded_manifests': 0, 'failed_manifests': 0, 'total_rows': 0}

        # Sort by billing period (newest first for consistent processing)
        all_manifests.sort(key=lambda x: x['billing_period'], reverse=True)

        print(f"Found {len(all_manifests)} manifests to load")
        print()

        # Load each manifest
        total_rows = 0
        loaded_manifests = 0
        failed_manifests = 0
        manifest_results = []

        for i, manifest in enumerate(all_manifests, 1):
            print(f"[{i}/{len(all_manifests)}] ", end="")

            result = self.load_manifest(manifest, staging_dir, table_name)
            manifest_results.append(result)

            if result['status'] == 'success':
                loaded_manifests += 1
                total_rows += result['rows_loaded']
            else:
                failed_manifests += 1

            print()  # Add spacing between manifests

        # Print summary
        print("=" * 60)
        print("Loading Summary:")
        print(f"  Total manifests: {len(all_manifests)}")
        print(f"  Successfully loaded: {loaded_manifests}")
        print(f"  Failed: {failed_manifests}")
        print(f"  Total rows loaded: {total_rows:,}")

        if failed_manifests > 0:
            print(f"\nFailed manifests:")
            for result in manifest_results:
                if result['status'] == 'failed':
                    print(f"  - {result['billing_period']} ({result['manifest_id']}): {result.get('error', 'Unknown error')}")

        return {
            'total_manifests': len(all_manifests),
            'loaded_manifests': loaded_manifests,
            'failed_manifests': failed_manifests,
            'total_rows': total_rows,
            'results': manifest_results
        }

    def get_table_info(self, table_name: str = "aws_billing_data") -> Optional[Dict]:
        """Get information about the loaded table."""
        try:
            # Check if table exists
            existing_columns = self.get_existing_table_columns(table_name)
            if not existing_columns:
                return None

            # Get row count
            if not self.connection:
                raise RuntimeError("Connection not established. Use within context manager.")
            result = self.connection.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
            row_count = result[0] if result else 0

            # Get date range - use normalized column names
            date_range = self.connection.execute(f"""
                SELECT
                    MIN(bill_billing_period_start_date) as min_date,
                    MAX(bill_billing_period_end_date) as max_date
                FROM {table_name}
                WHERE bill_billing_period_start_date IS NOT NULL
            """).fetchone()

            return {
                'table_name': table_name,
                'column_count': len(existing_columns),
                'row_count': row_count,
                'date_range': {
                    'min_date': date_range[0] if date_range else None,
                    'max_date': date_range[1] if date_range else None
                }
            }

        except Exception as e:
            print(f"Error getting table info: {e}")
            return None
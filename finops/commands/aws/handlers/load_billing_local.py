"""Load billing local command handler."""
from finops.commands.aws.utils import (
    handle_command_errors,
    load_and_validate_config,
    discover_and_filter_manifests,
    ensure_duckdb_path,
)
from finops.services.duckdb_loader import DuckDBLoader
from finops.services.state_checker import StateChecker


@handle_command_errors
def load_billing_local(config_path, args):
    """Load billing data to local DuckDB."""
    print("Loading billing data to local DuckDB...")

    # Load configuration
    config = load_and_validate_config(config_path)

    # Create data directory if needed
    ensure_duckdb_path(config)

    print(f"DuckDB database: {config.duckdb_path}")
    print(f"Staging directory: {config.staging_dir}")
    print()

    # Discover and filter manifests
    print("Discovering manifests...")
    start_date = getattr(args, 'start_date', None)
    end_date = getattr(args, 'end_date', None)

    state_checker = StateChecker(config)
    manifests = discover_and_filter_manifests(config, state_checker, start_date, end_date)

    if not manifests:
        print("No manifests to load")
        return

    # Load data using DuckDB loader
    with DuckDBLoader(config.duckdb_path) as loader:
        stats = loader.load_billing_data_from_manifests(
            manifests=manifests,
            staging_dir=config.staging_dir,
            table_name="aws_billing_data"
        )

        # Display final results
        print()
        if stats['loaded_executions'] > 0:
            print("Loading completed successfully!")
            if config.duckdb:
                print(f"Database location: {config.duckdb_path}")

            # Show table info
            table_info = loader.get_table_info("aws_billing_data")
            if table_info:
                print(f"\nTable Information:")
                print(f"  Table: {table_info['table_name']}")
                print(f"  Columns: {table_info['column_count']}")
                print(f"  Rows: {table_info['row_count']:,}")
                if table_info['date_range']['min_date']:
                    print(f"  Date range: {table_info['date_range']['min_date']} to {table_info['date_range']['max_date']}")

            print(f"\nNext steps:")
            if config.duckdb:
                print(f"  - Query data: duckdb {config.duckdb_path}")
            print(f"  - Export to Parquet: finops aws export-parquet")
            print(f"  - Load to BigQuery: finops aws load-billing-remote")
        else:
            print("No data was loaded.")
            print("Run 'finops aws extract-billing' to download CSV files first.")

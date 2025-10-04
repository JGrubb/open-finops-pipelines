"""Load billing remote command handler."""
import sys
from finops.config import FinopsConfig
from finops.commands.aws.utils import handle_command_errors
from finops.services.bigquery_loader import BigQueryLoader


@handle_command_errors
def load_billing_remote(config_path, args):
    """Load billing data to remote warehouse."""
    print("Loading billing data to remote warehouse...")

    # Load configuration
    config = FinopsConfig.from_file(config_path)

    # Validate BigQuery is configured
    if not config.bigquery:
        raise ValueError("BigQuery configuration is missing")

    print(f"Project: {config.bigquery.project_id}")
    print(f"Dataset: {config.bigquery.dataset_id}")
    print(f"Table: {config.bigquery.table_id}")
    print()

    # Initialize BigQuery loader
    loader = BigQueryLoader(config.bigquery, config.parquet_dir)

    # Validate connections
    if not loader.validate_bigquery_connection():
        raise ValueError("Cannot connect to BigQuery. Check credentials and dataset access.")

    # Get available billing periods from exported Parquet files
    available_periods = loader.get_available_billing_periods()
    if not available_periods:
        raise ValueError("No exported Parquet files found. Run 'finops aws export-parquet' first.")

    print(f"Found {len(available_periods)} exported Parquet files:")
    for period in available_periods:
        print(f"  - {period}")
    print()

    # Handle date range filtering
    periods_to_load = available_periods
    if hasattr(args, 'start_date') and args.start_date:
        periods_to_load = [p for p in periods_to_load if p >= args.start_date]
    if hasattr(args, 'end_date') and args.end_date:
        periods_to_load = [p for p in periods_to_load if p <= args.end_date]

    if not periods_to_load:
        print("No billing periods match the specified date range")
        return

    # Load billing data to BigQuery
    overwrite = getattr(args, 'overwrite', False)
    print(f"Loading {len(periods_to_load)} billing periods to BigQuery...")
    if overwrite:
        print("Overwrite mode enabled")
    print()

    results = loader.load_billing_periods(
        periods_to_load,
        vendor="aws",
        overwrite=overwrite
    )

    # Display summary
    loaded_count = len([r for r in results.values() if r == "loaded"])
    skipped_count = len([r for r in results.values() if r == "skipped"])
    failed_count = len([r for r in results.values() if r == "failed"])

    print()
    print("Load Summary:")
    print(f"  Loaded: {loaded_count}")
    print(f"  Skipped: {skipped_count}")
    print(f"  Failed: {failed_count}")

    if failed_count > 0:
        print()
        print("Some loads failed. Check logs above for details.")
        sys.exit(1)
    else:
        print()
        print("All billing periods loaded successfully!")

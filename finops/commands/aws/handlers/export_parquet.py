"""Export parquet command handler."""
from pathlib import Path
from finops.commands.aws.utils import (
    handle_command_errors,
    load_and_validate_config,
    validate_date_range,
)
from finops.services.parquet_exporter import ParquetExporter


@handle_command_errors
def export_parquet(config_path, args):
    """Export to Parquet format."""
    print("Exporting to Parquet format...")

    # Load configuration
    config = load_and_validate_config(Path(config_path))

    # Override parquet directory if specified
    parquet_dir = args.output_dir if args.output_dir else config.parquet_dir

    # Validate DuckDB configuration
    if not config.duckdb:
        raise ValueError("DuckDB configuration is missing")

    # Validate date format if provided
    if not validate_date_range(args.start_date, args.end_date):
        return

    # Export using ParquetExporter service
    with ParquetExporter(config.duckdb_path, parquet_dir, "aws_billing_data") as exporter:
        # Validate DuckDB table exists
        if not exporter.validate_table_exists():
            print("Error: aws_billing_data table not found or empty in DuckDB")
            print("   Run 'finops aws load-billing-local' to load data first")
            return

        # Get available billing periods from actual DuckDB data
        available_periods = exporter.get_available_billing_periods("aws")
        if not available_periods:
            print("No billing data found in DuckDB. Run 'finops aws load-billing-local' first.")
            return

        # Filter billing periods by date range
        periods_to_export = available_periods
        if args.start_date:
            periods_to_export = [p for p in periods_to_export if p >= args.start_date]
        if args.end_date:
            periods_to_export = [p for p in periods_to_export if p <= args.end_date]

        if not periods_to_export:
            print(f"No billing periods found in specified date range")
            if args.start_date or args.end_date:
                print(f"   Date range: {args.start_date or 'earliest'} to {args.end_date or 'latest'}")
            return

        print(f"Found {len(periods_to_export)} billing periods to export:")
        for period in sorted(periods_to_export):
            print(f"   - {period}")
        print(f"Output directory: {parquet_dir}")
        print(f"Compression: {args.compression}")

        # Export billing periods
        print("\nStarting export...")
        results = exporter.export_billing_periods(
            periods_to_export,
            vendor="aws",
            overwrite=args.overwrite,
            compression=args.compression
        )

        # Show results summary
        exported_count = sum(1 for status in results.values() if status == "exported")
        skipped_count = sum(1 for status in results.values() if status == "skipped")
        failed_count = sum(1 for status in results.values() if status == "failed")

        print(f"\nExport Summary:")
        print(f"   Exported: {exported_count}")
        if skipped_count > 0:
            print(f"   Skipped: {skipped_count} (use --overwrite to force)")
        if failed_count > 0:
            print(f"   Failed: {failed_count}")

        if exported_count > 0:
            print(f"\nExport completed successfully!")
            print(f"Files saved to: {parquet_dir}")

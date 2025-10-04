"""Run pipeline command handler."""
from finops.commands.aws.utils import (
    handle_command_errors,
    load_and_validate_config,
    validate_date_range,
    discover_and_filter_manifests,
    ensure_duckdb_path,
)
from finops.services.billing_extractor import BillingExtractorService
from finops.services.duckdb_loader import DuckDBLoader
from finops.services.parquet_exporter import ParquetExporter
from finops.services.bigquery_loader import BigQueryLoader


@handle_command_errors
def run_pipeline(config_path, args):
    """Run the complete meta-pipeline."""
    print("Running complete AWS billing pipeline...\n")

    # Load and validate configuration
    config = load_and_validate_config(config_path)

    # Validate date format
    if not validate_date_range(args.start_date, args.end_date):
        return

    # Show configuration
    print(f"Configuration:")
    print(f"  S3: s3://{config.aws.bucket}/{config.aws.prefix}")
    print(f"  Export: {config.aws.export_name} (CUR {config.aws.cur_version})")
    if args.start_date or args.end_date:
        print(f"  Date range: {args.start_date or 'earliest'} to {args.end_date or 'latest'}")
    if args.dry_run:
        print(f"  Mode: DRY RUN")
    print()

    # Step 1: Discover manifests
    print("Step 1/5: Discovering manifests...")
    # For full pipeline, don't check state - process everything from S3
    manifests = discover_and_filter_manifests(config, state_checker=None,
                                             start_date=args.start_date,
                                             end_date=args.end_date)

    if not manifests:
        print("  No manifests to process")
        return

    billing_periods = sorted(list(set(m.billing_period for m in manifests)))
    print(f"  Found {len(manifests)} manifest(s) covering {len(billing_periods)} period(s)")
    print(f"  Periods: {', '.join(billing_periods)}")

    if args.dry_run:
        print("\nDry run complete (no data processed)")
        return

    # Step 2: Extract billing files
    print(f"\nStep 2/5: Extracting billing files...")
    extractor = BillingExtractorService(config.aws)
    extract_stats = extractor.extract_billing_files(manifests, config.staging_dir)
    print(f"  Files downloaded: {extract_stats['files_downloaded']}")
    if extract_stats['errors'] > 0:
        print(f"  Errors: {extract_stats['errors']}")

    # Step 3: Load to DuckDB
    print(f"\nStep 3/5: Loading to DuckDB...")
    ensure_duckdb_path(config)

    with DuckDBLoader(config.duckdb_path) as loader:
        load_stats = loader.load_billing_data_from_manifests(
            manifests=manifests,
            staging_dir=config.staging_dir,
            table_name="aws_billing_data"
        )
        print(f"  Loaded: {load_stats['loaded_executions']} execution(s)")
        print(f"  Total rows: {load_stats['total_rows']:,}")

    # Step 4: Export to Parquet (by execution with execution_id)
    print(f"\nStep 4/5: Exporting to Parquet...")
    with ParquetExporter(config.duckdb_path, config.parquet_dir, "aws_billing_data") as exporter:
        export_stats = exporter.export_billing_data_by_execution(
            manifests,
            vendor="aws",
            overwrite=False,
            compression="snappy"
        )
        exported = sum(1 for s in export_stats.values() if s == "exported")
        skipped = sum(1 for s in export_stats.values() if s == "skipped")
        print(f"  Exported: {exported}, Skipped: {skipped}")

    # Step 5: Load to BigQuery (only new execution_ids)
    if config.bigquery:
        print(f"\nStep 5/5: Loading to BigQuery...")
        bq_loader = BigQueryLoader(config.bigquery, config.parquet_dir)
        bq_stats = bq_loader.load_billing_data_by_execution(
            manifests,
            vendor="aws",
            overwrite=False
        )
        loaded = sum(1 for s in bq_stats.values() if s == "loaded")
        skipped = sum(1 for s in bq_stats.values() if s == "skipped")
        print(f"  Loaded: {loaded}, Skipped: {skipped}")
    else:
        print(f"\nStep 5/5: Skipping BigQuery (not configured)")

    # Summary
    print(f"\nPipeline completed successfully!")
    print(f"\nSummary:")
    print(f"  Manifests: {len(manifests)}")
    print(f"  Billing periods: {', '.join(billing_periods)}")
    print(f"  Rows loaded: {load_stats['total_rows']:,}")
    if config.duckdb:
        print(f"\nData locations:")
        print(f"  - DuckDB: {config.duckdb_path}")
        print(f"  - Parquet: {config.parquet_dir}/")
    if config.bigquery:
        print(f"  - BigQuery: {config.bigquery.project_id}.{config.bigquery.dataset_id}.{config.bigquery.table_id}")

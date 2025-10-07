"""Run pipeline command handler."""
from finops.commands.aws.utils import (
    handle_command_errors,
    load_and_validate_config,
    validate_date_range,
    discover_and_filter_manifests,
    ensure_duckdb_path,
)
from finops.services.billing_extractor import BillingExtractorService
from finops.services.monthly_pipeline import MonthlyPipelineOrchestrator
from finops.services.bigquery_loader import BigQueryLoader
from finops.services.state_checker import StateChecker


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
    # Check state to only process changed manifests
    state_checker = StateChecker(config)
    manifests = discover_and_filter_manifests(config, state_checker=state_checker,
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
    print(f"\nStep 2/4: Extracting billing files...")
    extractor = BillingExtractorService(config.aws)
    extract_stats = extractor.extract_billing_files(manifests, config.staging_dir)
    print(f"  Files downloaded: {extract_stats['files_downloaded']}")
    if extract_stats['errors'] > 0:
        print(f"  Errors: {extract_stats['errors']}")

    # Step 3: Load to DuckDB and Export to Parquet (memory-optimized monthly pipeline)
    print(f"\nStep 3/4: Processing billing data (monthly pipeline)...")
    ensure_duckdb_path(config)

    pipeline = MonthlyPipelineOrchestrator(
        duckdb_path=config.duckdb_path,
        staging_dir=config.staging_dir,
        parquet_dir=config.parquet_dir,
        table_name="aws_billing_data"
    )

    pipeline_stats = pipeline.process_monthly(
        manifests=manifests,
        vendor="aws",
        compression="snappy",
        overwrite_parquet=False
    )

    print(f"  Months processed: {pipeline_stats['successful_months']}/{pipeline_stats['total_months']}")
    print(f"  Total rows: {pipeline_stats['total_rows_loaded']:,}")

    # Step 4: Load to BigQuery (only new execution_ids)
    if config.bigquery:
        print(f"\nStep 4/4: Loading to BigQuery...")
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
        print(f"\nStep 4/4: Skipping BigQuery (not configured)")

    # Summary
    print(f"\nPipeline completed successfully!")
    print(f"\nSummary:")
    print(f"  Manifests: {len(manifests)}")
    print(f"  Billing periods: {', '.join(billing_periods)}")
    print(f"  Rows loaded: {pipeline_stats['total_rows_loaded']:,}")
    if config.duckdb:
        print(f"\nData locations:")
        print(f"  - DuckDB: {config.duckdb_path} (flushed between months)")
        print(f"  - Parquet: {config.parquet_dir}/")
    if config.bigquery:
        print(f"  - BigQuery: {config.bigquery.project_id}.{config.bigquery.dataset_id}.{config.bigquery.table_id}")

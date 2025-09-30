from pathlib import Path


def setup_aws_parser(subparsers):
    """Set up the AWS subcommand parser."""
    aws_parser = subparsers.add_parser(
        "aws",
        help="AWS billing data operations"
    )

    aws_subparsers = aws_parser.add_subparsers(
        dest="aws_command",
        help="AWS commands",
        required=True
    )

    # discover-manifests command
    discover_parser = aws_subparsers.add_parser(
        "discover-manifests",
        help="Import AWS CUR billing data"
    )
    discover_parser.add_argument(
        "--bucket",
        help="S3 bucket containing CUR data"
    )
    discover_parser.add_argument(
        "--prefix",
        help="S3 prefix path to CUR data"
    )
    discover_parser.add_argument(
        "--export-name",
        help="CUR export name"
    )
    discover_parser.add_argument(
        "--cur-version",
        choices=["v1", "v2"],
        help="CUR version (default: from config or v2)"
    )
    discover_parser.add_argument(
        "--region",
        help="AWS region (default: us-east-1)"
    )
    discover_parser.set_defaults(func=discover_manifests)

    # extract-manifests command
    extract_parser = aws_subparsers.add_parser(
        "extract-manifests",
        help="List available CUR manifest files"
    )
    extract_parser.set_defaults(func=extract_manifests)

    # show-state command
    state_parser = aws_subparsers.add_parser(
        "show-state",
        help="Show previous pipeline executions and their state"
    )
    state_parser.set_defaults(func=show_state)

    # extract-billing command
    extract_billing_parser = aws_subparsers.add_parser(
        "extract-billing",
        help="Extract billing files from S3 to staging directory"
    )
    extract_billing_parser.add_argument(
        "--start-date",
        help="Start date for billing period (YYYY-MM format, e.g., 2024-01)"
    )
    extract_billing_parser.add_argument(
        "--end-date",
        help="End date for billing period (YYYY-MM format, e.g., 2024-12)"
    )
    extract_billing_parser.add_argument(
        "--staging-dir",
        help="Directory to download CSV files to (default: ./staging)"
    )
    extract_billing_parser.set_defaults(func=extract_billing)

    # load-billing-local command
    load_local_parser = aws_subparsers.add_parser(
        "load-billing-local",
        help="Load staged billing files into database"
    )
    load_local_parser.add_argument(
        "--start-date",
        help="Start date for billing period (YYYY-MM format, e.g., 2024-01)"
    )
    load_local_parser.add_argument(
        "--end-date",
        help="End date for billing period (YYYY-MM format, e.g., 2024-12)"
    )
    load_local_parser.set_defaults(func=load_billing_local)

    # export-parquet command
    export_parser = aws_subparsers.add_parser(
        "export-parquet",
        help="Exports each month to Parquet format"
    )
    export_parser.add_argument(
        "--output-dir",
        help="Output directory for Parquet files (overrides config)"
    )
    export_parser.add_argument(
        "--start-date",
        help="Start date for export range (YYYY-MM format)"
    )
    export_parser.add_argument(
        "--end-date",
        help="End date for export range (YYYY-MM format)"
    )
    export_parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing Parquet files"
    )
    export_parser.add_argument(
        "--compression",
        default="snappy",
        choices=["snappy", "gzip", "lz4", "zstd"],
        help="Parquet compression type (default: snappy)"
    )
    export_parser.set_defaults(func=export_parquet)

    # load-billing-remote command
    load_remote_parser = aws_subparsers.add_parser(
        "load-billing-remote",
        help="Load billing data to remote warehouse"
    )
    load_remote_parser.add_argument(
        "--start-date",
        help="Start date for loading (YYYY-MM format)",
        metavar="YYYY-MM"
    )
    load_remote_parser.add_argument(
        "--end-date",
        help="End date for loading (YYYY-MM format)",
        metavar="YYYY-MM"
    )
    load_remote_parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing data in remote warehouse"
    )
    load_remote_parser.set_defaults(func=load_billing_remote)

    # run-pipeline command - meta-pipeline that runs the complete flow
    pipeline_parser = aws_subparsers.add_parser(
        "run-pipeline",
        help="Run the complete pipeline: discover → extract → load → export → load-remote"
    )
    pipeline_parser.add_argument(
        "--start-date",
        help="Start date for billing period (YYYY-MM format, e.g., 2024-01)"
    )
    pipeline_parser.add_argument(
        "--end-date",
        help="End date for billing period (YYYY-MM format, e.g., 2024-12)"
    )
    pipeline_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be processed without executing"
    )
    pipeline_parser.set_defaults(func=run_pipeline)


def discover_manifests(config_path, args):
    """Discover AWS CUR manifests from S3."""
    from finops.config import FinopsConfig
    from finops.services.manifest_discovery import ManifestDiscoveryService
    from finops.services.state_checker import StateChecker

    print("🔍 Discovering AWS CUR manifests...")

    try:
        # Convert args to dict for config loading
        cli_args = {
            "bucket": args.bucket,
            "prefix": args.prefix,
            "export_name": args.export_name,
            "cur_version": args.cur_version,
            "region": args.region
        }

        # Load configuration with CLI precedence
        config = FinopsConfig.from_cli_args(config_path, cli_args)
        config.validate()

        print(f"Bucket: {config.aws.bucket}")
        print(f"Prefix: {config.aws.prefix}")
        print(f"Export: {config.aws.export_name}")
        print(f"Version: {config.aws.cur_version}")
        print()

        # Initialize state checker to query destination databases
        state_checker = StateChecker(config)

        # Discover manifests
        discovery_service = ManifestDiscoveryService(config.aws, state_checker)
        manifests = discovery_service.discover_manifests()

        # Display results
        summary = discovery_service.get_manifest_summary(manifests)
        print(summary)

        if manifests:
            print(f"\nNext step: Use 'finops aws extract-billing' to download CSV files")

    except Exception as e:
        print(f"Error: {e}")
        exit(1)


def extract_manifests(config, args):
    """Extract manifest files (deprecated)."""
    print("This command has been deprecated.")
    print("Manifests are now automatically discovered from S3 when running:")
    print("  finops aws extract-billing")
    print("  finops aws load-billing-local")
    print("\nNo separate manifest extraction step is needed.")


def show_state(config_path, args):
    """Show pipeline execution state (deprecated)."""
    print("This command has been deprecated.")
    print("\nThe pipeline now uses filesystem and destination databases for state.")
    print("\nTo check state:")
    print("  - Staging directory shows what's downloaded")
    print("  - Run 'finops aws discover-manifests' to see what's loaded vs available in S3")
    print("  - Query DuckDB or BigQuery directly to see loaded data")


def extract_billing(config_path, args):
    """Extract billing files from S3."""
    from finops.config import FinopsConfig
    from finops.services.manifest_discovery import ManifestDiscoveryService
    from finops.services.billing_extractor import BillingExtractorService
    from finops.services.state_checker import StateChecker

    print("💾 Extracting billing files from S3...")

    try:
        # Load configuration
        config = FinopsConfig.from_cli_args(config_path, {})
        config.validate()

        # Validate date arguments
        start_date = args.start_date
        end_date = args.end_date
        staging_dir = args.staging_dir or config.staging_dir

        if start_date and len(start_date) != 7:
            print("Error: --start-date must be in YYYY-MM format (e.g., 2024-01)")
            exit(1)

        if end_date and len(end_date) != 7:
            print("Error: --end-date must be in YYYY-MM format (e.g., 2024-12)")
            exit(1)

        # Show what we're doing
        if start_date and end_date:
            print(f"Date range: {start_date} to {end_date}")
        elif start_date:
            print(f"From: {start_date} onwards")
        elif end_date:
            print(f"Up to: {end_date}")
        else:
            print("All manifests")

        print(f"Staging directory: {staging_dir}")
        print()

        # Discover manifests from S3
        print("Discovering manifests...")
        state_checker = StateChecker(config)
        discovery_service = ManifestDiscoveryService(config.aws, state_checker)
        manifests = discovery_service.discover_manifests()

        # Filter by date range if specified
        if start_date or end_date:
            filtered_manifests = []
            for manifest in manifests:
                billing_period = manifest.billing_period
                if start_date and billing_period < start_date:
                    continue
                if end_date and billing_period > end_date:
                    continue
                filtered_manifests.append(manifest)
            manifests = filtered_manifests

        if not manifests:
            print("No manifests to extract")
            return

        print(f"Found {len(manifests)} manifest(s) to extract")
        print()

        # Initialize billing extractor
        extractor = BillingExtractorService(config.aws)

        # Extract files
        stats = extractor.extract_billing_files(manifests, staging_dir)

        # Display results
        print(f"\nExtraction complete:")
        print(f"  Manifests processed: {stats['manifests_processed']}")
        print(f"  Files downloaded: {stats['files_downloaded']}")
        if stats['errors'] > 0:
            print(f"  Errors: {stats['errors']}")

        if stats['manifests_processed'] > 0:
            print(f"\nNext step: Use 'finops aws load-billing-local' to load staged files into DuckDB")

    except Exception as e:
        print(f"Error: {e}")
        exit(1)


def load_billing_local(config_path, args):
    """Load billing data to local DuckDB."""
    from finops.config import FinopsConfig
    from finops.services.manifest_discovery import ManifestDiscoveryService
    from finops.services.state_checker import StateChecker
    from finops.services.duckdb_loader import DuckDBLoader
    from pathlib import Path

    print("🗄️  Loading billing data to local DuckDB...")

    try:
        # Load configuration
        config = FinopsConfig.from_cli_args(config_path, {})

        # Create data directory if needed
        duckdb_path = Path(config.duckdb.database_path)
        duckdb_path.parent.mkdir(parents=True, exist_ok=True)

        print(f"DuckDB database: {config.duckdb.database_path}")
        print(f"Staging directory: {config.staging_dir}")
        print()

        # Discover manifests (filters out already loaded)
        print("Discovering manifests...")
        state_checker = StateChecker(config)
        discovery_service = ManifestDiscoveryService(config.aws, state_checker)
        manifests = discovery_service.discover_manifests()

        # Filter by date range if specified
        start_date = getattr(args, 'start_date', None)
        end_date = getattr(args, 'end_date', None)

        if start_date or end_date:
            filtered_manifests = []
            for manifest in manifests:
                billing_period = manifest.billing_period
                if start_date and billing_period < start_date:
                    continue
                if end_date and billing_period > end_date:
                    continue
                filtered_manifests.append(manifest)
            manifests = filtered_manifests

        if not manifests:
            print("No manifests to load")
            return

        # Load data using DuckDB loader
        with DuckDBLoader(config.duckdb.database_path) as loader:
            stats = loader.load_billing_data_from_manifests(
                manifests=manifests,
                staging_dir=config.staging_dir,
                table_name="aws_billing_data"
            )

            # Display final results
            print()
            if stats['loaded_executions'] > 0:
                print("🎉 Loading completed successfully!")
                print(f"Database location: {config.duckdb.database_path}")

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
                print(f"  • Query data: duckdb {config.duckdb.database_path}")
                print(f"  • Export to Parquet: finops aws export-parquet")
                print(f"  • Load to BigQuery: finops aws load-billing-remote")
            else:
                print("No data was loaded.")
                print("Run 'finops aws extract-billing' to download CSV files first.")

    except Exception as e:
        print(f"Error: {e}")
        exit(1)


def export_parquet(config_path, args):
    """Export to Parquet format."""
    from pathlib import Path
    from finops.config import FinopsConfig
    from finops.services.parquet_exporter import ParquetExporter
    import re

    print("📁 Exporting to Parquet format...")

    try:
        # Load configuration
        config = FinopsConfig.from_cli_args(Path(config_path), {})

        # Override parquet directory if specified
        parquet_dir = args.output_dir if args.output_dir else config.parquet_dir

        # Validate date format if provided
        if args.start_date and not re.match(r'^\d{4}-\d{2}$', args.start_date):
            print("❌ Error: start-date must be in YYYY-MM format")
            return
        if args.end_date and not re.match(r'^\d{4}-\d{2}$', args.end_date):
            print("❌ Error: end-date must be in YYYY-MM format")
            return

        # Export using ParquetExporter service
        with ParquetExporter(config.duckdb.database_path, parquet_dir, "aws_billing_data") as exporter:
            # Validate DuckDB table exists
            if not exporter.validate_table_exists():
                print("❌ Error: aws_billing_data table not found or empty in DuckDB")
                print("   Run 'finops aws load-billing-local' to load data first")
                return

            # Get available billing periods from actual DuckDB data
            available_periods = exporter.get_available_billing_periods("aws")
            if not available_periods:
                print("❌ No billing data found in DuckDB. Run 'finops aws load-billing-local' first.")
                return

            # Filter billing periods by date range
            periods_to_export = available_periods
            if args.start_date:
                periods_to_export = [p for p in periods_to_export if p >= args.start_date]
            if args.end_date:
                periods_to_export = [p for p in periods_to_export if p <= args.end_date]

            if not periods_to_export:
                print(f"❌ No billing periods found in specified date range")
                if args.start_date or args.end_date:
                    print(f"   Date range: {args.start_date or 'earliest'} to {args.end_date or 'latest'}")
                return

            print(f"📊 Found {len(periods_to_export)} billing periods to export:")
            for period in sorted(periods_to_export):
                print(f"   • {period}")
            print(f"📁 Output directory: {parquet_dir}")
            print(f"🗜️  Compression: {args.compression}")

            # Export billing periods
            print("\n📤 Starting export...")
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

            print(f"\n📋 Export Summary:")
            print(f"   ✅ Exported: {exported_count}")
            if skipped_count > 0:
                print(f"   ⏭️  Skipped: {skipped_count} (use --overwrite to force)")
            if failed_count > 0:
                print(f"   ❌ Failed: {failed_count}")

            if exported_count > 0:
                print(f"\n🎉 Export completed successfully!")
                print(f"📁 Files saved to: {parquet_dir}")

    except FileNotFoundError as e:
        print(f"❌ Configuration file not found: {e}")
    except ValueError as e:
        print(f"❌ Configuration error: {e}")
    except Exception as e:
        print(f"❌ Export failed: {e}")
        import traceback
        traceback.print_exc()


def load_billing_remote(config_path, args):
    """Load billing data to remote warehouse."""
    from finops.config import FinopsConfig
    from finops.services.bigquery_loader import BigQueryLoader

    print("☁️  Loading billing data to remote warehouse...")

    try:
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
            print("⚠️  Overwrite mode enabled")
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
        print("📊 Load Summary:")
        print(f"  Loaded: {loaded_count}")
        print(f"  Skipped: {skipped_count}")
        print(f"  Failed: {failed_count}")

        if failed_count > 0:
            print()
            print("❌ Some loads failed. Check logs above for details.")
            exit(1)
        else:
            print()
            print("✅ All billing periods loaded successfully!")

    except Exception as e:
        print(f"❌ Error loading billing data to BigQuery: {str(e)}")
        exit(1)


def run_pipeline(config_path, args):
    """Run the complete meta-pipeline."""
    from finops.config import FinopsConfig
    from finops.services.pipeline_orchestrator import PipelineOrchestrator
    import re

    print("🚀 Running complete AWS billing pipeline...")

    try:
        # Load configuration
        config = FinopsConfig.from_cli_args(config_path, {})
        config.validate()

        # Validate date format if provided
        if args.start_date and not re.match(r'^\d{4}-\d{2}$', args.start_date):
            print("❌ Error: start-date must be in YYYY-MM format")
            return
        if args.end_date and not re.match(r'^\d{4}-\d{2}$', args.end_date):
            print("❌ Error: end-date must be in YYYY-MM format")
            return

        # Show configuration
        print(f"📋 Configuration:")
        print(f"  Bucket: {config.aws.bucket}")
        print(f"  Export: {config.aws.export_name}")
        print(f"  Version: {config.aws.cur_version}")
        if args.start_date or args.end_date:
            date_range = f"{args.start_date or 'earliest'} to {args.end_date or 'latest'}"
            print(f"  Date range: {date_range}")
        if args.dry_run:
            print(f"  Mode: DRY RUN")
        print()

        # Initialize and run pipeline
        orchestrator = PipelineOrchestrator(config)

        results = orchestrator.run_full_pipeline(
            start_date=args.start_date,
            end_date=args.end_date,
            dry_run=args.dry_run
        )

        # Show results
        if args.dry_run:
            print("\n📋 Dry Run Summary:")
            print(f"  Manifests to process: {results['manifests_to_process']}")
            print(f"  Billing periods: {', '.join(results['billing_periods']) if results['billing_periods'] else 'None'}")
        else:
            print(f"\n🎉 Pipeline completed!")
            print(f"  Run ID: {results['run_id']}")
            print(f"  Manifests processed: {results['manifests_processed']}")
            print(f"  Billing periods: {', '.join(results['billing_periods_processed']) if results['billing_periods_processed'] else 'None'}")

            if results['manifests_processed'] > 0:
                print(f"\n📊 Data loaded successfully!")
                if config.database.duckdb:
                    print(f"  • DuckDB: {config.database.duckdb.database_path}")
                print(f"  • Parquet: {config.parquet_dir}")
                if config.database.remote == "bigquery" and config.database.bigquery:
                    print(f"  • BigQuery: {config.database.bigquery.project_id}.{config.database.bigquery.dataset_id}.{config.database.bigquery.table_id}")

    except Exception as e:
        print(f"❌ Pipeline failed: {str(e)}")
        print("\nTo retry: simply rerun the same command")
        exit(1)
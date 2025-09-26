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
    load_remote_parser.set_defaults(func=load_billing_remote)


def discover_manifests(config_path, args):
    """Discover AWS CUR manifests and save to state database."""
    from finops.config import FinopsConfig
    from finops.services.manifest_discovery import ManifestDiscoveryService
    from finops.services.state_db import StateDB
    from pathlib import Path

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

        # Initialize state database
        state_db = StateDB(Path(config.state_db))

        # Discover manifests and persist to database
        discovery_service = ManifestDiscoveryService(config.aws, state_db)
        manifests = discovery_service.discover_manifests()

        # Display results
        summary = discovery_service.get_manifest_summary(manifests)
        print(summary)

        # Show state summary
        state_summary = state_db.get_manifest_summary()
        print(f"\nState database summary:")
        for state, count in state_summary.items():
            print(f"  {state}: {count}")

        if manifests:
            print(f"\nNext step: Use 'finops aws extract-manifests' to download CSV files")
            print(f"Or use 'finops aws show-state' to view detailed state information")

    except Exception as e:
        print(f"Error: {e}")
        exit(1)


def extract_manifests(config, args):
    """Extract manifest files."""
    print("📦 Extracting manifest files...")
    print("[STUB] Implementation pending")


def show_state(config_path, args):
    """Show pipeline execution state."""
    from finops.config import FinopsConfig
    from finops.services.state_db import StateDB
    from pathlib import Path
    import json

    print("📊 Pipeline execution state:")

    try:
        # Load configuration
        config = FinopsConfig.from_cli_args(config_path, {})

        # Initialize state database
        state_db = StateDB(Path(config.state_db))

        # Get state summary
        state_summary = state_db.get_manifest_summary()
        if not state_summary:
            print("No manifests found in state database.")
            print("Run 'finops aws discover-manifests' first to populate the database.")
            return

        print("\nState Summary:")
        total = 0
        for state, count in state_summary.items():
            print(f"  {state}: {count}")
            total += count
        print(f"  Total: {total}")

        # Get latest manifests
        latest = state_db.get_latest_manifests(limit=10)
        if latest:
            print(f"\nLatest 10 manifests:")
            for manifest in latest:
                files = json.loads(manifest['csv_files'])
                print(f"  • {manifest['billing_period']} ({manifest['cur_version']}) - "
                      f"{len(files)} files - {manifest['state']} - {manifest['manifest_id']}")

        # Show state transition counts
        print(f"\nState Details:")
        for state in ['discovered', 'downloading', 'staged', 'loading', 'loaded', 'failed']:
            if state in state_summary:
                manifests_in_state = state_db.get_manifests_by_state(state)
                print(f"  {state.title()}: {len(manifests_in_state)} manifests")
                if state == 'failed':
                    for manifest in manifests_in_state[:5]:  # Show first 5 failed
                        error = manifest.get('error_message', 'No error message')
                        print(f"    - {manifest['billing_period']} {manifest['manifest_id']}: {error}")

    except Exception as e:
        print(f"Error: {e}")
        exit(1)


def extract_billing(config_path, args):
    """Extract billing files from S3."""
    from finops.config import FinopsConfig
    from finops.services.state_db import StateDB
    from finops.services.billing_extractor import BillingExtractorService
    from pathlib import Path

    print("💾 Extracting billing files from S3...")

    try:
        # Load configuration
        config = FinopsConfig.from_cli_args(config_path, {})
        config.validate()

        # Initialize state database
        state_db = StateDB(Path(config.state_db))

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
            print("All discovered manifests")

        print(f"Staging directory: {staging_dir}")
        print()

        # Initialize billing extractor
        extractor = BillingExtractorService(config.aws, state_db)

        # Extract files
        stats = extractor.extract_billing_files(start_date, end_date, staging_dir)

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
    from finops.services.state_db import StateDB
    from finops.services.duckdb_loader import DuckDBLoader
    from pathlib import Path

    print("🗄️  Loading billing data to local DuckDB...")

    try:
        # Load configuration
        config = FinopsConfig.from_cli_args(config_path, {})

        # Initialize state database
        state_db = StateDB(Path(config.state_db))

        # Create data directory if needed
        duckdb_path = Path(config.duckdb_path)
        duckdb_path.parent.mkdir(parents=True, exist_ok=True)

        print(f"DuckDB database: {config.duckdb_path}")
        print(f"Staging directory: {config.staging_dir}")
        print()

        # Load data using DuckDB loader
        with DuckDBLoader(config.duckdb_path, state_db) as loader:
            stats = loader.load_billing_data(
                staging_dir=config.staging_dir,
                start_date=getattr(args, 'start_date', None),
                end_date=getattr(args, 'end_date', None)
            )

            # Display final results
            print()
            if stats['loaded_manifests'] > 0:
                print("🎉 Loading completed successfully!")
                print(f"Database location: {config.duckdb_path}")

                # Show table info
                table_info = loader.get_table_info()
                if table_info:
                    print(f"\nTable Information:")
                    print(f"  Table: {table_info['table_name']}")
                    print(f"  Columns: {table_info['column_count']}")
                    print(f"  Rows: {table_info['row_count']:,}")
                    if table_info['date_range']['min_date']:
                        print(f"  Date range: {table_info['date_range']['min_date']} to {table_info['date_range']['max_date']}")

                print(f"\nNext steps:")
                print(f"  • Query data: duckdb {config.duckdb_path}")
                print(f"  • Export to Parquet: finops aws export-parquet")
                print(f"  • Load to BigQuery: finops aws load-billing-remote")
            else:
                print("No data was loaded. Check that you have staged manifests.")
                print("Run 'finops aws extract-billing' to stage CSV files first.")

    except Exception as e:
        print(f"Error: {e}")
        exit(1)


def export_parquet(config_path, args):
    """Export to Parquet format."""
    from pathlib import Path
    from finops.config import FinopsConfig
    from finops.services.state_db import StateDB
    from finops.services.parquet_exporter import ParquetExporter
    import re

    print("📁 Exporting to Parquet format...")

    try:
        # Load configuration (skip AWS validation since export only needs local DuckDB)
        config = FinopsConfig.from_cli_args(Path(config_path), {})

        # Override parquet directory if specified
        parquet_dir = args.output_dir if args.output_dir else config.parquet_dir

        # Initialize state database
        state_db = StateDB(Path(config.state_db))

        # Validate date format if provided
        if args.start_date and not re.match(r'^\d{4}-\d{2}$', args.start_date):
            print("❌ Error: start-date must be in YYYY-MM format")
            return
        if args.end_date and not re.match(r'^\d{4}-\d{2}$', args.end_date):
            print("❌ Error: end-date must be in YYYY-MM format")
            return

        # Export using ParquetExporter service
        with ParquetExporter(config.duckdb_path, state_db, parquet_dir) as exporter:
            # Validate DuckDB table exists
            if not exporter.validate_table_exists():
                print("❌ Error: finops table not found or empty in DuckDB")
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


def load_billing_remote(config, args):
    """Load billing data to remote warehouse."""
    print("☁️  Loading billing data to remote warehouse...")
    print("[STUB] Implementation pending")
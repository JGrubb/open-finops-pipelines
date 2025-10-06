from finops.commands.aws.handlers import (
    discover_manifests,
    extract_billing,
    load_billing_local,
    export_parquet,
    load_billing_remote,
    run_pipeline,
)


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
    load_local_parser.add_argument(
        "--no-monthly-pipeline",
        action="store_true",
        help="Disable memory-optimized monthly pipeline (load all months at once)"
    )
    load_local_parser.add_argument(
        "--compression",
        default="snappy",
        choices=["snappy", "gzip", "lz4", "zstd"],
        help="Parquet compression type for monthly pipeline (default: snappy)"
    )
    load_local_parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing Parquet files in monthly pipeline"
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